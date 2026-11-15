# Specification

## Purpose

The service accepts a payment request, processes it asynchronously through a
payment gateway emulator, and sends the result to the client's URL. The public
API is protected by one static `X-API-Key`.

The project demonstrates FastAPI, asynchronous PostgreSQL access, a transactional
outbox, RabbitMQ/FastStream, idempotency, retries, a DLQ, and Docker Compose.

## Scope

The scope includes `pending`, `succeeded`, and `failed` payment states, webhooks,
broker retries, and a DLQ. The system does not implement registration, accounts,
roles, payment cancellation, refunds, Celery, Redis, or Kubernetes.

The gateway is an internal worker component, not a separate container. It only
emulates an external integration. In the local setup, `webhook-sink` represents
the client's system and receives outgoing notifications.

## Public API

Every endpoint requires the `X-API-Key` header. A missing or invalid key returns
`401 Unauthorized`.

### Create a payment

`POST /api/v1/payments`

Required header: `Idempotency-Key`.

Request body:

```json
{
  "amount": "1250.00",
  "currency": "RUB",
  "description": "Order #42 payment",
  "metadata": {"order_id": "42"},
  "webhook_url": "http://webhook-sink:8080/callbacks/payments"
}
```

`amount` is a positive `Decimal`; `currency` is one of `RUB`, `USD`, or `EUR`.
Unknown top-level fields are rejected with `422`.

A successful new request returns `202 Accepted`:

```json
{
  "payment_id": "uuid",
  "status": "pending",
  "created_at": "2026-11-02T12:00:00Z"
}
```

### Get a payment

`GET /api/v1/payments/{payment_id}` returns public payment data, its status, and
timestamps. An unknown ID returns `404 Not Found`.

## Creation idempotency

`idempotency_key` has a unique constraint in PostgreSQL. For a new key, the API
creates the payment and one outbox record in the same transaction.

For a repeated key, the service loads the existing payment and structurally
compares all significant input fields: amount, currency, description, metadata,
and webhook URL.

- If the fields match, the original payment is returned without creating another
  outbox record.
- If any field differs, the service returns `409 Conflict`.

The database unique constraint, rather than an application-level pre-check,
arbitrates idempotency and handles two concurrent POST requests with the same key.

## Data

Two tables are sufficient for the assignment.

### `payments`

| Field | Purpose |
| --- | --- |
| `id` | Payment UUID and public `payment_id`. |
| `amount`, `currency`, `description`, `metadata` | Immutable snapshot of the payment request. |
| `status` | `pending`, `succeeded`, or `failed`. |
| `idempotency_key` | Unique creation key. |
| `webhook_url` | Callback URL. |
| `created_at`, `processed_at` | Creation time and time when the gateway result became final. |
| `webhook_attempts` | Number of completed POST attempts whose results were recorded in the database. |
| `last_recorded_webhook_attempt` | Broker attempt number for the most recently recorded webhook result; used for deduplication. |
| `webhook_delivered_at` | Time when the webhook endpoint returned a successful response. |
| `webhook_delivery_exhausted_at` | Time when all three attempts were exhausted, if applicable. |
| `last_error` | Sanitized technical description of the most recent error. |

### `outbox`

| Field | Purpose |
| --- | --- |
| `id` | Event UUID (`event_id`). |
| `payment_id` | Foreign key to the payment. |
| `event_type`, `event_version` | Event type `payment.created` and contract version. |
| `payload` | Immutable JSON snapshot of the payment request data. |
| `status` | `pending`, `processing`, or `published`. |
| `publish_attempts`, `published_at`, `created_at` | Relay state. |
| `claim_token`, `lease_expires_at` | Ownership and expiry for the current relay claim; set only while `processing`. |
| `last_error` | Sanitized reason for the last known publish failure. |

The outbox has a unique constraint on `(payment_id, event_type)`, so each payment
has one creation event.

Outbox states form a small state machine:

```text
pending -- claim --> processing -- broker confirm --> published
                         |
                         +-- known publish failure --> pending
```

`processing` means that the relay has persisted a claim and is attempting to
publish the event. It is not the PostgreSQL row lock itself: the lock is only
held during the short claim transaction. An unfinished `processing` claim can
be recovered after its lease expires.

## Event

The `payment.created` event contains `event_id`, `payment_id`, type, version,
and immutable payment data: amount, currency, description, metadata, webhook URL,
and creation time. It does not contain the mutable `status`.

The worker uses the payload as input for the gateway and webhook, but checks
PostgreSQL before processing. The database remains the source of truth for the
current status and webhook delivery state.

## Payment processing

The regular gateway emulator waits between two and five seconds and returns
`succeeded` 90% of the time and a confirmed business decline (`failed`) 10% of
the time. This is the selected interpretation of the assignment; controllable
technical failures are used only in tests and demo scenarios.

The worker processes an event as follows:

1. Read the payment state from the database.
2. If the gateway result is already final, skip the gateway and only continue
   with an undelivered webhook.
3. If the payment is `pending`, call the gateway emulator and persist
   `succeeded` or `failed` together with `processed_at`.
4. Send the webhook with the payment result.
5. On webhook success, persist `webhook_delivered_at` and acknowledge the
   RabbitMQ message.

One consumer runs with one active handler at a time. This limits ordinary
concurrent races. If the process fails after the external gateway call but
before persisting its result, the outcome may be unknown and the event may be
delivered again. The emulator must be deterministic for a given `payment_id`; a
real provider would need its own idempotency key and reconciliation. The service
does not claim exactly-once gateway calls.

## Webhook

The worker sends a `POST` to the `webhook_url` supplied when the payment was
created. Any HTTP 2xx response counts as success. The payload contains at least
`payment_id`, `status`, `amount`, `currency`, `metadata`, `processed_at`, and
`event_id`.

A network error, timeout, or non-2xx response means the webhook was not delivered.
The payment result does not change: a webhook can fail after either `succeeded`
or `failed`.

## Retries and DLQ

Three attempts consist of the initial message handling and two retries, with a
shared budget for gateway and webhook processing. If the gateway has temporary
errors, fewer than three POST requests may occur. Retry delays are one and two
seconds. RabbitMQ implements the delays using TTL retry queues:

| Failed attempt | Action |
| --- | --- |
| First | Publish to `payments.retry.1` with a 1-second TTL. |
| Second | Publish to `payments.retry.2` with a 2-second TTL. |
| Third | Route to `payments.dlq`. |

Before acknowledging the original message, the worker waits for publisher
confirmation that the retry message was accepted. A duplicate is still possible
if the process fails between that confirmation and the ACK. The attempt number
is carried in a message header, and completed webhook attempts are persisted in
the database. When a redelivery repeats an attempt whose result is already
recorded, the worker skips another webhook POST but republishes the event to the
next retry queue. This also recovers a failure between recording the error and
publishing the retry. A crash after the HTTP request but before recording its
result can cause another POST; concurrent consumers can also race. The receiver
should deduplicate by `event_id`. Exactly-once webhook delivery is not guaranteed.

A confirmed business decline from the gateway is not retried. A technical
gateway error before a known result leaves the payment `pending`; after three
attempts, the message goes to the DLQ. If webhook retries are exhausted, the
payment remains `succeeded` or `failed`, and the undelivered state is visible in
the payment fields and the DLQ message.

Technical gateway errors do not have a durable counter: duplicate messages can
cause additional calls until a final result is recorded. For records created
before the counter-separation migration, `webhook_attempts` may contain the old
broker attempt number. The exact historical number of POST requests cannot be
reconstructed after gateway errors; the value is retained as a deduplication
marker.

## Outbox relay

The API writes the payment and outbox event in one transaction, then returns
`202`. A background relay in the API reads small batches of `pending` records
using `FOR UPDATE SKIP LOCKED`, persists their `processing` state, publishes
persistent messages to RabbitMQ, and marks each row `published` only after
publisher confirmation and routing verification.

A crash after publication but before marking the row `published` causes a
duplicate publish. This is the expected at-least-once guarantee. The relay does
not delete `pending` events when the broker is unavailable. The API starts
independently of the RabbitMQ connection; the relay retries the connection and
resumes after the broker recovers. After each successful pass, cleanup deletes
only `published` records older than `APP_OUTBOX_RETENTION_DAYS` (30 days by
default), and removes no more than `APP_OUTBOX_CLEANUP_BATCH_SIZE` (100 by default)
per pass.

During graceful shutdown, the relay stops claiming new rows and completes
in-flight publishes within a bounded time. It marks a row `published` only after
publisher confirmation. If the deadline expires or the process is cancelled,
the unconfirmed row remains `processing` until its lease expires, after which a
new relay can claim it. RabbitMQ may have accepted the message before
cancellation, so a duplicate is possible and must be handled safely by the
worker.

The application writes operational Loguru events to stdout: payment acceptance,
outbox publication, gateway result, webhook delivery, retries, and DLQ routing.
These logs do not contain API keys, idempotency keys, webhook URLs, webhook
bodies, or metadata.

## Scenarios to verify

1. A new payment creates one payment row and one outbox event.
2. An identical repeated POST does not create a second event.
3. Reusing a key with a changed field returns `409`.
4. Gateway success sets the payment to `succeeded` and delivers a webhook.
5. A confirmed gateway decline sets the payment to `failed` and still sends a webhook.
6. Two webhook failures followed by success produce three attempts and delivery.
7. Three webhook failures preserve the payment result, record exhaustion, and
   route the message to the DLQ.
8. A relay crash after publishing creates a duplicate that does not call the
   gateway again for an already-final payment.
9. A worker crash before ACK causes redelivery without losing the message.
