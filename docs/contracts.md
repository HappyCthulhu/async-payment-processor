# Component contracts

This document defines the public boundaries of the code. It does not prescribe
internal implementation, but stabilizes the behavior checked by tests. Processing
rules and business values belong to the [specification](spec.md); this document
focuses on types, data shapes, and operations.

## Code boundaries and package structure

Code lives in `src/payment_processor/`.

| Area | Responsibility |
| --- | --- |
| `application/` | Use cases, integration event DTOs, and `Protocol` ports for storage, the gateway, webhooks, and publishing. |
| `infrastructure/db/` | SQLAlchemy models, session factory, Alembic, and concrete repositories. |
| `infrastructure/messaging/` | FastStream broker, exchange, queues, and publisher adapter. |
| `infrastructure/integrations/` | Gateway emulator and webhook HTTP client. |
| `api/` | FastAPI app, routers, Pydantic schemas, API-key dependency, and relay startup in the lifespan. |
| `worker/` | FastStream consumer and worker use-case wiring. |
| `core/` | Configuration, logging, and explicit composition root. |

Dependencies point inward: `api` and `worker` call `application`, while
`infrastructure` implements its ports. `application` does not import concrete
infrastructure implementations. Wiring is explicit at API and worker startup,
without a separate dependency-injection library.

The use cases are `create_payment`, `get_payment`, `relay_outbox`, and
`handle_payment_event`. Each describes one scenario; HTTP routers and consumers
contain no business rules. `AsyncSession` remains an infrastructure detail and
serves as a practical Unit of Work; no shared UoW abstraction is introduced.

Only domain-specific ports are used: `PaymentRepository`, `OutboxRepository`,
`PaymentGateway`, `WebhookClient`, and `EventPublisher`. A generic
`BaseRepository[T]`, CQRS/read models, Event Sourcing, a mediator, and a duplicate
rich domain model are out of scope.

## Domain types

```text
PaymentStatus = pending | succeeded | failed
Currency = RUB | USD | EUR
OutboxStatus = pending | processing | published
GatewayOutcome = succeeded | failed | temporary_error
```

A gateway `failed` result is a confirmed business outcome. `temporary_error`
means no outcome is known yet and the event should be retried.

## HTTP contract

### `POST /api/v1/payments`

Input: `CreatePaymentRequest`.

```text
amount: Decimal > 0
currency: Currency
description: str
metadata: dict[str, JSON value]
webhook_url: HttpUrl
```

Unknown top-level fields are rejected. The `X-API-Key` and `Idempotency-Key`
headers are required.

Successful output: `CreatePaymentResponse`.

```text
payment_id: UUID
status: PaymentStatus
created_at: datetime
```

Status codes: `202` for a new request or an identical retry, `401` for an invalid
API key, `409` for a conflicting retry, and `422` for an invalid body.

### `GET /api/v1/payments/{payment_id}`

Output: `PaymentResponse` with public input fields, `payment_id`, `status`,
`created_at`, and `processed_at`. Internal counters, errors, and outbox state are
not returned. Status codes: `200`, `401`, and `404`.

## Storage contract

The API calls one atomic creation operation. Its observable result is:

```text
create_payment_with_outbox(request, idempotency_key)
  -> Created(payment) | Existing(payment) | Conflict
```

The operation either creates the payment and its `payment.created` outbox event
together or creates neither. The unique `idempotency_key` constraint is the
final arbiter when requests race.

The relay reads and changes outbox state only through operations with constrained
transitions:

```text
claim_pending_outbox(limit) -> list[ClaimedOutboxEvent]
mark_outbox_published(event_id, claim_token, published_at)
record_outbox_publish_failure(event_id, claim_token, error)
```

`claim_pending_outbox` locks rows in a short transaction using `FOR UPDATE SKIP
LOCKED` and moves them from `pending` to `processing`. The SQL lock exists only
until the claim transaction commits; `processing` is persisted state, not the
name of the SQL lock.

Only the relay that owns a claim may move it from `processing` to `published`
after publisher confirmation. A known publish failure returns the event to
`pending`. During shutdown, the relay stops claiming rows; unfinished events
remain `processing` until another relay can recover them after lease expiry. This
intentionally allows a duplicate publish after a restart.

Ownership is identified by `claim_token`, not only by `event_id`. The relay sends
the token with each finalization operation, so a stale owner cannot mark an event
published or return it to `pending` after another relay has reclaimed it.

The worker reads payment state before external actions and records only allowed
changes:

```text
get_payment_state_for_handling(payment_id)
record_gateway_outcome(payment_id, status, processed_at)
record_webhook_delivery(payment_id, delivered_at)
record_webhook_failure(payment_id, attempt, error, exhausted_at?)
```

## RabbitMQ event contract

Exchange: `payments.exchange`. Main routing key/queue: `payments.new`.

```json
{
  "event_id": "uuid",
  "event_type": "payment.created",
  "event_version": 1,
  "payment_id": "uuid",
  "created_at": "2026-11-02T12:00:00Z",
  "payment": {
    "amount": "1250.00",
    "currency": "RUB",
    "description": "Order #42 payment",
    "metadata": {"order_id": "42"},
    "webhook_url": "http://webhook-sink:8080/callbacks/payments"
  }
}
```

The event contains a snapshot of immutable data, so the worker does not need to
read those fields from the database again. Payment status and webhook delivery
state are not included in the event.

The `x-payment-attempt` header starts at `1`. When the worker republishes a
message for retry, it increments the value after the new message is confirmed.
The value is used alongside persistent payment state, not as a replacement for it.

## Gateway contract

The worker depends on an asynchronous port that accepts `payment_id` and returns
one `GatewayOutcome`.

The regular emulator's delay and result distribution, as well as controllable
test scenarios, are defined in the [specification](spec.md#payment-processing).
These are behavioral rules, not details of the programming interface.

## Webhook contract

The worker sends a `POST` to the URL in the event with this body:

```json
{
  "event_id": "uuid",
  "payment_id": "uuid",
  "status": "succeeded",
  "amount": "1250.00",
  "currency": "RUB",
  "metadata": {"order_id": "42"},
  "processed_at": "2026-11-02T12:00:04Z"
}
```

An HTTP 2xx response means success. A timeout, network error, or any other status
routes the message through the broker retry path. The webhook adapter makes no
business decisions; it performs the HTTP request and returns its technical
result. When a repeated message corresponds to a recorded failure, the worker
skips the HTTP request and continues the retry path. Recording the result is not
atomic with the HTTP request, so the receiver should deduplicate by `event_id`.

## Test substitution points

Tests may replace only external boundaries:

- gateway outcomes;
- webhook endpoint HTTP behavior;
- retry timing and delays;
- RabbitMQ or PostgreSQL in lower-level tests.

Integration tests use real PostgreSQL and RabbitMQ. They must not replace the
repository or broker in scenarios that verify the outbox, ACK, retries, and DLQ.
