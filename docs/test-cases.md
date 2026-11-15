# Test cases

This document records expected behavior before automation. Tests are written
against these cases; changing an expectation requires changing the specification
or correcting an error in the case itself.

## Test environment

- PostgreSQL and RabbitMQ run in Docker Compose.
- The API uses a known test `X-API-Key`.
- The gateway emulator is controllable in tests: it can return success, a
  confirmed decline, or a temporary technical error without randomness or the
  usual 2–5 second delay.
- The webhook sink stores received callbacks and can return a configured sequence
  of HTTP status codes or simulate a timeout.
- The worker runs one active consumer with manual ACK.

## API and validation

| ID | Scenario | Actions | Expected result |
| --- | --- | --- | --- |
| API-01 | Create payment | Send a valid POST with both keys. | `202`, `payment_id`, `pending`, and `created_at`; one payment and one pending outbox event in the database. |
| API-02 | Get payment | GET a created payment. | `200` and public payment fields. |
| API-03 | Unknown payment | GET an unknown UUID. | `404`. |
| API-04 | Missing API key | Send POST and GET without `X-API-Key`. | `401`; POST does not change the database. |
| API-05 | Invalid API key | Send POST with a different `X-API-Key`. | `401`; POST does not change the database. |
| API-06 | Invalid body | Send a zero/negative amount, unknown currency, or invalid URL. | `422`; no payment or outbox event is created. |
| API-07 | Unknown field | Add an unknown top-level field to the POST body. | `422`; the extra field is not ignored. |

## Idempotency

| ID | Scenario | Actions | Expected result |
| --- | --- | --- | --- |
| IDEM-01 | Identical retry | Send the same POST twice with the same key and significant fields. | Both responses refer to one `payment_id`; the database contains one payment and one outbox event. |
| IDEM-02 | Changed significant field | Reuse a key while changing amount, currency, description, metadata, or webhook URL. | `409`; the existing payment is unchanged. |
| IDEM-03 | Concurrent identical requests | Send two identical POST requests concurrently with the same key. | Exactly one payment and one outbox event are created; both responses refer to that payment. |
| IDEM-04 | Concurrent conflicting requests | Send two different request bodies concurrently with the same key. | Exactly one payment is created; the other request returns `409`. |

## Outbox relay

| ID | Scenario | Actions | Expected result |
| --- | --- | --- | --- |
| OUT-01 | Publish new event | Create a payment, stop the worker, and run the relay while RabbitMQ is available. | One event appears in `payments.new`; the outbox row is marked `published` after broker confirmation. |
| OUT-02 | Broker temporarily unavailable | Create a payment while RabbitMQ is unavailable, then restore the broker. | The API stays available; the outbox remains `pending` or temporarily `processing` until its lease recovers, then gets published; the event is not lost. |
| OUT-03 | Duplicate after relay crash | Simulate a publish followed by a crash before marking the outbox row `published`; restart the relay. | The event is published again; the consumer handles the duplicate safely. |
| OUT-04 | Concurrent relays | Run two relay instances with multiple pending events. | Each row is claimed by one relay per pass; a row is not published concurrently by both. |
| OUT-05 | Graceful relay shutdown | Start a publish, request application shutdown before broker confirmation, and then restart. | The unfinished row stays `processing` until its lease expires; if a duplicate occurs, the worker does not call the gateway again for a final payment. |

## Gateway and payment status

| ID | Scenario | Actions | Expected result |
| --- | --- | --- | --- |
| PAY-01 | Gateway success | Deliver an event to the worker with a successful gateway outcome. | Payment becomes `succeeded`, `processed_at` is set, then the webhook is sent. |
| PAY-02 | Confirmed decline | Deliver an event to the worker with a gateway decline. | Payment becomes `failed`, `processed_at` is set, then the webhook is sent. |
| PAY-03 | Duplicate final event | Redeliver an event after gateway success and webhook delivery. | Gateway and webhook are not called again; the message is acknowledged. |
| PAY-04 | Retry after result persisted | Payment is final but its webhook has not been delivered; redeliver the event. | The gateway is not called; the worker only attempts the webhook. |
| PAY-05 | Unknown gateway result | Gateway returns a temporary error before an outcome is known. | Payment remains `pending`; the message is retried rather than sent to the webhook. |
| PAY-06 | Crash before ACK | Stop the worker after persisting the result but before ACK. | RabbitMQ redelivers the event; the worker does not call the gateway again for a final payment. |

## Webhook, retries, and DLQ

Three attempts mean the initial handling plus two retries; the gateway and webhook
share the same attempt budget. Temporary gateway errors can reduce the number of
actual POST requests. Delays are one and two seconds.

| ID | Scenario | Actions | Expected result |
| --- | --- | --- | --- |
| WH-01 | Successful webhook | Sink returns `204`. | One POST; `webhook_delivered_at` is set; message is ACKed. |
| WH-02 | Webhook recovers | Sink returns `500`, `500`, then `200`. | Three POST requests with 1s and 2s delays; payment retains its final status; webhook is marked delivered; DLQ stays empty. |
| WH-03 | Attempts exhausted | Sink returns `500` or times out three times. | Three POST requests; payment retains its final status; `webhook_delivery_exhausted_at` is set; message appears in `payments.dlq`. |
| WH-04 | Webhook failure after gateway decline | Gateway declines and the sink fails three times. | Payment remains `failed`; the message goes to the DLQ because of webhook failure, not gateway failure. |
| WH-05 | Gateway technical errors | Gateway returns a temporary error three times. | Payment remains `pending`; the message goes to the DLQ after the third attempt. |
| WH-06 | Duplicate retry publish | Simulate a crash between retry publisher confirmation and ACK of the original delivery. | A webhook attempt whose result was already recorded is not sent again, though its retry can be published more than once; the gateway is not called after the final result. A crash after POST but before recording the result can repeat the POST; gateway technical errors do not have a durable counter. |
| WH-07 | Gateway errors before webhook | Return two temporary gateway errors, then success and HTTP 204. | Three message attempts, one POST, and `webhook_attempts = 1`; broker attempt marker is 3. |

## Migrations and environment

| ID | Scenario | Actions | Expected result |
| --- | --- | --- | --- |
| ENV-01 | Fresh migration | Start an empty PostgreSQL database and run Alembic upgrade. | Creates `payments`, `outbox`, indexes, unique constraints, and foreign keys. |
| ENV-02 | Repeated migration | Run Alembic upgrade a second time. | Command succeeds without changing the schema. |
| ENV-03 | Compose smoke test | Start the main Compose stack after migrations. | PostgreSQL, RabbitMQ, API, and worker become healthy; the API accepts an authorized request. |
| ENV-04 | Demo profile | Start Compose with the webhook sink. | The sink is reachable from the worker by service name and from the host through its published port. |

## Automation priority

Automate API-01, API-04, IDEM-01, IDEM-02, IDEM-03, OUT-01, OUT-02, PAY-01,
PAY-02, PAY-03, WH-02, and WH-03 first. These cover all explicit assignment
requirements. Failure-boundary scenarios should be added as soon as the relevant
infrastructure exists.
