# ADR 002: Payment state as the durable source of consumer idempotency

## Status

Accepted.

## Context

RabbitMQ does not keep a permanent registry of processed `event_id` values. The
relay may publish an event more than once, and the broker may redeliver a message
that was not acknowledged.

## Decision

An event contains a stable `event_id` for tracing, but the worker makes its
decision using the payment state in PostgreSQL. A repeated event for a final
payment whose webhook was delivered is a no-op. For a final payment whose
webhook is still undelivered, the worker retries only the webhook.

## Consequences

A separate processed-events table is unnecessary. This does not guarantee an
exactly-once call to a real gateway if the process fails between the external
call and persisting the result; a future provider integration must accept its
own idempotency key.
