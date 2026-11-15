# ADR 001: Transactional outbox for `payment.created`

## Status

Accepted.

## Context

Creating a payment in PostgreSQL and publishing an event to RabbitMQ do not share
a transaction. Publishing before the database commit could deliver an event for
a payment that does not exist. Publishing after the commit leaves a failure
window between the two operations.

## Decision

The API creates the payment and outbox event in one PostgreSQL transaction. The
relay publishes unpublished records asynchronously and marks them `published`
only after broker confirmation.

## Consequences

The delivery guarantee is at least once. Republishing after a failure is expected;
the worker must safely handle duplicate deliveries.

During graceful shutdown, the relay stops claiming new records and completes
in-flight publishes within a bounded time. Without broker confirmation, a record
is not marked `published`; after cancellation or timeout it remains available
for a later attempt. If RabbitMQ accepted the message but the process did not
persist the `published` state, publishing it again after restart is expected.
