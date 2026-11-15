# ADR 003: RabbitMQ retries with TTL queues and a DLQ

## Status

Accepted.

## Context

The assignment requires three attempts with exponential delays and a DLQ. Using
`sleep` in the worker reduces consumer availability and hides retry state inside
the code.

## Decision

Use a main queue, two retry queues with TTLs of one and two seconds, and a DLQ.
Before ACKing the original delivery, the worker confirms publication of the retry
message. The three attempts include the original delivery; after the third
failure, the message goes to the DLQ.

## Consequences

Retries are visible in the RabbitMQ Management UI and do not block the worker.
Duplicates can occur between publishing a retry and ACKing the original message,
so payment and webhook state must be durable.
