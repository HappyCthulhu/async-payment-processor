# ADR 004: Pragmatic hexagonal architecture

## Status

Accepted.

## Context

The service is small but has several external boundaries: HTTP API, PostgreSQL,
RabbitMQ, a gateway emulator, and a webhook endpoint. Mixing these details into
the router or FastStream handler would make tests harder and tie payment rules
to frameworks. Full Clean Architecture with a rich separate domain model, generic
repository, mediator, and a separate dependency-injection container would add
duplication without benefit at this scale.

## Decision

Use a lightweight ports-and-adapters approach:

- `application` contains use cases and `Protocol` ports;
- `domain` contains only payment types and pure rules;
- `infrastructure` implements ports with SQLAlchemy, FastStream, and `httpx`;
- `api` and `worker` are entry points that validate transport input and call
  use cases;
- dependencies are assembled explicitly in the composition root.

`AsyncSession` acts as a practical Unit of Work in the infrastructure layer.
The ORM `Payment` model is not duplicated as a separate domain entity.

## Consequences

Use cases can be tested with fake adapters, while integration tests use real
PostgreSQL and RabbitMQ. The structure allows the gateway or webhook transport
to be replaced without changing the business flow, without adding unnecessary
abstraction layers.
