# Async Payment Processor

An educational implementation of a payment-processing assignment. The service
accepts a payment request, processes it asynchronously, and sends the result to a
webhook endpoint.

## Project documentation

- [Specification](docs/spec.md): API, data, invariants, and processing scenarios.
- [Architecture](docs/architecture.md): components, queues, and sequences.
- [ADRs](docs/adr/): decisions about the outbox, redelivery, retries, and the DLQ.
- [Test cases](docs/test-cases.md): behavior to verify before writing automated tests.
- [Component contracts](docs/contracts.md): boundaries between the API, database,
  relay, and worker.

`docs/spec.md` is the single source of truth for business rules. Other documents
avoid repeating those rules: the architecture describes component relationships,
contracts describe data shapes and interfaces, ADRs explain decisions, and test
cases describe how to verify behavior.

## Quick start

Docker and Docker Compose are required. This setup is intended for local
development: published ports are bound to `127.0.0.1`, and the demo UI uses the
configured API key.

From a clean clone:

```bash
cp .env.example .env
docker compose up -d --build
docker compose ps
```

Compose starts PostgreSQL and RabbitMQ, applies migrations in a separate job, and
starts the API with its outbox relay in the lifespan, the worker, and the demo
webhook sink. The migration job exits successfully with code 0. To stop the stack
without deleting data, run `docker compose down`; `docker compose down -v` also
deletes local data. If `just` is installed, `just up`, `just down`, and `just logs`
are available.

- Demo form and received callbacks: [http://localhost:8080](http://localhost:8080).
- Swagger UI: [http://localhost:8000/docs](http://localhost:8000/docs).
- RabbitMQ Management: [http://localhost:15672](http://localhost:15672), with
  `payment_broker` / `payment_broker_password` by default.
- Received webhook JSON: [http://localhost:8080/callbacks](http://localhost:8080/callbacks).

Create and inspect a payment without the form (use a new key for each new payment):

```bash
curl -sS -X POST http://localhost:8000/api/v1/payments \
  -H 'Content-Type: application/json' -H 'X-API-Key: local-development-key' \
  -H 'Idempotency-Key: example-1' \
  -d '{"amount":"1250.00","currency":"RUB","description":"Order 42","metadata":{"order_id":"42"},"webhook_url":"http://webhook-sink:8080/callbacks/payments"}'
# Replace PAYMENT_ID with the ID from the response:
curl -sS -H 'X-API-Key: local-development-key' http://localhost:8000/api/v1/payments/PAYMENT_ID
curl -sS http://localhost:8080/callbacks
```

The `webhook-sink` address is available inside the Compose network. The regular
gateway finishes processing in 2–5 seconds, after which a callback appears. To
demonstrate two delivery failures followed by success, set
`WEBHOOK_RESPONSE_SEQUENCE=500,500,204` in `.env` and recreate the sink with
`docker compose up -d webhook-sink`. The sequence is tracked separately for each
payment; after the sequence ends, its last status code is reused. The sink keeps
callback history and counters in memory only, so they are cleared on restart. It
is a local demo service, not a production endpoint. The sink only receives HTTP
notifications on behalf of the client system; the payment gateway emulator runs
inside the worker.

To see the DLQ, set `WEBHOOK_RESPONSE_SEQUENCE=500,500,500`, recreate the sink,
and create a new payment. After three attempts, the message appears in the
`payments.dlq` queue in RabbitMQ Management (Queues and Streams). The payment
itself retains the gateway result. Whether a payment ends in `failed` is chosen
by the gateway emulator based on the payment ID; it cannot be selected in the
form. Create payments until one fails, or test the scenario deterministically in
the gateway tests.

For development on the host, install Python 3.14, `uv`, and `just`:

```bash
uv sync --locked
just test                 # Starts PostgreSQL and a separate test RabbitMQ, then runs pytest.
uv run ruff check .
uv run ty check
```

## Local infrastructure

Copy `.env.example` to `.env` and adjust the values if needed. Start local
PostgreSQL and RabbitMQ with `just db-up`. By default, PostgreSQL listens on port
`5433` to avoid conflicting with a local PostgreSQL instance on `5432`. Alembic
commands are available in `justfile`.

`APP_DB_ECHO` is disabled by default. Enable it only when needed for debugging,
because SQL logs may contain webhook URLs and payment metadata.

RabbitMQ AMQP is available on port `5672`; the Management UI is at
[http://localhost:15672](http://localhost:15672). Credentials are in `.env`;
`just rabbit-ui` prints the address and login. Use `just rabbit-up` if you only
need the broker.

`just infra-up` remains an alias for `db-up`. `just test` starts PostgreSQL and a
separate, ephemeral `rabbitmq-test` service (AMQP `5673`, Management `15673` by
default). Tests use isolated PostgreSQL schemas and the test broker, so they do
not clear the demo queues. Change the test broker ports with `TEST_RABBITMQ_PORT`
and `TEST_RABBITMQ_MANAGEMENT_PORT` in `.env`. After `just up`, run `just test` to
check that the demo keeps working and its queues in RabbitMQ Management are
unchanged. The test service is not part of the regular startup; remove only that
service with `docker compose rm -sf rabbitmq-test`.
