# Архитектура

## Компоненты

```mermaid
flowchart LR
    Client[curl / backend магазина] -->|POST payment\nX-API-Key + Idempotency-Key| API[API + outbox relay]
    API -->|одна транзакция| DB[(PostgreSQL\npayments + outbox)]
    API -->|broker confirm| Exchange{{RabbitMQ\npayments.exchange}}
    Exchange --> Work[payments.new]
    Work -->|delivery| Worker[Один FastStream worker]
    Worker -->|payment state + результат| DB
    Worker --> Gateway[Внутренний gateway emulator]
    Worker -->|POST результата| Hook[Webhook клиента]
    Worker --> Retry1[payments.retry.1\nTTL 1s]
    Retry1 --> Work
    Worker --> Retry2[payments.retry.2\nTTL 2s]
    Retry2 --> Work
    Worker --> DLQ[payments.dlq]
```

`webhook-sink` подключается как demo-сервис в Docker Compose. Он не участвует
в обработке платежа: принимает callback и позволяет увидеть результат в логах
или через диагностический endpoint.

## Успешный путь

```mermaid
sequenceDiagram
    participant C as Клиент
    participant A as API / relay
    participant P as PostgreSQL
    participant R as RabbitMQ
    participant W as Worker
    participant G as Gateway emulator
    participant H as Webhook endpoint

    C->>A: Create payment with keys
    A->>P: Store payment and outbox in one transaction
    A-->>C: Return pending payment
    A->>P: Claim pending outbox record
    A->>R: Publish event
    R-->>A: Confirm publication
    A->>P: Mark outbox record published
    R->>W: Deliver event
    W->>P: Read current payment state
    W->>G: Process immutable event snapshot
    G-->>W: Return final result
    W->>P: Store final payment status
    W->>H: Send webhook
    H-->>W: Return successful response
    W->>P: Store webhook delivery result
    W->>R: ACK
```

## Ошибка webhook

```mermaid
sequenceDiagram
    participant W as Worker
    participant R as RabbitMQ
    participant H as Webhook endpoint
    participant P as PostgreSQL

    R->>W: Deliver attempt one
    W->>H: Send webhook
    H-->>W: Return error
    W->>R: Publish first retry
    R-->>W: Confirm publication
    W->>R: Acknowledge original event
    R-->>R: Delay one second then route to main queue
    R->>W: Deliver attempt two
    W->>H: Send webhook
    H-->>W: Return error
    W->>R: Publish second retry
    R-->>W: Confirm publication
    W->>R: Acknowledge original event
    R-->>R: Delay two seconds then route to main queue
    R->>W: Deliver attempt three
    W->>H: Send webhook
    H-->>W: Return error
    W->>P: Store exhausted delivery state
    W->>R: Reject event to dead letter queue
```

## Очереди и ответственность

| Очередь | Назначение | Кто пишет | Кто читает |
| --- | --- | --- | --- |
| `payments.new` | Основная обработка payment event | Relay и retry-очереди через DLX | Worker |
| `payments.retry.1` | Первая задержка, 1 секунда | Worker | RabbitMQ TTL/DLX |
| `payments.retry.2` | Вторая задержка, 2 секунды | Worker | RabbitMQ TTL/DLX |
| `payments.dlq` | Сообщения после третьей неудачи | RabbitMQ | Человек через Management UI |

Все рабочие очереди и сообщения durable/persistent. Worker использует manual ACK,
чтобы RabbitMQ мог redeliver не подтверждённое при падении consumer сообщение.
Publisher confirms подтверждают только приём сообщения брокером, а не завершение
работы worker.
