# Архитектура

## Компоненты

```mermaid
flowchart LR
    Client[curl / backend магазина] -->|POST payment\nX-API-Key and Idempotency-Key| API
    subgraph ApiContainer[api container]
        direction TB
        API[FastAPI API]
        Relay[Outbox relay\nbackground task]
    end
    API -->|создаёт payment and outbox\nin one transaction| DB[(PostgreSQL\npayments and outbox)]
    Relay -->|claim pending outbox rows| DB
    Relay -->|publish event| Exchange{{RabbitMQ\npayments.exchange}}
    Exchange -->|publisher confirm| Relay
    Relay -->|mark outbox published| DB
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

Relay — отдельный **логический компонент**, но не отдельный Compose-сервис: при
старте `api`-контейнера FastAPI lifespan запускает один фоновый asyncio-цикл.
HTTP-обработчик только записывает `payment` и `outbox` в транзакции и никогда не
публикует сообщение в RabbitMQ сам. Relay периодически захватывает pending
строки из PostgreSQL, публикует их и отмечает `published` только после broker
confirm. Его можно вынести в самостоятельный процесс без изменения контракта с
БД или RabbitMQ; для объёма тестового пока оставляем его внутри `api`-контейнера.

`webhook-sink` подключается как demo-сервис в Docker Compose. Он не участвует
в обработке платежа: принимает callback и позволяет увидеть результат в логах
или через диагностический endpoint.

## Правило реализации портов

Порты application-слоя определяются через `Protocol`. Каждый конкретный adapter,
включая test double, **явно наследуется** от реализуемого порта и помечает его
методы `@override`. Это даёт IDE и статическому анализатору возможность
проверить имя и сигнатуру метода рядом с его объявлением, а не только в точке,
где adapter передаётся в use case. Импорт порта в infrastructure — допустимая
зависимость наружного слоя от внутреннего; скрывать базовый класс под
`TYPE_CHECKING` нельзя, потому что он нужен Python при создании класса.

## Успешный путь

```mermaid
sequenceDiagram
    participant C as Клиент
    participant A as API
    participant L as Outbox relay
    participant P as PostgreSQL
    participant R as RabbitMQ
    participant W as Worker
    participant G as Gateway emulator
    participant H as Webhook endpoint

    C->>A: Create payment with keys
    A->>P: Store payment and outbox in one transaction
    A-->>C: Return pending payment
    L->>P: Claim pending outbox record
    L->>R: Publish event
    R-->>L: Confirm publication
    L->>P: Mark outbox record published
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
