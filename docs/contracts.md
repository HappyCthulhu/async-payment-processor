# Контракты компонентов

Этот документ задаёт публичные границы будущего кода. Он не предписывает
внутреннюю реализацию, но стабилизирует то, что будут проверять тесты.
Правила обработки и бизнес-значения принадлежат
[спецификации](spec.md); здесь остаются только типы, формы данных и операции.

## Границы кода и структура пакета

Код располагается в `src/payment_processor/`.

| Область | Ответственность |
| --- | --- |
| `domain/` | Enum'ы, value-типы и чистые правила переходов payment. Не содержит отдельной ORM-дублирующей сущности `Payment`. |
| `application/` | Use case'ы, интеграционные event DTO и `Protocol`-порты для хранилища, gateway, webhook и publisher. |
| `infrastructure/db/` | SQLAlchemy-модели, session factory, Alembic и конкретные repository. |
| `infrastructure/messaging/` | FastStream broker, exchange, queue и publisher-адаптер. |
| `infrastructure/integrations/` | Gateway emulator и HTTP-клиент webhook. |
| `api/` | FastAPI app, routers, Pydantic-схемы, API key dependency и запуск relay в lifespan. |
| `worker/` | FastStream consumer и wiring use case'ов worker. |
| `core/` | Конфигурация, логирование и явный composition root. |

Зависимости направлены внутрь: `api` и `worker` вызывают `application`, а
`infrastructure` реализует его порты. `domain` не импортирует FastAPI,
Pydantic, SQLAlchemy, FastStream или `httpx`; `application` не импортирует их
конкретные реализации. Wiring выполняется явно при старте API и worker, без
отдельной DI-библиотеки.

Use case'ы: `create_payment`, `get_payment`, `relay_outbox` и
`handle_payment_event`. Каждый описывает один сценарий; HTTP-router и consumer
не содержат бизнес-правил. `AsyncSession` остаётся деталью infrastructure и
служит практическим Unit of Work — отдельную общую UoW-абстракцию не вводим.

Используются только предметные порты: `PaymentRepository`, `OutboxRepository`,
`PaymentGateway`, `WebhookClient`, `EventPublisher`. Универсальный
`BaseRepository[T]`, CQRS/read model, Event Sourcing, mediator и дублирующая
богатая domain-модель не входят в проект.

## Типы домена

```text
PaymentStatus = pending | succeeded | failed
Currency = RUB | USD | EUR
OutboxStatus = pending | processing | published
GatewayOutcome = succeeded | failed | temporary_error
```

`failed` от gateway — подтверждённый бизнес-результат. `temporary_error`
означает, что итога ещё нет и event должен быть повторён.

## HTTP-контракт

### POST /api/v1/payments

Вход: `CreatePaymentRequest`.

```text
amount: Decimal > 0
currency: Currency
description: str
metadata: dict[str, JSON value]
webhook_url: HttpUrl
```

Верхнеуровневые неизвестные поля запрещены. Заголовки `X-API-Key` и
`Idempotency-Key` обязательны.

Выход при успехе: `CreatePaymentResponse`.

```text
payment_id: UUID
status: PaymentStatus
created_at: datetime
```

Коды: `202` для новой или идентичной повторной заявки, `401` для ключа API,
`409` при конфликтующем повторе, `422` при невалидном body.

### GET /api/v1/payments/{payment_id}

Выход: `PaymentResponse` с публичными полями input, `payment_id`, `status`,
`created_at`, `processed_at`. Внутренние счётчики, ошибки и outbox state не
возвращаются. Коды: `200`, `401`, `404`.

## Контракт хранилища

API вызывает одну атомарную операцию создания. Её observable-результат:

```text
create_payment_with_outbox(request, idempotency_key)
  -> Created(payment) | Existing(payment) | Conflict
```

Операция либо создаёт payment и `payment.created` outbox вместе, либо не создаёт
ничего. Уникальное ограничение `idempotency_key` — окончательный арбитр при
конкурентных запросах.

Relay получает и меняет состояние outbox только через операции с ограниченными
переходами:

```text
claim_pending_outbox(limit) -> list[ClaimedOutboxEvent]
mark_outbox_published(event_id, claim_token, published_at)
record_outbox_publish_failure(event_id, claim_token, error)
```

`claim_pending_outbox` захватывает строки короткой транзакцией с `FOR UPDATE
SKIP LOCKED` и переводит их из `pending` в `processing`. SQL-lock существует
только до коммита claim-транзакции; `processing` — устойчиво сохранённое
состояние, а не название самого SQL-lock.

Только relay, владеющий захваченным событием, может перевести его из
`processing` в `published` после publisher confirm. Известная ошибка публикации
возвращает событие из `processing` в `pending`. При shutdown relay прекращает
новый claim; незавершённое событие остаётся `processing` до восстановления его
lease следующим relay. Это сознательно допускает повторную публикацию после
рестарта.

Захват идентифицируется `claim_token`, а не только `event_id`. Relay передаёт
token вместе с каждой операцией финализации: запоздавший владелец не может
пометить опубликованной или вернуть в `pending` запись, которую уже повторно
захватил другой relay.

Worker читает payment state перед внешними действиями и записывает только
допустимые изменения:

```text
get_payment_state_for_handling(payment_id)
record_gateway_outcome(payment_id, status, processed_at)
record_webhook_delivery(payment_id, delivered_at)
record_webhook_failure(payment_id, attempt, error, exhausted_at?)
```

## Контракт события RabbitMQ

Exchange: `payments.exchange`. Основной routing key/queue: `payments.new`.

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
    "description": "Оплата заказа #42",
    "metadata": {"order_id": "42"},
    "webhook_url": "http://webhook-sink:8080/callbacks/payments"
  }
}
```

Event содержит снимок неизменяемых данных, поэтому worker не повторяет их read из
БД. Его status и webhook delivery state не входят в event.

Заголовок `x-payment-attempt` начинается с `1`. При передаче в retry worker
увеличивает его после подтверждённой публикации нового сообщения. Значение
используется вместе с устойчивым состоянием payments, а не вместо него.

## Контракт gateway

Worker зависит от асинхронного порта, который принимает event/payment snapshot и
возвращает один `GatewayOutcome`.

Задержка, распределение результатов обычного эмулятора и управляемые сценарии
для тестов определены в [спецификации](spec.md#обработка-платежа). Это поведение,
а не деталь программного контракта.

## Контракт webhook

Worker делает `POST` на URL из event с телом:

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

HTTP 2xx означает успех. Таймаут, сетевая ошибка и любой иной статус передают
сообщение в broker retry-путь. Webhook adapter не принимает бизнес-решения: он
только выполняет HTTP-запрос и возвращает его технический результат.

## Точки подмены в тестах

Тестам разрешено подменять только внешние границы:

- gateway outcome;
- HTTP-поведение webhook endpoint;
- время/задержки retry;
- RabbitMQ или PostgreSQL в тестах меньшего уровня.

Интеграционные тесты запускают реальные PostgreSQL и RabbitMQ. Они не должны
подменять repository или broker в сценариях, проверяющих outbox, ACK, retry и DLQ.
