# Async Payment Processor

Учебная реализация тестового задания: сервис принимает платёж, обрабатывает его
асинхронно и сообщает итог через webhook.

## Документация проекта

- [Спецификация](docs/spec.md): API, данные, инварианты и сценарии обработки.
- [Архитектура](docs/architecture.md): компоненты, очереди и последовательности.
- [ADR](docs/adr/): решения по outbox, повторной доставке и retry/DLQ.
- [Тест-кейсы](docs/test-cases.md): проверяемое поведение до написания
  автоматических тестов.
- [Контракты компонентов](docs/contracts.md): границы между API, БД, relay и
  worker.

`docs/spec.md` — единственный источник бизнес-правил. Остальные документы не
пересказывают их: architecture показывает связи компонентов, contracts — формы
данных и интерфейсы, ADR — причины решений, test-cases — способ проверки.

## Быстрый запуск

Нужны Docker и Docker Compose.
Стенд предназначен для локальной разработки: опубликованные порты доступны только
на `127.0.0.1`, а demo UI использует настроенный ключ API.

Из чистого клона:

```bash
cp .env.example .env
docker compose up -d --build
docker compose ps
```

Compose поднимает PostgreSQL и RabbitMQ, применяет миграции отдельным заданием,
запускает API с outbox relay в lifespan, worker и demo webhook-sink. Миграция
успешно завершается с кодом 0. Для остановки без удаления данных: `docker compose
down`; `docker compose down -v` также удаляет локальные данные. При наличии
`just` доступны `just up`, `just down` и `just logs`.

- Демо-форма и полученные callbacks: [http://localhost:8080](http://localhost:8080).
- Swagger: [http://localhost:8000/docs](http://localhost:8000/docs).
- RabbitMQ Management: [http://localhost:15672](http://localhost:15672), по
  умолчанию `payment_broker` / `payment_broker_password`.
- JSON полученных уведомлений: [http://localhost:8080/callbacks](http://localhost:8080/callbacks).

Создание и проверка платежа без формы (используйте новый ключ для нового платежа):

```bash
curl -sS -X POST http://localhost:8000/api/v1/payments \
  -H 'Content-Type: application/json' -H 'X-API-Key: local-development-key' \
  -H 'Idempotency-Key: example-1' \
  -d '{"amount":"1250.00","currency":"RUB","description":"Order 42","metadata":{"order_id":"42"},"webhook_url":"http://webhook-sink:8080/callbacks/payments"}'
# Подставьте payment_id из ответа:
curl -sS -H 'X-API-Key: local-development-key' http://localhost:8000/api/v1/payments/PAYMENT_ID
curl -sS http://localhost:8080/callbacks
```

Адрес `webhook-sink` доступен внутри сети Compose. Обычный gateway завершает
обработку за 2–5 секунд; затем появляется callback. Для демонстрации двух ошибок
доставки и успеха задайте в `.env` `WEBHOOK_RESPONSE_SEQUENCE=500,500,204` и
пересоздайте sink командой `docker compose up -d webhook-sink`. Последовательность
считается отдельно для каждого payment; после её конца повторяется последний код.
История callbacks и счётчики sink хранятся только в памяти и сбрасываются при
перезапуске. Это локальный демонстрационный сервис, не production endpoint.
Sink только принимает HTTP-уведомления как система клиента; эмулятор платёжного
gateway находится внутри worker.

Чтобы увидеть DLQ, задайте `WEBHOOK_RESPONSE_SEQUENCE=500,500,500`, пересоздайте
sink и создайте новый платёж. После трёх попыток его сообщение появится в очереди
`payments.dlq` в RabbitMQ Management (Queues and Streams). Сам платёж при этом
сохраняет результат gateway. Результат `failed` определяется идентификатором
платежа и вручную в форме не выбирается; создать платежи до появления `failed`
можно через форму или проверить сценарий детерминированно в тестах gateway.

Для разработки на хосте нужны Python 3.14, `uv` и `just`:

```bash
uv sync --locked
just test                 # запускает PostgreSQL и отдельный тестовый RabbitMQ, затем pytest
uv run ruff check .
uv run ty check
```

## Локальная инфраструктура

Скопируй `.env.example` в `.env` и при необходимости измени значения. Локальная
PostgreSQL и RabbitMQ запускаются одной командой `just db-up`. PostgreSQL по
умолчанию доступен на порту `5433`, чтобы не конфликтовать с PostgreSQL на
`5432`. После запуска можно выполнять Alembic-команды из `justfile`.
`APP_DB_ECHO` по умолчанию выключен: включайте его только при осознанной
отладке, поскольку SQL-логи могут содержать webhook URL и metadata платежа.

AMQP RabbitMQ доступен на порту `5672`, а Management UI — на
[http://localhost:15672](http://localhost:15672). Логин и пароль находятся в
`.env`; команда `just rabbit-ui` напомнит адрес и логин. `just rabbit-up`
остаётся доступной, если нужно поднять только брокер.

`just infra-up` остаётся алиасом `db-up`. `just test` запускает PostgreSQL и
отдельный непостоянный `rabbitmq-test` (AMQP `5673`, Management `15673` по
умолчанию); тесты используют изолированные схемы PostgreSQL и тестовый broker,
поэтому не очищают очереди demo. Порты тестового broker можно изменить через
`TEST_RABBITMQ_PORT` и `TEST_RABBITMQ_MANAGEMENT_PORT` в `.env`. После `just up`
можно выполнить `just test` и проверить, что demo продолжает работать и его
очереди в RabbitMQ Management не изменились из-за тестов. Тестовый сервис не
входит в обычный запуск; удалить только его можно командой
`docker compose rm -sf rabbitmq-test`.
