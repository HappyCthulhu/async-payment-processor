# ADR 004: облегчённая hexagonal architecture

## Статус

Принято.

## Контекст

Сервис мал, но имеет несколько внешних границ: HTTP API, PostgreSQL, RabbitMQ,
gateway emulator и webhook endpoint. Смешивание этих деталей в router или
FastStream handler усложнит тесты и сделает правила обработки платежа зависимыми
от фреймворков. Полная Clean Architecture с отдельной богатой domain-моделью,
generic repository, mediator и отдельным DI-контейнером добавит дублирование без
пользы для данного объёма.

## Решение

Используем облегчённые ports and adapters:

- `application` содержит use case'ы и `Protocol`-порты;
- `domain` содержит только типы и чистые правила payment;
- `infrastructure` реализует порты через SQLAlchemy, FastStream и `httpx`;
- `api` и `worker` — entrypoint'ы, которые валидируют транспортный ввод и
  вызывают use case;
- зависимости собираются явно в composition root.

`AsyncSession` является практическим Unit of Work в infrastructure. ORM-модель
`Payment` не дублируется отдельной domain-сущностью.

## Последствия

Use case'ы можно проверять с fake-адаптерами, а интеграционные тесты — с
реальными PostgreSQL и RabbitMQ. Структура позволяет заменить gateway или
webhook transport без изменения бизнес-сценария, но не вводит лишних уровней
абстракции.
