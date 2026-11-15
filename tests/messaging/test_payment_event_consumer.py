import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast, override
from uuid import uuid4

import pytest
from faststream import AckPolicy, FastStream
from faststream.rabbit import Channel, RabbitBroker, RabbitMessage, RabbitQueue
from loguru import logger

import payment_processor.worker.app as worker_app
from payment_processor.application.errors import GatewayTemporaryError, WebhookDeliveryError
from payment_processor.application.events import PaymentCreatedEvent, PaymentSnapshot
from payment_processor.application.ports import PaymentEventHandler, RetryPublisher
from payment_processor.core.config import settings
from payment_processor.worker.consumer import PaymentEventConsumer


class RecordingHandlePaymentEventUseCase(PaymentEventHandler):
    def __init__(self, error: Exception | None = None) -> None:
        self._error = error
        self.calls: list[tuple[PaymentCreatedEvent, int]] = []

    @override
    async def execute(self, event: PaymentCreatedEvent, attempt: int) -> None:
        self.calls.append((event, attempt))
        if self._error is not None:
            raise self._error


class RecordingRetryPublisher(RetryPublisher):
    def __init__(self, error: Exception | None = None) -> None:
        self._error = error
        self.published_events: list[tuple[PaymentCreatedEvent, int]] = []

    @override
    async def publish(self, event: PaymentCreatedEvent, retry_attempt: int) -> None:
        if self._error is not None:
            raise self._error
        self.published_events.append((event, retry_attempt))


class RecordingMessage:
    def __init__(self, attempt: int | str | None = None) -> None:
        self.headers = {} if attempt is None else {"x-payment-attempt": attempt}
        self.acknowledged = False
        self.rejected_with: list[bool] = []

    async def ack(self) -> None:
        self.acknowledged = True

    async def reject(self, *, requeue: bool) -> None:
        self.rejected_with.append(requeue)


def payment_created_event() -> PaymentCreatedEvent:
    return PaymentCreatedEvent(
        event_id=uuid4(),
        event_type="payment.created",
        event_version=1,
        payment_id=uuid4(),
        created_at=datetime(2026, 11, 2, 12, 0, tzinfo=UTC),
        payment=PaymentSnapshot(
            amount=Decimal("1250.00"),
            currency="RUB",
            description="Payment for order #42",
            metadata={"order_id": "42"},
            webhook_url="http://webhook-sink:8080/callbacks/payments",
        ),
    )


async def test_consumer_acknowledges_a_successfully_handled_event() -> None:
    use_case = RecordingHandlePaymentEventUseCase()
    retry_publisher = RecordingRetryPublisher()
    message = RecordingMessage()
    event = payment_created_event()
    consumer = PaymentEventConsumer(use_case=use_case, retry_publisher=retry_publisher)

    await consumer.consume(event=event, message=cast("RabbitMessage", message))

    assert use_case.calls == [(event, 1)]
    assert retry_publisher.published_events == []
    assert message.acknowledged is True
    assert message.rejected_with == []


@pytest.mark.parametrize(
    ("current_attempt", "retry_attempt"),
    [(None, 2), (1, 2), (2, 3), ("2", 3)],
)
async def test_consumer_publishes_temporary_failures_to_the_next_retry_queue(
    current_attempt: int | str | None,
    retry_attempt: int,
) -> None:
    use_case = RecordingHandlePaymentEventUseCase(GatewayTemporaryError())
    retry_publisher = RecordingRetryPublisher()
    message = RecordingMessage(attempt=current_attempt)
    event = payment_created_event()
    consumer = PaymentEventConsumer(use_case=use_case, retry_publisher=retry_publisher)

    await consumer.consume(event=event, message=cast("RabbitMessage", message))

    expected_attempt = 1 if current_attempt is None else int(current_attempt)
    assert use_case.calls == [(event, expected_attempt)]
    assert retry_publisher.published_events == [(event, retry_attempt)]
    assert message.acknowledged is True
    assert message.rejected_with == []


@pytest.mark.parametrize("attempt", [3, 4])
async def test_consumer_rejects_an_exhausted_temporary_failure_for_dead_lettering(attempt: int) -> None:
    use_case = RecordingHandlePaymentEventUseCase(GatewayTemporaryError())
    retry_publisher = RecordingRetryPublisher()
    message = RecordingMessage(attempt=attempt)
    event = payment_created_event()
    consumer = PaymentEventConsumer(use_case=use_case, retry_publisher=retry_publisher)

    await consumer.consume(event=event, message=cast("RabbitMessage", message))

    assert use_case.calls == [(event, attempt)]
    assert retry_publisher.published_events == []
    assert message.acknowledged is False
    assert message.rejected_with == [False]


async def test_consumer_requeues_original_message_when_retry_publish_fails() -> None:
    retry_publish_error = RuntimeError("RabbitMQ is unavailable")
    use_case = RecordingHandlePaymentEventUseCase(GatewayTemporaryError())
    retry_publisher = RecordingRetryPublisher(error=retry_publish_error)
    message = RecordingMessage(attempt=1)
    consumer = PaymentEventConsumer(use_case=use_case, retry_publisher=retry_publisher)

    event = payment_created_event()
    await consumer.consume(event=event, message=cast("RabbitMessage", message))

    assert retry_publisher.published_events == []
    assert message.acknowledged is False
    assert message.rejected_with == [True]

    retry_publisher._error = None
    redelivery = RecordingMessage(attempt=1)
    await consumer.consume(event=event, message=cast("RabbitMessage", redelivery))
    assert retry_publisher.published_events == [(event, 2)]
    assert redelivery.acknowledged is True


async def test_consumer_requeues_after_unexpected_failure_and_handles_redelivery() -> None:
    use_case = RecordingHandlePaymentEventUseCase(RuntimeError("database unavailable"))
    consumer = PaymentEventConsumer(use_case=use_case, retry_publisher=RecordingRetryPublisher())
    event = payment_created_event()
    first = RecordingMessage()
    await consumer.consume(event=event, message=cast("RabbitMessage", first))
    assert first.rejected_with == [True]
    assert first.acknowledged is False

    use_case._error = None
    redelivery = RecordingMessage()
    await consumer.consume(event=event, message=cast("RabbitMessage", redelivery))
    assert redelivery.acknowledged is True


@pytest.mark.integration
@pytest.mark.parametrize("failure", ["publisher", "handler"])
async def test_worker_releases_prefetch_and_continues_after_transient_error(failure: str) -> None:
    completed = asyncio.Event()
    calls = 0
    publish_calls = 0
    second_call = 2
    third_call = 3

    class FailingOnceHandler:
        async def execute(self, event: PaymentCreatedEvent, attempt: int) -> None:
            nonlocal calls
            assert event.event_id is not None
            assert attempt == 1
            calls += 1
            if failure == "handler" and calls == 1:
                raise RuntimeError("temporary database error")
            if failure == "publisher" and calls <= second_call:
                raise GatewayTemporaryError
            if calls == third_call:
                completed.set()

    class FailingOncePublisher:
        async def publish(self, event: PaymentCreatedEvent, retry_attempt: int) -> None:
            nonlocal publish_calls
            assert event.event_id is not None
            assert retry_attempt == second_call
            publish_calls += 1
            if publish_calls == 1:
                raise RuntimeError("temporary publish error")

    broker = RabbitBroker(settings.rabbitmq_url)
    queue = RabbitQueue(f"test.consumer.{uuid4().hex}", auto_delete=True)
    consumer = PaymentEventConsumer(FailingOnceHandler(), FailingOncePublisher())

    @broker.subscriber(queue, ack_policy=AckPolicy.MANUAL, channel=Channel(prefetch_count=1))
    async def handle(event: PaymentCreatedEvent, message: RabbitMessage) -> None:
        await consumer.consume(event, message)

    async with FastStream(broker):
        event = payment_created_event()
        await broker.publish(event, queue=queue)
        await broker.publish(event, queue=queue)
        await asyncio.wait_for(completed.wait(), timeout=5)
    assert calls == third_call


@pytest.mark.integration
async def test_subscriber_requeues_when_session_setup_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    completed = asyncio.Event()
    factory_calls = 0
    handled = 0

    @asynccontextmanager
    async def session_factory() -> AsyncGenerator[object]:
        nonlocal factory_calls
        factory_calls += 1
        if factory_calls == 1:
            raise ConnectionError("database temporarily unavailable")
        yield object()

    class SuccessfulHandler:
        async def execute(self, event: PaymentCreatedEvent, attempt: int) -> None:
            nonlocal handled
            assert event.event_id is not None
            assert attempt == 1
            handled += 1
            if handled == 2:  # noqa: PLR2004
                completed.set()

    consumer = PaymentEventConsumer(SuccessfulHandler(), RecordingRetryPublisher())

    def build_consumer(session: object) -> PaymentEventConsumer:
        assert session is not None
        return consumer

    monkeypatch.setattr(worker_app, "session_factory", session_factory)
    monkeypatch.setattr(worker_app, "build_payment_event_consumer", build_consumer)
    broker = RabbitBroker(settings.rabbitmq_url)
    queue = RabbitQueue(f"test.subscriber.{uuid4().hex}", auto_delete=True)

    @broker.subscriber(queue, ack_policy=AckPolicy.MANUAL, channel=Channel(prefetch_count=1))
    async def handle(event: PaymentCreatedEvent, message: RabbitMessage) -> None:
        await worker_app.consume_payment_created_event(event, message)

    async with FastStream(broker):
        event = payment_created_event()
        await broker.publish(event, queue=queue)
        await broker.publish(event, queue=queue)
        await asyncio.wait_for(completed.wait(), timeout=5)
    assert factory_calls == 3  # noqa: PLR2004


async def test_consumer_retries_a_webhook_delivery_error() -> None:
    use_case = RecordingHandlePaymentEventUseCase(WebhookDeliveryError("Webhook request failed"))
    retry_publisher = RecordingRetryPublisher()
    message = RecordingMessage(attempt=1)
    event = payment_created_event()
    consumer = PaymentEventConsumer(use_case=use_case, retry_publisher=retry_publisher)

    await consumer.consume(event=event, message=cast("RabbitMessage", message))

    assert use_case.calls == [(event, 1)]
    assert retry_publisher.published_events == [(event, 2)]
    assert message.acknowledged is True
    assert message.rejected_with == []


async def test_retry_log_excludes_error_details_and_event_payload() -> None:
    logs: list[str] = []
    sink_id = logger.add(lambda message: logs.append(str(message)), format="{message}")
    try:
        event = payment_created_event()
        consumer = PaymentEventConsumer(
            use_case=RecordingHandlePaymentEventUseCase(WebhookDeliveryError("sensitive-error-detail")),
            retry_publisher=RecordingRetryPublisher(),
        )
        await consumer.consume(event, cast("RabbitMessage", RecordingMessage()))
    finally:
        logger.remove(sink_id)

    output = "".join(logs)
    assert "event=payment_retry_published" in output
    assert str(event.payment_id) in output
    assert "sensitive-error-detail" not in output
    assert "webhook-sink" not in output
    assert "order_id" not in output
