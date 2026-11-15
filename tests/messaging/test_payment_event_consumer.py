from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, cast, override
from uuid import uuid4

import pytest

from payment_processor.application.errors import GatewayTemporaryError, WebhookDeliveryError
from payment_processor.application.events import PaymentCreatedEvent, PaymentSnapshot
from payment_processor.application.ports import PaymentEventHandler, RetryPublisher
from payment_processor.worker.consumer import PaymentEventConsumer

if TYPE_CHECKING:
    from faststream.rabbit import RabbitMessage


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


async def test_consumer_keeps_original_message_unacknowledged_when_retry_publish_fails() -> None:
    retry_publish_error = RuntimeError("RabbitMQ is unavailable")
    use_case = RecordingHandlePaymentEventUseCase(GatewayTemporaryError())
    retry_publisher = RecordingRetryPublisher(error=retry_publish_error)
    message = RecordingMessage(attempt=1)
    consumer = PaymentEventConsumer(use_case=use_case, retry_publisher=retry_publisher)

    with pytest.raises(RuntimeError, match="RabbitMQ is unavailable"):
        await consumer.consume(event=payment_created_event(), message=cast("RabbitMessage", message))

    assert retry_publisher.published_events == []
    assert message.acknowledged is False
    assert message.rejected_with == []


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
