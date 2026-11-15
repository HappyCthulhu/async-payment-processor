from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, cast
from uuid import uuid4

import pytest
from aio_pika import AMQPException

from payment_processor.application.errors import EventPublishError
from payment_processor.application.events import PaymentCreatedEvent, PaymentSnapshot
from payment_processor.infrastructure.messaging.publisher import RabbitRetryPublisher
from payment_processor.infrastructure.messaging.topology import (
    PAYMENTS_EXCHANGE,
    PAYMENTS_RETRY_1_QUEUE,
    PAYMENTS_RETRY_2_QUEUE,
)

if TYPE_CHECKING:
    from faststream.rabbit import RabbitBroker


class RecordingBroker:
    def __init__(self) -> None:
        self.published: list[tuple[PaymentCreatedEvent, dict[str, object]]] = []

    async def publish(self, message: PaymentCreatedEvent, **kwargs: object) -> None:
        self.published.append((message, kwargs))


class FailingBroker:
    def __init__(self, error: Exception) -> None:
        self._error = error

    async def publish(self, message: PaymentCreatedEvent, **kwargs: object) -> None:
        del message, kwargs
        raise self._error


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


@pytest.mark.parametrize(
    ("retry_attempt", "expected_queue"),
    [(2, PAYMENTS_RETRY_1_QUEUE), (3, PAYMENTS_RETRY_2_QUEUE)],
)
async def test_retry_publisher_routes_persistent_event_to_its_retry_queue(
    retry_attempt: int,
    expected_queue: object,
) -> None:
    broker = RecordingBroker()
    event = payment_created_event()
    publisher = RabbitRetryPublisher(cast("RabbitBroker", broker))

    await publisher.publish(event, retry_attempt)

    assert broker.published == [
        (
            event,
            {
                "queue": expected_queue,
                "exchange": PAYMENTS_EXCHANGE,
                "persist": True,
                "headers": {"x-payment-attempt": retry_attempt},
            },
        )
    ]


async def test_retry_publisher_rejects_an_unsupported_attempt_number() -> None:
    publisher = RabbitRetryPublisher(cast("RabbitBroker", RecordingBroker()))

    with pytest.raises(ValueError, match="Unsupported retry attempt: 4"):
        await publisher.publish(payment_created_event(), retry_attempt=4)


@pytest.mark.parametrize("broker_error", [AMQPException("RabbitMQ unavailable"), TimeoutError()])
async def test_retry_publisher_wraps_a_broker_failure(broker_error: Exception) -> None:
    publisher = RabbitRetryPublisher(cast("RabbitBroker", FailingBroker(broker_error)))

    with pytest.raises(EventPublishError) as error:
        await publisher.publish(payment_created_event(), retry_attempt=2)

    assert error.value.__cause__ is broker_error
