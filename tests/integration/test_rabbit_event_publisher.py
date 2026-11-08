from urllib.parse import quote, urlsplit
from uuid import uuid4

import httpx
import pytest

from payment_processor.application.events import PaymentCreatedEvent, PaymentSnapshot
from payment_processor.core.config import settings
from payment_processor.infrastructure.messaging.broker import broker
from payment_processor.infrastructure.messaging.publisher import RabbitEventPublisher
from payment_processor.infrastructure.messaging.topology import NEW_PAYMENTS_Q_NAME, declare_topology

PERSISTENT_DELIVERY_MODE = 2


@pytest.mark.integration
async def test_publisher_sends_a_persistent_payment_created_event_to_main_queue() -> None:
    """Relay needs a confirmed, persistent message in the route consumed by the worker."""
    connection = urlsplit(settings.rabbitmq_url)
    assert connection.username is not None
    assert connection.password is not None

    event = PaymentCreatedEvent(
        event_id=uuid4(),
        event_type="payment.created",
        event_version=1,
        payment_id=uuid4(),
        created_at="2026-11-02T12:00:00Z",
        payment=PaymentSnapshot(
            amount="1250.00",
            currency="RUB",
            description="Payment for order #42",
            metadata={"order_id": "42"},
            webhook_url="http://webhook-sink:8080/callbacks/payments",
        ),
    )

    vhost = quote("/", safe="")
    auth = (connection.username, connection.password)
    async with httpx.AsyncClient(trust_env=False) as client:
        clear_response = await client.delete(
            f"{settings.rabbitmq_management_url}/api/queues/{vhost}/{NEW_PAYMENTS_Q_NAME}/contents",
            auth=auth,
        )
    assert clear_response.status_code == httpx.codes.NO_CONTENT

    async with broker:
        await declare_topology(broker)
        await RabbitEventPublisher(broker).publish(event)

    async with httpx.AsyncClient(trust_env=False) as client:
        messages_response = await client.post(
            f"{settings.rabbitmq_management_url}/api/queues/{vhost}/{NEW_PAYMENTS_Q_NAME}/get",
            auth=auth,
            json={"count": 1, "ackmode": "ack_requeue_false", "encoding": "auto", "truncate": 50_000},
        )

    assert messages_response.status_code == httpx.codes.OK
    messages = messages_response.json()
    assert len(messages) == 1
    assert messages[0]["exchange"] == "payments.exchange"
    assert messages[0]["routing_key"] == NEW_PAYMENTS_Q_NAME
    assert messages[0]["properties"]["delivery_mode"] == PERSISTENT_DELIVERY_MODE
    assert messages[0]["payload"] == event.model_dump_json()
