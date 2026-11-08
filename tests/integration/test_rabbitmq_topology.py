from urllib.parse import quote, urlsplit

import httpx
import pytest

from payment_processor.core.config import settings
from payment_processor.infrastructure.messaging.broker import broker
from payment_processor.infrastructure.messaging.topology import (
    NEW_PAYMENTS_Q_NAME,
    PAYMENTS_EXCHANGE_NAME,
    declare_topology,
)


@pytest.mark.integration
async def test_declare_topology_creates_payments_exchange_queue_and_binding() -> None:
    """The durable payment route must exist before relay or worker start."""
    connection = urlsplit(settings.rabbitmq_url)
    assert connection.username is not None
    assert connection.password is not None

    async with broker:
        await declare_topology(broker)

    vhost = quote("/", safe="")
    async with httpx.AsyncClient(trust_env=False) as client:
        exchange_response = await client.get(
            f"{settings.rabbitmq_management_url}/api/exchanges/{vhost}/{PAYMENTS_EXCHANGE_NAME}",
            auth=(connection.username, connection.password),
        )
        queue_response = await client.get(
            f"{settings.rabbitmq_management_url}/api/queues/{vhost}/{NEW_PAYMENTS_Q_NAME}",
            auth=(connection.username, connection.password),
        )
        bindings_response = await client.get(
            f"{settings.rabbitmq_management_url}/api/bindings/{vhost}/e/{PAYMENTS_EXCHANGE_NAME}/q/{NEW_PAYMENTS_Q_NAME}",
            auth=(connection.username, connection.password),
        )

    assert exchange_response.status_code == httpx.codes.OK
    assert exchange_response.json()["type"] == "direct"
    assert exchange_response.json()["durable"] is True

    assert queue_response.status_code == httpx.codes.OK
    assert queue_response.json()["durable"] is True

    assert bindings_response.status_code == httpx.codes.OK
    assert any(binding["routing_key"] == NEW_PAYMENTS_Q_NAME for binding in bindings_response.json())
