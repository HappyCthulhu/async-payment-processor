import json
from typing import TYPE_CHECKING
from urllib.parse import quote, urlsplit
from uuid import UUID

import httpx
import pytest
from fastapi import status
from sqlalchemy import select

from payment_processor.core.config import settings
from payment_processor.infrastructure.db.models import OutboxEvent, OutboxStatus
from payment_processor.infrastructure.messaging.broker import broker
from payment_processor.infrastructure.messaging.publisher import RabbitEventPublisher
from payment_processor.infrastructure.messaging.relay_runner import OutboxRelayRunner
from payment_processor.infrastructure.messaging.topology import NEW_PAYMENTS_Q_NAME, declare_topology

if TYPE_CHECKING:
    from httpx import AsyncClient
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


PAYMENTS_PATH = "/api/v1/payments"
PAYMENT_REQUEST = {
    "amount": "1250.00",
    "currency": "RUB",
    "description": "Payment for relay integration",
    "metadata": {"order_id": "relay-42"},
    "webhook_url": "http://webhook-sink:8080/callbacks/payments",
}


def payment_headers() -> dict[str, str]:
    return {
        "X-API-Key": settings.api_key,
        "Idempotency-Key": "relay-integration-key",
    }


@pytest.mark.integration
async def test_relay_publishes_created_payment_event_and_marks_outbox_row_published(
    client: AsyncClient,
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    connection = urlsplit(settings.rabbitmq_url)
    assert connection.username is not None
    assert connection.password is not None

    vhost = quote("/", safe="")
    auth = (connection.username, connection.password)
    async with broker:
        await declare_topology(broker)

        async with httpx.AsyncClient(trust_env=False) as management_client:
            clear_response = await management_client.delete(
                f"{settings.rabbitmq_management_url}/api/queues/{vhost}/{NEW_PAYMENTS_Q_NAME}/contents",
                auth=auth,
            )
        assert clear_response.status_code == httpx.codes.NO_CONTENT

        response = await client.post(PAYMENTS_PATH, headers=payment_headers(), json=PAYMENT_REQUEST)
        assert response.status_code == status.HTTP_202_ACCEPTED
        payment_id = UUID(response.json()["payment_id"])

        async with db_session_factory() as session:
            outbox_event = (await session.scalars(select(OutboxEvent))).one()
            assert outbox_event.status is OutboxStatus.PENDING

        relay = OutboxRelayRunner(
            session_factory=db_session_factory,
            publisher=RabbitEventPublisher(broker),
            poll_interval_seconds=1,
        )
        await relay.run_once(limit=10)

    async with db_session_factory() as session:
        outbox_event = (await session.scalars(select(OutboxEvent))).one()

    assert outbox_event.status is OutboxStatus.PUBLISHED
    assert outbox_event.published_at is not None
    assert outbox_event.claim_token is None
    assert outbox_event.lease_expires_at is None

    async with httpx.AsyncClient(trust_env=False) as management_client:
        messages_response = await management_client.post(
            f"{settings.rabbitmq_management_url}/api/queues/{vhost}/{NEW_PAYMENTS_Q_NAME}/get",
            auth=auth,
            json={"count": 1, "ackmode": "ack_requeue_false", "encoding": "auto", "truncate": 50_000},
        )

    assert messages_response.status_code == httpx.codes.OK
    messages = messages_response.json()
    assert len(messages) == 1
    assert messages[0]["exchange"] == "payments.exchange"
    assert messages[0]["routing_key"] == NEW_PAYMENTS_Q_NAME
    assert json.loads(messages[0]["payload"])["payment_id"] == str(payment_id)
