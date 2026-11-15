import asyncio
from typing import TYPE_CHECKING
from uuid import UUID

import pytest
from fastapi import status
from sqlalchemy import select

import payment_processor.api.lifespan as lifespan_module
from payment_processor.application.errors import EventPublishError
from payment_processor.core.config import settings
from payment_processor.infrastructure.db.models import OutboxEvent, OutboxStatus

if TYPE_CHECKING:
    from fastapi import FastAPI
    from httpx import AsyncClient
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


PAYMENTS_PATH = "/api/v1/payments"
POLL_INTERVAL_SECONDS = 0.01
RELAY_TIMEOUT_SECONDS = 3
PAYMENT_REQUEST = {
    "amount": "1250.00",
    "currency": "RUB",
    "description": "Payment processed by the background relay",
    "metadata": {"order_id": "lifespan-42"},
    "webhook_url": "http://webhook-sink:8080/callbacks/payments",
}


async def wait_until_published(
    db_session_factory: async_sessionmaker[AsyncSession],
    event_id: UUID,
) -> OutboxEvent:
    async with asyncio.timeout(RELAY_TIMEOUT_SECONDS):
        while True:
            async with db_session_factory() as session:
                event = await session.get(OutboxEvent, event_id)

            assert event is not None
            if event.status is OutboxStatus.PUBLISHED:
                return event

            await asyncio.sleep(POLL_INTERVAL_SECONDS)


@pytest.mark.integration
async def test_lifespan_starts_relay_that_publishes_new_outbox_events(
    app: FastAPI,
    client: AsyncClient,
    db_session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(lifespan_module, "session_factory", db_session_factory)
    monkeypatch.setattr(settings, "relay_poll_interval_seconds", POLL_INTERVAL_SECONDS)

    async with lifespan_module.lifespan(app):
        response = await client.post(
            PAYMENTS_PATH,
            headers={"X-API-Key": settings.api_key, "Idempotency-Key": "lifespan-relay-key"},
            json=PAYMENT_REQUEST,
        )

        assert response.status_code == status.HTTP_202_ACCEPTED

        async with db_session_factory() as session:
            outbox_event = (await session.scalars(select(OutboxEvent))).one()

        published_event = await wait_until_published(db_session_factory, outbox_event.id)

    assert published_event.claim_token is None
    assert published_event.lease_expires_at is None
    assert published_event.published_at is not None


@pytest.mark.integration
async def test_api_accepts_payments_during_broker_outage_and_relay_recovers(
    app: FastAPI,
    client: AsyncClient,
    db_session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker_available = asyncio.Event()
    connect_attempted = asyncio.Event()
    publish_failed = asyncio.Event()

    class UnavailableBroker:
        async def __aenter__(self) -> None:
            connect_attempted.set()
            if not broker_available.is_set():
                raise ConnectionError("broker unavailable")

        async def __aexit__(self, *args: object) -> None:
            pass

    class RecoveringPublisher:
        def __init__(self, broker: object) -> None:
            pass

        async def publish(self, _event: object) -> None:
            if not publish_failed.is_set():
                publish_failed.set()
                raise EventPublishError("broker temporarily unavailable")

    async def declare_fake_topology(broker: object) -> None:
        pass

    monkeypatch.setattr(lifespan_module, "broker", UnavailableBroker())
    monkeypatch.setattr(lifespan_module, "declare_topology", declare_fake_topology)
    monkeypatch.setattr(lifespan_module, "RabbitEventPublisher", RecoveringPublisher)
    monkeypatch.setattr(lifespan_module, "session_factory", db_session_factory)
    monkeypatch.setattr(settings, "relay_poll_interval_seconds", POLL_INTERVAL_SECONDS)

    async with lifespan_module.lifespan(app):
        await asyncio.wait_for(connect_attempted.wait(), RELAY_TIMEOUT_SECONDS)
        response = await client.post(
            PAYMENTS_PATH,
            headers={"X-API-Key": settings.api_key, "Idempotency-Key": "outage-key"},
            json=PAYMENT_REQUEST,
        )
        assert response.status_code == status.HTTP_202_ACCEPTED
        async with db_session_factory() as session:
            event = (await session.scalars(select(OutboxEvent))).one()
            assert event.status is OutboxStatus.PENDING
            event_id = event.id

        broker_available.set()
        published_event = await wait_until_published(db_session_factory, event_id)
        assert publish_failed.is_set()
        assert published_event.publish_attempts > 1
