import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING
from urllib.parse import quote, urlsplit
from uuid import UUID

import httpx
import pytest
from sqlalchemy import select

import payment_processor.worker.app as worker_app
from payment_processor.application.commands import CreatePaymentCommand
from payment_processor.application.events import PaymentCreatedEvent
from payment_processor.core.config import settings
from payment_processor.infrastructure.db.models import OutboxEvent, Payment, PaymentStatus
from payment_processor.infrastructure.db.repositories import SqlAlchemyPaymentRepository
from payment_processor.infrastructure.integrations.gateway_emulator import GatewayEmulator
from payment_processor.infrastructure.messaging.broker import broker
from payment_processor.infrastructure.messaging.topology import (
    PAYMENTS_EXCHANGE,
    PAYMENTS_NEW_QUEUE,
    PAYMENTS_NEW_QUEUE_NAME,
    declare_topology,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


async def clear_main_queue() -> None:
    connection = urlsplit(settings.rabbitmq_url)
    assert connection.username is not None
    assert connection.password is not None

    vhost = quote("/", safe="")
    url = f"{settings.rabbitmq_management_url}/api/queues/{vhost}/{PAYMENTS_NEW_QUEUE_NAME}/contents"
    async with httpx.AsyncClient(trust_env=False) as client:
        response = await client.delete(
            url,
            auth=(connection.username, connection.password),
        )

    assert response.status_code == httpx.codes.NO_CONTENT


async def main_queue_message_count() -> int:
    connection = urlsplit(settings.rabbitmq_url)
    assert connection.username is not None
    assert connection.password is not None

    vhost = quote("/", safe="")
    url = f"{settings.rabbitmq_management_url}/api/queues/{vhost}/{PAYMENTS_NEW_QUEUE_NAME}"
    async with httpx.AsyncClient(trust_env=False) as client:
        response = await client.get(
            url,
            auth=(connection.username, connection.password),
        )

    response.raise_for_status()
    return int(response.json()["messages"])


def immediate_gateway() -> GatewayEmulator:
    return GatewayEmulator(min_delay_seconds=0, max_delay_seconds=0)


async def wait_for_final_payment_status(
    session_factory: async_sessionmaker[AsyncSession],
    payment_id: UUID,
) -> Payment:
    deadline = asyncio.get_running_loop().time() + 5

    while asyncio.get_running_loop().time() < deadline:
        async with session_factory() as session:
            payment = await session.get(Payment, payment_id)

        assert payment is not None
        if payment.status is not PaymentStatus.PENDING:
            return payment

        await asyncio.sleep(0.05)

    pytest.fail("Worker did not persist a final payment status within five seconds")


async def create_payment_event(
    session_factory: async_sessionmaker[AsyncSession],
) -> tuple[UUID, PaymentCreatedEvent]:
    command = CreatePaymentCommand(
        amount=Decimal("1250.00"),
        currency="RUB",
        description="Payment for worker integration test",
        idempotency_key="worker-integration-test-key",
        metadata={"order_id": "worker-test"},
        webhook_url="http://webhook-sink:8080/callbacks/payments",
    )

    async with session_factory() as session:
        repository = SqlAlchemyPaymentRepository(session)
        payment = await repository.create_or_get_with_outbox(command)
        outbox_event = (await session.scalars(select(OutboxEvent))).one()

    return payment.payment_id, PaymentCreatedEvent.model_validate(outbox_event.payload)


@pytest.mark.integration
async def test_worker_consumes_event_and_persists_gateway_outcome(
    db_session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(worker_app, "session_factory", db_session_factory)
    monkeypatch.setattr(worker_app, "GatewayEmulator", immediate_gateway)

    async with broker:
        await declare_topology(broker)
    await clear_main_queue()

    payment_id, event = await create_payment_event(db_session_factory)
    expected_status = PaymentStatus.FAILED if payment_id.int % 10 == 0 else PaymentStatus.SUCCEEDED

    async with worker_app.app:
        await broker.publish(
            event,
            queue=PAYMENTS_NEW_QUEUE,
            exchange=PAYMENTS_EXCHANGE,
            persist=True,
        )
        payment = await wait_for_final_payment_status(db_session_factory, payment_id)

    assert payment.status is expected_status
    assert payment.processed_at is not None
    assert payment.processed_at <= datetime.now(tz=UTC)
    assert await main_queue_message_count() == 0
