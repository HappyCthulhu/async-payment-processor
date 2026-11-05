from collections.abc import AsyncIterator
from dataclasses import replace
from decimal import Decimal
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.schema import CreateSchema, DropSchema

from payment_processor.application.commands import CreatePaymentCommand
from payment_processor.application.errors import IdempotencyConflictError
from payment_processor.core.config import settings
from payment_processor.infrastructure.db.base import BaseModel
from payment_processor.infrastructure.db.models import OutboxEvent, OutboxStatus, Payment
from payment_processor.infrastructure.db.repositories import SqlAlchemyPaymentRepository


@pytest_asyncio.fixture
async def db_session_factory() -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    schema_name = f"test_{uuid4().hex}"
    engine = create_async_engine(
        settings.database_url,
        connect_args={"server_settings": {"search_path": schema_name}},
    )

    async with engine.begin() as connection:
        await connection.execute(CreateSchema(schema_name))
        await connection.run_sync(BaseModel.metadata.create_all)

    try:
        yield async_sessionmaker(engine, expire_on_commit=False)
    finally:
        async with engine.begin() as connection:
            await connection.execute(DropSchema(schema_name, cascade=True))
        await engine.dispose()


@pytest.mark.integration
async def test_create_payment_persists_outbox_and_replay_is_idempotent(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    command = CreatePaymentCommand(
        amount=Decimal("1250.00"),
        currency="RUB",
        description="Payment for order #42",
        metadata={"order_id": "42"},
        webhook_url="http://webhook-sink:8080/callbacks/payments",
        idempotency_key="repository-test-key",
    )

    async with db_session_factory() as session:
        first_result = await SqlAlchemyPaymentRepository(session).create_or_get_with_outbox(command)

    async with db_session_factory() as session:
        second_result = await SqlAlchemyPaymentRepository(session).create_or_get_with_outbox(command)
        payments = list((await session.scalars(select(Payment))).all())
        events = list((await session.scalars(select(OutboxEvent))).all())

    assert first_result == second_result
    assert len(payments) == 1
    assert len(events) == 1

    event = events[0]
    assert event.payment_id == first_result.payment_id
    assert event.status is OutboxStatus.PENDING
    assert event.payload == {
        "event_id": str(event.id),
        "event_version": 1,
        "event_type": "payment.created",
        "payment_id": str(first_result.payment_id),
        "created_at": first_result.created_at.isoformat(),
        "payment": {
            "amount": "1250.00",
            "currency": "RUB",
            "description": "Payment for order #42",
            "metadata": {"order_id": "42"},
            "webhook_url": "http://webhook-sink:8080/callbacks/payments",
        },
    }


@pytest.mark.integration
async def test_replay_with_changed_payload_raises_conflict_and_preserves_existing_payment(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    original_command = CreatePaymentCommand(
        amount=Decimal("1250.00"),
        currency="RUB",
        description="Payment for order #42",
        metadata={"order_id": "42"},
        webhook_url="http://webhook-sink:8080/callbacks/payments",
        idempotency_key="repository-conflict-key",
    )
    conflicting_command = replace(original_command, amount=Decimal("1300.00"))

    async with db_session_factory() as session:
        await SqlAlchemyPaymentRepository(session).create_or_get_with_outbox(original_command)

    async with db_session_factory() as session:
        with pytest.raises(IdempotencyConflictError):
            await SqlAlchemyPaymentRepository(session).create_or_get_with_outbox(conflicting_command)

    async with db_session_factory() as session:
        payments = list((await session.scalars(select(Payment))).all())
        events = list((await session.scalars(select(OutboxEvent))).all())

    assert len(payments) == 1
    assert payments[0].amount == Decimal("1250.00")
    assert len(events) == 1
