from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import select

from payment_processor.application.commands import CreatePaymentCommand
from payment_processor.infrastructure.db.models import OutboxEvent, OutboxStatus
from payment_processor.infrastructure.db.repositories import SqlAlchemyOutboxRepository, SqlAlchemyPaymentRepository

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


EVENT_COUNT = 3
BATCH_SIZE = 2


def create_payment_command(index: int) -> CreatePaymentCommand:
    return CreatePaymentCommand(
        amount=Decimal("1250.00"),
        currency="RUB",
        description=f"Payment for order #{index}",
        metadata={"order_id": str(index)},
        webhook_url="http://webhook-sink:8080/callbacks/payments",
        idempotency_key=f"outbox-repository-test-{index}",
    )


async def create_outbox_events(
    db_session_factory: async_sessionmaker[AsyncSession],
    count: int,
) -> list[OutboxEvent]:
    async with db_session_factory() as session:
        repository = SqlAlchemyPaymentRepository(session)
        for index in range(count):
            await repository.create_or_get_with_outbox(create_payment_command(index))

        return list((await session.scalars(select(OutboxEvent).order_by(OutboxEvent.created_at))).all())


@pytest.mark.integration
async def test_claim_marks_a_limited_batch_as_processing_with_a_fresh_lease(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    events = await create_outbox_events(db_session_factory, count=EVENT_COUNT)
    before_claim = datetime.now(UTC)

    async with db_session_factory() as session:
        claimed = await SqlAlchemyOutboxRepository(session).claim_pending_events(limit=BATCH_SIZE)

    assert len(claimed) == BATCH_SIZE
    assert {item.event.event_id for item in claimed}.issubset({event.id for event in events})
    assert len({item.claim_token for item in claimed}) == BATCH_SIZE

    async with db_session_factory() as session:
        stored_events = list((await session.scalars(select(OutboxEvent).order_by(OutboxEvent.created_at))).all())

    claimed_ids = {item.event.event_id for item in claimed}
    for event in stored_events:
        if event.id in claimed_ids:
            assert event.status is OutboxStatus.PROCESSING
            assert event.claim_token in {item.claim_token for item in claimed}
            assert event.lease_expires_at is not None
            assert event.lease_expires_at > before_claim
            assert event.publish_attempts == 1
        else:
            assert event.status is OutboxStatus.PENDING
            assert event.claim_token is None
            assert event.lease_expires_at is None
            assert event.publish_attempts == 0


@pytest.mark.integration
async def test_claim_skips_an_active_lease_and_reclaims_an_expired_one_with_a_new_token(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    event = (await create_outbox_events(db_session_factory, count=1))[0]

    async with db_session_factory() as session:
        first_claim = (await SqlAlchemyOutboxRepository(session).claim_pending_events(limit=1))[0]

    async with db_session_factory() as session:
        assert await SqlAlchemyOutboxRepository(session).claim_pending_events(limit=1) == []

    async with db_session_factory() as session:
        stored_event = await session.get(OutboxEvent, event.id)
        assert stored_event is not None
        stored_event.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
        await session.commit()

    async with db_session_factory() as session:
        second_claim = (await SqlAlchemyOutboxRepository(session).claim_pending_events(limit=1))[0]

    assert second_claim.event.event_id == first_claim.event.event_id
    assert second_claim.claim_token != first_claim.claim_token

    async with db_session_factory() as session:
        stored_event = await session.get(OutboxEvent, event.id)

    assert stored_event is not None
    assert stored_event.status is OutboxStatus.PROCESSING
    assert stored_event.claim_token == second_claim.claim_token
    assert stored_event.publish_attempts == BATCH_SIZE


@pytest.mark.integration
async def test_only_the_current_claim_can_mark_an_event_as_published(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    event = (await create_outbox_events(db_session_factory, count=1))[0]

    async with db_session_factory() as session:
        first_claim = (await SqlAlchemyOutboxRepository(session).claim_pending_events(limit=1))[0]

    async with db_session_factory() as session:
        stored_event = await session.get(OutboxEvent, event.id)
        assert stored_event is not None
        stored_event.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
        await session.commit()

    async with db_session_factory() as session:
        second_claim = (await SqlAlchemyOutboxRepository(session).claim_pending_events(limit=1))[0]

    published_at = datetime.now(UTC)
    async with db_session_factory() as session:
        await SqlAlchemyOutboxRepository(session).mark_as_published(
            event.id,
            first_claim.claim_token,
            published_at,
        )

    async with db_session_factory() as session:
        stored_event = await session.get(OutboxEvent, event.id)

    assert stored_event is not None
    assert stored_event.status is OutboxStatus.PROCESSING
    assert stored_event.claim_token == second_claim.claim_token
    assert stored_event.published_at is None

    async with db_session_factory() as session:
        await SqlAlchemyOutboxRepository(session).mark_as_published(
            event.id,
            second_claim.claim_token,
            published_at,
        )

    async with db_session_factory() as session:
        stored_event = await session.get(OutboxEvent, event.id)

    assert stored_event is not None
    assert stored_event.status is OutboxStatus.PUBLISHED
    assert stored_event.published_at == published_at
    assert stored_event.claim_token is None
    assert stored_event.lease_expires_at is None


@pytest.mark.integration
async def test_current_claim_records_a_failure_and_releases_the_event_for_retry(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    event = (await create_outbox_events(db_session_factory, count=1))[0]

    async with db_session_factory() as session:
        claimed = (await SqlAlchemyOutboxRepository(session).claim_pending_events(limit=1))[0]

    async with db_session_factory() as session:
        await SqlAlchemyOutboxRepository(session).record_publish_failure(
            event.id,
            claimed.claim_token,
            "RabbitMQ is unavailable",
        )

    async with db_session_factory() as session:
        stored_event = await session.get(OutboxEvent, event.id)

    assert stored_event is not None
    assert stored_event.status is OutboxStatus.PENDING
    assert stored_event.claim_token is None
    assert stored_event.lease_expires_at is None
    assert stored_event.last_error == "RabbitMQ is unavailable"
    assert stored_event.publish_attempts == 1
