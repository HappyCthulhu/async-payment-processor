from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, override
from uuid import UUID, uuid4

from sqlalchemy import and_, or_, select, update

from payment_processor.application.errors import IdempotencyConflictError
from payment_processor.application.events import PaymentCreatedEvent
from payment_processor.application.ports import OutboxRepository, PaymentRepository
from payment_processor.application.results import ClaimedOutboxEvent, PaymentDetails, PaymentResult
from payment_processor.infrastructure.db.models import (
    OutboxEvent,
    OutboxEventType,
    OutboxStatus,
    Payment,
    PaymentCurrency,
    PaymentStatus,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from payment_processor.application.commands import CreatePaymentCommand
    from payment_processor.application.payment_types import FinalPaymentStatus

CLAIM_LEASE_DURATION = timedelta(seconds=30)


class SqlAlchemyPaymentRepository(PaymentRepository):
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    @override
    async def create_or_get_with_outbox(self, command: CreatePaymentCommand) -> PaymentResult:

        statement = select(Payment).filter_by(idempotency_key=command.idempotency_key)

        async with self._session.begin():
            scalars = await self._session.scalars(statement)
            payment_obj = scalars.one_or_none()

            metadata = dict(command.metadata)

            if payment_obj is None:
                payment_obj = Payment(
                    idempotency_key=command.idempotency_key,
                    amount=command.amount,
                    webhook_url=command.webhook_url,
                    currency=PaymentCurrency(command.currency),
                    description=command.description,
                    payment_metadata=metadata,
                )
                self._session.add(payment_obj)
                await self._session.flush()

                event_id = uuid4()
                outbox_event = OutboxEvent(
                    id=event_id,
                    payment_id=payment_obj.id,
                    event_type=OutboxEventType.CREATED,
                    payload={
                        "event_id": str(event_id),
                        "event_version": 1,
                        "event_type": OutboxEventType.CREATED.value,
                        "payment_id": str(payment_obj.id),
                        "created_at": payment_obj.created_at.isoformat(),
                        "payment": {
                            "amount": str(payment_obj.amount),
                            "currency": payment_obj.currency.value,
                            "description": payment_obj.description,
                            "metadata": command.metadata,
                            "webhook_url": payment_obj.webhook_url,
                        },
                    },
                )
                self._session.add(outbox_event)
            else:
                for pare in (
                    (payment_obj.amount, command.amount),
                    (payment_obj.webhook_url, command.webhook_url),
                    (payment_obj.currency.value, command.currency),
                    (payment_obj.description, command.description),
                    (payment_obj.payment_metadata, metadata),
                ):
                    if pare[0] != pare[1]:
                        raise IdempotencyConflictError(
                            f"Idempotency conflict: value {pare[0]} is not equal to {pare[1]}",
                        )

        return PaymentResult(
            payment_id=payment_obj.id,
            status=payment_obj.status.value,
            created_at=payment_obj.created_at,
        )

    @override
    async def get_by_id(self, payment_id: UUID) -> PaymentDetails | None:
        async with self._session.begin():
            statement = select(Payment).filter_by(id=payment_id)
            payments = await self._session.scalars(statement)
            result = payments.one_or_none()
            if result is not None:
                return PaymentDetails(
                    payment_id=result.id,
                    amount=result.amount,
                    currency=result.currency.value,
                    description=result.description,
                    metadata=result.payment_metadata,
                    webhook_url=result.webhook_url,
                    status=result.status.value,
                    created_at=result.created_at,
                    processed_at=result.processed_at,
                )
            return result

    @override
    async def record_gateway_outcome(
        self,
        payment_id: UUID,
        status: FinalPaymentStatus,
        processed_at: datetime,
    ) -> bool:
        async with self._session.begin():
            statement = (
                update(Payment)
                .where(
                    Payment.id == payment_id,
                    Payment.status == PaymentStatus.PENDING,
                )
                .values(status=PaymentStatus(status), processed_at=processed_at)
                .returning(Payment.id)
            )

            result = await self._session.execute(statement)
            if len(result.scalars().all()) == 0:
                return False

        return True


class SqlAlchemyOutboxRepository(OutboxRepository):
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    @override
    async def claim_pending_events(self, limit: int) -> list[ClaimedOutboxEvent]:

        claimed_events: list[ClaimedOutboxEvent] = []
        async with self._session.begin():
            now = datetime.now(tz=UTC)
            statement = (
                select(OutboxEvent)
                .where(
                    or_(
                        OutboxEvent.status == OutboxStatus.PENDING,
                        and_(
                            OutboxEvent.status == OutboxStatus.PROCESSING,
                            OutboxEvent.lease_expires_at <= now,
                        ),
                    )
                )
                .order_by(OutboxEvent.created_at, OutboxEvent.id)
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
            records = (await self._session.scalars(statement)).all()
            for record in records:
                claim_token = uuid4()
                record.claim_token = claim_token
                record.status = OutboxStatus.PROCESSING
                record.lease_expires_at = now + CLAIM_LEASE_DURATION
                record.publish_attempts += 1

                claimed_event = ClaimedOutboxEvent(
                    claim_token=claim_token, event=PaymentCreatedEvent.model_validate(record.payload)
                )
                claimed_events.append(claimed_event)

        return claimed_events

    @override
    async def mark_as_published(self, event_id: UUID, claim_token: UUID, published_at: datetime) -> None:
        statement = (
            update(OutboxEvent)
            .where(
                OutboxEvent.id == event_id,
                OutboxEvent.claim_token == claim_token,
                OutboxEvent.status == OutboxStatus.PROCESSING,
            )
            .values(
                status=OutboxStatus.PUBLISHED,
                published_at=published_at,
                claim_token=None,
                lease_expires_at=None,
                last_error=None,
            )
        )

        async with self._session.begin():
            await self._session.execute(statement)

    @override
    async def record_publish_failure(self, event_id: UUID, claim_token: UUID, error: str) -> None:
        statement = (
            update(OutboxEvent)
            .filter_by(
                id=event_id,
                claim_token=claim_token,
                status=OutboxStatus.PROCESSING,
            )
            .values(
                last_error=error,
                claim_token=None,
                lease_expires_at=None,
                status=OutboxStatus.PENDING,
            )
        )
        async with self._session.begin():
            await self._session.execute(statement)
