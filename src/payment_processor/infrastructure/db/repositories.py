from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy import select

from payment_processor.application.errors import IdempotencyConflictError
from payment_processor.application.ports import PaymentRepository
from payment_processor.application.results import PaymentResult
from payment_processor.infrastructure.db.models import OutboxEvent, OutboxEventType, Payment, PaymentCurrency

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from payment_processor.application.commands import CreatePaymentCommand


class SqlAlchemyPaymentRepository(PaymentRepository):
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

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
