from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from uuid import UUID

    from payment_processor.application.commands import CreatePaymentCommand
    from payment_processor.application.results import PaymentDetails, PaymentResult


class PaymentRepository(Protocol):
    async def create_or_get_with_outbox(self, command: CreatePaymentCommand) -> PaymentResult: ...

    async def get_by_id(self, payment_id: UUID) -> PaymentDetails | None: ...
