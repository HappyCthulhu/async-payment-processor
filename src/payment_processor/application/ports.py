from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from payment_processor.application.commands import CreatePaymentCommand
    from payment_processor.application.results import PaymentResult


class PaymentRepository(Protocol):
    async def create_or_get_with_outbox(self, command: CreatePaymentCommand) -> PaymentResult: ...
