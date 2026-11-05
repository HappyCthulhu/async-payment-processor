from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from payment_processor.application.commands import CreatePaymentCommand
    from payment_processor.application.ports import PaymentRepository
    from payment_processor.application.results import PaymentResult


class CreatePaymentUseCase:
    def __init__(self, repository: PaymentRepository) -> None:
        self._repository = repository

    async def execute(self, command: CreatePaymentCommand) -> PaymentResult:
        return await self._repository.create_or_get_with_outbox(command)
