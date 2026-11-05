from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from uuid import UUID

    from payment_processor.application.ports import PaymentRepository
    from payment_processor.application.results import PaymentDetails


class GetPaymentUseCase:
    def __init__(self, repository: PaymentRepository) -> None:
        self._repository = repository

    async def get_by_id(self, payment_id: UUID) -> PaymentDetails | None:
        return await self._repository.get_by_id(payment_id)
