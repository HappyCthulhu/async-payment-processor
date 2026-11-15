from datetime import UTC, datetime
from typing import TYPE_CHECKING, override

from payment_processor.application.errors import GatewayTemporaryError
from payment_processor.application.gateway import GatewayOutcome
from payment_processor.application.ports import PaymentEventHandler

if TYPE_CHECKING:
    from payment_processor.application.events import PaymentCreatedEvent
    from payment_processor.application.ports import PaymentGateway, PaymentRepository


class HandlePaymentEventUseCase(PaymentEventHandler):
    def __init__(self, repository: PaymentRepository, gateway: PaymentGateway) -> None:
        self._payment_repo = repository
        self._gateway_repo = gateway

    @override
    async def execute(self, event: PaymentCreatedEvent) -> None:
        result = await self._payment_repo.get_handling_state(event.payment_id)
        if result is None:
            return

        match result.status:
            case "succeeded", "failed":
                return
            case "pending":
                outcome = await self._gateway_repo.process(payment_id=event.payment_id)

                now = datetime.now(tz=UTC)
                match outcome:
                    case GatewayOutcome.SUCCEEDED:
                        status = "succeeded"
                    case GatewayOutcome.FAILED:
                        status = "failed"
                    case GatewayOutcome.TEMPORARY_ERROR:
                        raise GatewayTemporaryError

                await self._payment_repo.record_gateway_outcome(event.payment_id, status, now)
