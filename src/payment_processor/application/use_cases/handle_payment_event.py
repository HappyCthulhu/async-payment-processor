from datetime import UTC, datetime
from typing import TYPE_CHECKING, override

from payment_processor.application.errors import GatewayTemporaryError, WebhookDeliveryError
from payment_processor.application.events import WebhookNotification
from payment_processor.application.gateway import GatewayOutcome
from payment_processor.application.ports import PaymentEventHandler, WebhookClient

if TYPE_CHECKING:
    from payment_processor.application.events import PaymentCreatedEvent
    from payment_processor.application.payment_types import FinalPaymentStatus
    from payment_processor.application.ports import PaymentGateway, PaymentRepository

MAX_WEBHOOK_DELIVERY_ATTEMPTS = 3


class HandlePaymentEventUseCase(PaymentEventHandler):
    def __init__(self, repository: PaymentRepository, gateway: PaymentGateway, webhook_client: WebhookClient) -> None:
        self._payment_repo = repository
        self._gateway_repo = gateway
        self._webhook_client = webhook_client

    @override
    async def execute(self, event: PaymentCreatedEvent, attempt: int) -> None:
        state = await self._payment_repo.get_handling_state(event.payment_id)
        if state is None or state.webhook_delivered_at is not None:
            return

        match state.status:
            case "pending":
                outcome = await self._gateway_repo.process(payment_id=event.payment_id)
                match outcome:
                    case GatewayOutcome.SUCCEEDED:
                        status: FinalPaymentStatus = "succeeded"
                    case GatewayOutcome.FAILED:
                        status = "failed"
                    case GatewayOutcome.TEMPORARY_ERROR:
                        raise GatewayTemporaryError

                processed_at = datetime.now(tz=UTC)
                was_recorded = await self._payment_repo.record_gateway_outcome(
                    event.payment_id,
                    status,
                    processed_at,
                )
                if not was_recorded:
                    return
            case "succeeded" | "failed":
                status = state.status
                processed_at = state.processed_at
                if processed_at is None:
                    return

        notification = WebhookNotification(
            amount=event.payment.amount,
            currency=event.payment.currency,
            event_id=event.event_id,
            payment_id=event.payment_id,
            processed_at=processed_at,
            metadata=event.payment.metadata,
            status=status,
        )

        try:
            await self._webhook_client.deliver(str(event.payment.webhook_url), notification)
        except WebhookDeliveryError as error:
            exhausted_at = datetime.now(tz=UTC) if attempt == MAX_WEBHOOK_DELIVERY_ATTEMPTS else None
            await self._payment_repo.record_webhook_failure(
                payment_id=event.payment_id,
                attempt=attempt,
                error=str(error),
                exhausted_at=exhausted_at,
            )
            raise

        await self._payment_repo.record_webhook_delivery(
            payment_id=event.payment_id,
            attempt=attempt,
            delivered_at=datetime.now(tz=UTC),
        )
