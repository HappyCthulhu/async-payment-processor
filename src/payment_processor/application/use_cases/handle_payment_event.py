from datetime import UTC, datetime
from typing import TYPE_CHECKING, override

from loguru import logger

from payment_processor.application.errors import GatewayTemporaryError, WebhookDeliveryError
from payment_processor.application.events import WebhookNotification
from payment_processor.application.gateway import GatewayOutcome
from payment_processor.application.ports import PaymentEventHandler, WebhookClient

if TYPE_CHECKING:
    from payment_processor.application.events import PaymentCreatedEvent
    from payment_processor.application.payment_types import FinalPaymentStatus
    from payment_processor.application.ports import PaymentGateway, PaymentRepository
    from payment_processor.application.results import PaymentHandlingState

MAX_WEBHOOK_DELIVERY_ATTEMPTS = 3


class HandlePaymentEventUseCase(PaymentEventHandler):
    def __init__(self, repository: PaymentRepository, gateway: PaymentGateway, webhook_client: WebhookClient) -> None:
        self._payment_repo = repository
        self._gateway_repo = gateway
        self._webhook_client = webhook_client

    @override
    async def execute(self, event: PaymentCreatedEvent, attempt: int) -> None:  # noqa: C901
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
                    state = await self._payment_repo.get_handling_state(event.payment_id)
                    if state is None or state.webhook_delivered_at is not None:
                        return
                    status, processed_at = self._final_outcome(state)
                logger.info("event=gateway_outcome payment_id={} status={}", event.payment_id, status)
            case "succeeded" | "failed":
                status, processed_at = self._final_outcome(state)

        # A confirmed retry can coexist with a redelivery of the original message.
        # Republish the next attempt without repeating an already recorded POST.
        if state.webhook_attempts >= attempt:
            logger.info("event=webhook_attempt_already_recorded payment_id={} attempt={}", event.payment_id, attempt)
            raise WebhookDeliveryError("Webhook attempt already recorded")

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
            logger.warning("event=webhook_failed payment_id={} attempt={}", event.payment_id, attempt)
            raise

        await self._payment_repo.record_webhook_delivery(
            payment_id=event.payment_id,
            attempt=attempt,
            delivered_at=datetime.now(tz=UTC),
        )
        logger.info("event=webhook_delivered payment_id={} attempt={}", event.payment_id, attempt)

    @staticmethod
    def _final_outcome(state: PaymentHandlingState) -> tuple[FinalPaymentStatus, datetime]:
        if state.status == "pending" or state.processed_at is None:
            raise GatewayTemporaryError
        return state.status, state.processed_at
