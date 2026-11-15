from typing import TYPE_CHECKING

from loguru import logger

from payment_processor.application.errors import GatewayTemporaryError, WebhookDeliveryError

if TYPE_CHECKING:
    from faststream.rabbit import RabbitMessage

    from payment_processor.application.events import PaymentCreatedEvent
    from payment_processor.application.ports import PaymentEventHandler, RetryPublisher


class PaymentEventConsumer:
    def __init__(self, use_case: PaymentEventHandler, retry_publisher: RetryPublisher) -> None:
        self._use_case = use_case
        self._publisher = retry_publisher

    async def consume(self, event: PaymentCreatedEvent, message: RabbitMessage) -> None:
        try:
            header_attempt = message.headers.get("x-payment-attempt")
            attempt = int(header_attempt) if header_attempt is not None else 1

            try:
                await self._use_case.execute(event, attempt)
                await message.ack()
            except (GatewayTemporaryError, WebhookDeliveryError) as error:
                logger.warning(
                    "event=payment_attempt_failed payment_id={} attempt={} error_type={}",
                    event.payment_id,
                    attempt,
                    type(error).__name__,
                )
                match attempt:
                    case 1:
                        await self._publisher.publish(event, 2)
                        logger.info("event=payment_retry_published payment_id={} attempt=2", event.payment_id)
                        await message.ack()
                    case 2:
                        await self._publisher.publish(event, 3)
                        logger.info("event=payment_retry_published payment_id={} attempt=3", event.payment_id)
                        await message.ack()
                    case _:
                        await message.reject(requeue=False)
                        logger.warning(
                            "event=payment_rejected_to_dlq payment_id={} attempt={}", event.payment_id, attempt
                        )
        except Exception as error:
            logger.warning(
                "event=payment_processing_error payment_id={} error_type={} action=requeue",
                event.payment_id,
                type(error).__name__,
            )
            await message.reject(requeue=True)
