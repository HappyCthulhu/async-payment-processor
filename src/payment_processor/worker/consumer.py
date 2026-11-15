from typing import TYPE_CHECKING

from payment_processor.application.errors import GatewayTemporaryError

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
            await self._use_case.execute(event)
            await message.ack()
        except GatewayTemporaryError:
            header_attempt = message.headers.get("x-payment-attempt")
            attempt = int(header_attempt) if header_attempt is not None else 1

            match attempt:
                case 1:
                    await self._publisher.publish(event, 2)
                    await message.ack()
                case 2:
                    await self._publisher.publish(event, 3)
                    await message.ack()
                case _:
                    await message.reject(requeue=False)
