from typing import TYPE_CHECKING, override

from aio_pika import AMQPException

from payment_processor.application.errors import EventPublishError
from payment_processor.application.ports import EventPublisher, RetryPublisher
from payment_processor.infrastructure.messaging.topology import (
    PAYMENTS_EXCHANGE,
    PAYMENTS_NEW_QUEUE,
    PAYMENTS_RETRY_1_QUEUE,
    PAYMENTS_RETRY_2_QUEUE,
)

if TYPE_CHECKING:
    from faststream.rabbit import RabbitBroker

    from payment_processor.application.events import PaymentCreatedEvent


class RabbitEventPublisher(EventPublisher):
    def __init__(self, broker: RabbitBroker) -> None:
        self._broker = broker

    @override
    async def publish(self, event: PaymentCreatedEvent) -> None:
        try:
            await self._broker.publish(event, queue=PAYMENTS_NEW_QUEUE, exchange=PAYMENTS_EXCHANGE, persist=True)
        except (AMQPException, TimeoutError) as error:
            raise EventPublishError(error) from error


class RabbitRetryPublisher(RetryPublisher):
    def __init__(self, broker: RabbitBroker) -> None:
        self._broker = broker

    @override
    async def publish(self, event: PaymentCreatedEvent, retry_attempt: int) -> None:

        try:
            match retry_attempt:
                case 2:
                    await self._broker.publish(
                        message=event,
                        exchange=PAYMENTS_EXCHANGE,
                        queue=PAYMENTS_RETRY_1_QUEUE,
                        persist=True,
                        headers={"x-payment-attempt": retry_attempt},
                    )
                case 3:
                    await self._broker.publish(
                        message=event,
                        exchange=PAYMENTS_EXCHANGE,
                        queue=PAYMENTS_RETRY_2_QUEUE,
                        persist=True,
                        headers={"x-payment-attempt": retry_attempt},
                    )

                case _:
                    raise ValueError(f"Unsupported retry attempt: {retry_attempt}")

        except (AMQPException, TimeoutError) as error:
            raise EventPublishError(error) from error
