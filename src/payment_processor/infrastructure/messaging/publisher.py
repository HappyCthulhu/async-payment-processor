from typing import TYPE_CHECKING

from aio_pika import AMQPException

from payment_processor.application.errors import EventPublishError
from payment_processor.infrastructure.messaging.topology import PAYMENT_EXCHANGE, PAYMENT_NEW_QUEUE

if TYPE_CHECKING:
    from faststream.rabbit import RabbitBroker

    from payment_processor.application.events import PaymentCreatedEvent


class RabbitEventPublisher:
    def __init__(self, broker: RabbitBroker) -> None:
        self._broker = broker

    async def publish(self, event: PaymentCreatedEvent) -> None:
        try:
            await self._broker.publish(event, queue=PAYMENT_NEW_QUEUE, exchange=PAYMENT_EXCHANGE, persist=True)
        except (AMQPException, TimeoutError) as error:
            raise EventPublishError(error) from error
