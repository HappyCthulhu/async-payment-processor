from typing import TYPE_CHECKING, override

from aio_pika import AMQPException

from payment_processor.application.errors import EventPublishError
from payment_processor.application.ports import EventPublisher
from payment_processor.infrastructure.messaging.topology import PAYMENTS_EXCHANGE, PAYMENTS_NEW_QUEUE

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
