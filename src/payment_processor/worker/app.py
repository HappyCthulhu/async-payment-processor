from typing import TYPE_CHECKING

from faststream import AckPolicy, FastStream
from faststream.rabbit import Channel, RabbitMessage

from payment_processor.application.events import PaymentCreatedEvent  # noqa: TC001
from payment_processor.application.use_cases.handle_payment_event import HandlePaymentEventUseCase
from payment_processor.infrastructure.db.repositories import SqlAlchemyPaymentRepository
from payment_processor.infrastructure.db.session import session_factory
from payment_processor.infrastructure.integrations.gateway_emulator import GatewayEmulator
from payment_processor.infrastructure.messaging.broker import broker
from payment_processor.infrastructure.messaging.publisher import RabbitRetryPublisher
from payment_processor.infrastructure.messaging.topology import PAYMENTS_EXCHANGE, PAYMENTS_NEW_QUEUE, declare_topology
from payment_processor.worker.consumer import PaymentEventConsumer

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


async def startup() -> None:
    await declare_topology(broker)


def build_payment_event_consumer(session: AsyncSession) -> PaymentEventConsumer:
    retry_publisher = RabbitRetryPublisher(broker=broker)
    gateway = GatewayEmulator()
    repository = SqlAlchemyPaymentRepository(session)
    use_case = HandlePaymentEventUseCase(gateway=gateway, repository=repository)

    return PaymentEventConsumer(retry_publisher=retry_publisher, use_case=use_case)


@broker.subscriber(
    queue=PAYMENTS_NEW_QUEUE,
    exchange=PAYMENTS_EXCHANGE,
    ack_policy=AckPolicy.MANUAL,
    channel=Channel(
        prefetch_count=1,
    ),
)
async def consume_payment_created_event(event: PaymentCreatedEvent, message: RabbitMessage) -> None:
    async with session_factory() as session:
        consumer = build_payment_event_consumer(session=session)
        await consumer.consume(event=event, message=message)


app = FastStream(broker, after_startup=(startup,))
