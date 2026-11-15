from dataclasses import dataclass
from typing import TYPE_CHECKING

import httpx
from faststream import AckPolicy, FastStream
from faststream.rabbit import Channel, RabbitMessage

from payment_processor.application.events import PaymentCreatedEvent  # noqa: TC001
from payment_processor.application.use_cases.handle_payment_event import HandlePaymentEventUseCase
from payment_processor.core.config import settings
from payment_processor.infrastructure.db.repositories import SqlAlchemyPaymentRepository
from payment_processor.infrastructure.db.session import session_factory
from payment_processor.infrastructure.integrations.gateway_emulator import GatewayEmulator
from payment_processor.infrastructure.integrations.webhook_http import HttpWebhookClient
from payment_processor.infrastructure.messaging.broker import broker
from payment_processor.infrastructure.messaging.publisher import RabbitRetryPublisher
from payment_processor.infrastructure.messaging.topology import PAYMENTS_EXCHANGE, PAYMENTS_NEW_QUEUE, declare_topology
from payment_processor.worker.consumer import PaymentEventConsumer

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


@dataclass
class WorkerResources:
    webhook_http_client: httpx.AsyncClient | None = None


resources = WorkerResources()


def create_webhook_http_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=settings.webhook_timeout_seconds,
        trust_env=False,
    )


async def startup() -> None:
    await declare_topology(broker)
    resources.webhook_http_client = create_webhook_http_client()


async def shutdown() -> None:
    if resources.webhook_http_client is not None:
        await resources.webhook_http_client.aclose()
        resources.webhook_http_client = None


def build_payment_event_consumer(session: AsyncSession) -> PaymentEventConsumer:
    if resources.webhook_http_client is None:
        raise RuntimeError("Webhook HTTP client is not initialized")

    retry_publisher = RabbitRetryPublisher(broker=broker)
    gateway = GatewayEmulator()
    repository = SqlAlchemyPaymentRepository(session)
    webhook_client = HttpWebhookClient(resources.webhook_http_client)
    use_case = HandlePaymentEventUseCase(
        gateway=gateway,
        repository=repository,
        webhook_client=webhook_client,
    )

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


app = FastStream(broker, after_startup=(startup,), after_shutdown=(shutdown,))
