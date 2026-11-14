import asyncio
from asyncio import Event, create_task
from contextlib import asynccontextmanager, suppress
from typing import TYPE_CHECKING

from payment_processor.core.config import settings
from payment_processor.infrastructure.db.session import session_factory
from payment_processor.infrastructure.messaging.broker import broker
from payment_processor.infrastructure.messaging.publisher import RabbitEventPublisher
from payment_processor.infrastructure.messaging.relay_runner import OutboxRelayRunner
from payment_processor.infrastructure.messaging.topology import declare_topology

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from fastapi import FastAPI


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncGenerator[None]:
    async with broker:
        await declare_topology(broker=broker)
        stop_event = Event()
        publisher = RabbitEventPublisher(broker=broker)
        runner = OutboxRelayRunner(
            poll_interval_seconds=settings.relay_poll_interval_seconds,
            publisher=publisher,
            session_factory=session_factory,
        )

        relay_task = create_task(runner.run_forever(settings.relay_batch_size, stop_event=stop_event))
        yield

        stop_event.set()

        with suppress(TimeoutError):
            await asyncio.wait_for(relay_task, settings.relay_shutdown_timeout_seconds)
