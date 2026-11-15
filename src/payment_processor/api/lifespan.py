import asyncio
from asyncio import Event, create_task
from contextlib import asynccontextmanager, suppress
from typing import TYPE_CHECKING

from loguru import logger

from payment_processor.core.config import settings
from payment_processor.core.logging import configure_logging
from payment_processor.infrastructure.db.session import session_factory
from payment_processor.infrastructure.messaging.broker import broker
from payment_processor.infrastructure.messaging.publisher import RabbitEventPublisher
from payment_processor.infrastructure.messaging.relay_runner import OutboxRelayRunner
from payment_processor.infrastructure.messaging.topology import declare_topology

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from fastapi import FastAPI


async def run_relay(stop_event: Event) -> None:
    while not stop_event.is_set():
        try:
            async with broker:
                await declare_topology(broker=broker)
                runner = OutboxRelayRunner(
                    poll_interval_seconds=settings.relay_poll_interval_seconds,
                    publisher=RabbitEventPublisher(broker=broker),
                    session_factory=session_factory,
                    retention_days=settings.outbox_retention_days,
                    cleanup_batch_size=settings.outbox_cleanup_batch_size,
                )
                await runner.run_forever(settings.relay_batch_size, stop_event=stop_event)
        except Exception as error:
            logger.warning("event=relay_connection_failed error_type={} action=retry", type(error).__name__)

        if not stop_event.is_set():
            with suppress(TimeoutError):
                await asyncio.wait_for(stop_event.wait(), timeout=settings.relay_poll_interval_seconds)


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncGenerator[None]:
    configure_logging()
    logger.info("event=api_started")
    stop_event = Event()
    relay_task = create_task(run_relay(stop_event))
    try:
        yield
    finally:
        stop_event.set()
        with suppress(TimeoutError):
            await asyncio.wait_for(relay_task, settings.relay_shutdown_timeout_seconds)
        logger.info("event=api_stopped")
