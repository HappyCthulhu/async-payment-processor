import asyncio
import logging
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from payment_processor.application.use_cases.relay_outbox import RelayOutboxUseCase
from payment_processor.infrastructure.db.repositories import SqlAlchemyOutboxRepository

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from payment_processor.application.ports import EventPublisher

logger = logging.getLogger(__name__)


class OutboxRelayRunner:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        publisher: EventPublisher,
        poll_interval_seconds: float,
        retention_days: int = 30,
        cleanup_batch_size: int = 100,
    ) -> None:
        self._session_factory = session_factory
        self._publisher = publisher
        self._poll_interval_seconds = poll_interval_seconds
        self._retention_days = retention_days
        self._cleanup_batch_size = cleanup_batch_size

    async def run_once(self, limit: int) -> None:
        async with self._session_factory() as session:
            repository = SqlAlchemyOutboxRepository(session=session)
            relay_use_case = RelayOutboxUseCase(repository=repository, publisher=self._publisher)
            await relay_use_case.execute(limit)
            await repository.delete_old_published(
                before=datetime.now(UTC) - timedelta(days=self._retention_days),
                limit=self._cleanup_batch_size,
            )

    async def run_forever(self, limit: int, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                await self.run_once(limit)
            except Exception as error:
                logger.warning("Outbox relay pass failed (%s); retrying", type(error).__name__)

            if stop_event.is_set():
                return

            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self._poll_interval_seconds)
            except TimeoutError:
                continue
