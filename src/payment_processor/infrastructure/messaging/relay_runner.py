import asyncio
from typing import TYPE_CHECKING

from payment_processor.application.use_cases.relay_outbox import RelayOutboxUseCase
from payment_processor.infrastructure.db.repositories import SqlAlchemyOutboxRepository

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from payment_processor.application.ports import EventPublisher


class OutboxRelayRunner:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        publisher: EventPublisher,
        poll_interval_seconds: float,
    ) -> None:
        self._session_factory = session_factory
        self._publisher = publisher
        self._poll_interval_seconds = poll_interval_seconds

    async def run_once(self, limit: int) -> None:
        async with self._session_factory() as session:
            repository = SqlAlchemyOutboxRepository(session=session)
            relay_use_case = RelayOutboxUseCase(repository=repository, publisher=self._publisher)
            await relay_use_case.execute(limit)

    async def run_forever(self, limit: int, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            await self.run_once(limit)

            if stop_event.is_set():
                return

            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self._poll_interval_seconds)
            except TimeoutError:
                continue
