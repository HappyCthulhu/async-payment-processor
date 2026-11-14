import asyncio
from typing import TYPE_CHECKING, cast

from payment_processor.infrastructure.messaging.relay_runner import OutboxRelayRunner

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from payment_processor.application.ports import EventPublisher


STOP_AFTER_RUNS = 2


class RecordingOutboxRelayRunner(OutboxRelayRunner):
    def __init__(self, stop_event: asyncio.Event) -> None:
        super().__init__(
            session_factory=cast("async_sessionmaker[AsyncSession]", object()),
            publisher=cast("EventPublisher", object()),
            poll_interval_seconds=0,
        )
        self._stop_event = stop_event
        self.run_limits: list[int] = []

    async def run_once(self, limit: int) -> None:
        self.run_limits.append(limit)
        if len(self.run_limits) == STOP_AFTER_RUNS:
            self._stop_event.set()


async def test_runner_repeats_relay_passes_until_stop_is_requested() -> None:
    stop_event = asyncio.Event()
    runner = RecordingOutboxRelayRunner(stop_event)

    await runner.run_forever(limit=5, stop_event=stop_event)

    assert runner.run_limits == [5] * STOP_AFTER_RUNS
