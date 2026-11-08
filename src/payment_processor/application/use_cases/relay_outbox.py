from datetime import UTC, datetime
from typing import TYPE_CHECKING

from payment_processor.application.errors import EventPublishError

if TYPE_CHECKING:
    from payment_processor.application.ports import EventPublisher, OutboxRepository


class RelayOutboxUseCase:
    def __init__(self, repository: OutboxRepository, publisher: EventPublisher) -> None:
        self._repository = repository
        self._publisher = publisher

    async def execute(self, limit: int) -> None:
        claimed_events = await self._repository.claim_pending_events(limit)
        for claimed in claimed_events:
            try:
                await self._publisher.publish(claimed.event)
            except EventPublishError as error:
                await self._repository.record_publish_failure(claimed.event.event_id, claimed.claim_token, str(error))
            else:
                await self._repository.mark_as_published(
                    claimed.event.event_id, claimed.claim_token, datetime.now(tz=UTC)
                )
