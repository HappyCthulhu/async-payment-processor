from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from datetime import datetime
    from uuid import UUID

    from payment_processor.application.commands import CreatePaymentCommand
    from payment_processor.application.events import PaymentCreatedEvent
    from payment_processor.application.results import ClaimedOutboxEvent, PaymentDetails, PaymentResult


class PaymentRepository(Protocol):
    async def create_or_get_with_outbox(self, command: CreatePaymentCommand) -> PaymentResult: ...

    async def get_by_id(self, payment_id: UUID) -> PaymentDetails | None: ...


class OutboxRepository(Protocol):
    async def claim_pending_events(self, limit: int) -> list[ClaimedOutboxEvent]: ...

    async def mark_as_published(self, event_id: UUID, claim_token: UUID, published_at: datetime) -> None: ...

    async def record_publish_failure(self, event_id: UUID, claim_token: UUID, error: str) -> None: ...


class EventPublisher(Protocol):
    async def publish(self, event: PaymentCreatedEvent) -> None: ...
