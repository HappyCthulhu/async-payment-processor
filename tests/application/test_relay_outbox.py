from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import pytest

from payment_processor.application.errors import EventPublishError
from payment_processor.application.events import PaymentCreatedEvent, PaymentSnapshot
from payment_processor.application.results import ClaimedOutboxEvent
from payment_processor.application.use_cases.relay_outbox import RelayOutboxUseCase


class FakeOutboxRepository:
    def __init__(self, events: list[ClaimedOutboxEvent]) -> None:
        self._events = events
        self.claim_limits: list[int] = []
        self.published: list[tuple[UUID, UUID, datetime]] = []
        self.failures: list[tuple[UUID, UUID, str]] = []

    async def claim_pending_events(self, limit: int) -> list[ClaimedOutboxEvent]:
        self.claim_limits.append(limit)
        return self._events[:limit]

    async def mark_as_published(self, event_id: UUID, claim_token: UUID, published_at: datetime) -> None:
        self.published.append((event_id, claim_token, published_at))

    async def record_publish_failure(self, event_id: UUID, claim_token: UUID, error: str) -> None:
        self.failures.append((event_id, claim_token, error))


class RecordingEventPublisher:
    def __init__(self) -> None:
        self.published_events: list[PaymentCreatedEvent] = []

    async def publish(self, event: PaymentCreatedEvent) -> None:
        self.published_events.append(event)


class UnavailableEventPublisher:
    def __init__(self) -> None:
        self.attempted_events: list[PaymentCreatedEvent] = []

    async def publish(self, event: PaymentCreatedEvent) -> None:
        self.attempted_events.append(event)
        message = "RabbitMQ is unavailable"
        raise EventPublishError(message)


class BrokenEventPublisher:
    def __init__(self) -> None:
        self.attempted_events: list[PaymentCreatedEvent] = []

    async def publish(self, event: PaymentCreatedEvent) -> None:
        self.attempted_events.append(event)
        message = "Unexpected programming error"
        raise ValueError(message)


def payment_created_event() -> PaymentCreatedEvent:
    return PaymentCreatedEvent(
        event_id=uuid4(),
        event_type="payment.created",
        event_version=1,
        payment_id=uuid4(),
        created_at=datetime(2026, 11, 2, 12, 0, tzinfo=UTC),
        payment=PaymentSnapshot(
            amount=Decimal("1250.00"),
            currency="RUB",
            description="Payment for order #42",
            metadata={"order_id": "42"},
            webhook_url="http://webhook-sink:8080/callbacks/payments",
        ),
    )


def claimed_outbox_event(event: PaymentCreatedEvent) -> ClaimedOutboxEvent:
    return ClaimedOutboxEvent(event=event, claim_token=uuid4())


async def test_relay_publishes_every_claimed_event_and_marks_it_published() -> None:
    events = [payment_created_event(), payment_created_event(), payment_created_event()]
    claimed_events = [claimed_outbox_event(event) for event in events]
    outbox = FakeOutboxRepository(claimed_events)
    publisher = RecordingEventPublisher()
    relay = RelayOutboxUseCase(outbox, publisher)

    await relay.execute(limit=2)

    assert outbox.claim_limits == [2]
    assert publisher.published_events == events[:2]
    assert [(event_id, token) for event_id, token, _ in outbox.published] == [
        (claimed.event.event_id, claimed.claim_token) for claimed in claimed_events[:2]
    ]
    assert all(published_at.tzinfo is not None for _, _, published_at in outbox.published)
    assert outbox.failures == []


async def test_relay_does_nothing_when_the_outbox_has_no_pending_events() -> None:
    outbox = FakeOutboxRepository([])
    publisher = RecordingEventPublisher()
    relay = RelayOutboxUseCase(outbox, publisher)

    await relay.execute(limit=100)

    assert outbox.claim_limits == [100]
    assert publisher.published_events == []
    assert outbox.published == []
    assert outbox.failures == []


async def test_relay_keeps_an_event_unpublished_when_broker_publish_fails() -> None:
    event = payment_created_event()
    claimed = claimed_outbox_event(event)
    outbox = FakeOutboxRepository([claimed])
    publisher = UnavailableEventPublisher()
    relay = RelayOutboxUseCase(outbox, publisher)

    await relay.execute(limit=1)

    assert publisher.attempted_events == [event]
    assert outbox.published == []
    assert outbox.failures == [(event.event_id, claimed.claim_token, "RabbitMQ is unavailable")]


async def test_relay_does_not_hide_an_unexpected_publisher_error() -> None:
    outbox = FakeOutboxRepository([claimed_outbox_event(payment_created_event())])
    relay = RelayOutboxUseCase(outbox, BrokenEventPublisher())

    with pytest.raises(ValueError, match="Unexpected programming error"):
        await relay.execute(limit=1)

    assert outbox.published == []
    assert outbox.failures == []
