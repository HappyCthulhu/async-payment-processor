from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, override
from uuid import UUID, uuid4

import pytest

from payment_processor.application.errors import GatewayTemporaryError, WebhookDeliveryError
from payment_processor.application.events import PaymentCreatedEvent, PaymentSnapshot, WebhookNotification
from payment_processor.application.gateway import GatewayOutcome
from payment_processor.application.ports import PaymentGateway, PaymentRepository, WebhookClient
from payment_processor.application.results import PaymentHandlingState
from payment_processor.application.use_cases.handle_payment_event import HandlePaymentEventUseCase

if TYPE_CHECKING:
    from payment_processor.application.commands import CreatePaymentCommand
    from payment_processor.application.payment_types import FinalPaymentStatus, PaymentStatus
    from payment_processor.application.results import PaymentDetails, PaymentResult


class StubPaymentRepository(PaymentRepository):
    def __init__(self, state: PaymentHandlingState | None) -> None:
        self._state = state
        self.recorded_outcomes: list[tuple[UUID, FinalPaymentStatus, datetime]] = []
        self.recorded_deliveries: list[tuple[UUID, int, datetime]] = []
        self.recorded_failures: list[tuple[UUID, int, str, datetime | None]] = []

    @override
    async def create_or_get_with_outbox(self, command: CreatePaymentCommand) -> PaymentResult:
        raise AssertionError(f"Unexpected call with command {command}")

    @override
    async def get_by_id(self, payment_id: UUID) -> PaymentDetails | None:
        raise AssertionError(f"Unexpected call for payment {payment_id}")

    @override
    async def record_gateway_outcome(
        self,
        payment_id: UUID,
        status: FinalPaymentStatus,
        processed_at: datetime,
    ) -> bool:
        self.recorded_outcomes.append((payment_id, status, processed_at))
        return True

    @override
    async def get_handling_state(self, payment_id: UUID) -> PaymentHandlingState | None:
        return self._state

    @override
    async def record_webhook_delivery(self, payment_id: UUID, attempt: int, delivered_at: datetime) -> None:
        self.recorded_deliveries.append((payment_id, attempt, delivered_at))

    @override
    async def record_webhook_failure(
        self,
        payment_id: UUID,
        attempt: int,
        error: str,
        exhausted_at: datetime | None,
    ) -> None:
        self.recorded_failures.append((payment_id, attempt, error, exhausted_at))


class RecordingGateway(PaymentGateway):
    def __init__(self, outcome: GatewayOutcome) -> None:
        self._outcome = outcome
        self.processed_payment_ids: list[UUID] = []

    @override
    async def process(self, payment_id: UUID) -> GatewayOutcome:
        self.processed_payment_ids.append(payment_id)
        return self._outcome


class RecordingWebhookClient(WebhookClient):
    def __init__(self, error: WebhookDeliveryError | None = None) -> None:
        self._error = error
        self.deliveries: list[tuple[str, WebhookNotification]] = []

    @override
    async def deliver(self, webhook_url: str, notification: WebhookNotification) -> None:
        self.deliveries.append((webhook_url, notification))
        if self._error is not None:
            raise self._error


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


def handling_state(
    payment_id: UUID,
    status: PaymentStatus,
    *,
    processed_at: datetime | None = None,
    webhook_delivered_at: datetime | None = None,
) -> PaymentHandlingState:
    return PaymentHandlingState(
        payment_id=payment_id,
        status=status,
        processed_at=processed_at,
        webhook_attempts=0,
        last_recorded_webhook_attempt=0,
        webhook_delivered_at=webhook_delivered_at,
        webhook_delivery_exhausted_at=None,
    )


async def test_handle_event_finalizes_pending_payment_and_delivers_webhook() -> None:
    event = payment_created_event()
    repository = StubPaymentRepository(handling_state(event.payment_id, status="pending"))
    gateway = RecordingGateway(GatewayOutcome.SUCCEEDED)
    webhook_client = RecordingWebhookClient()
    use_case = HandlePaymentEventUseCase(
        repository=repository,
        gateway=gateway,
        webhook_client=webhook_client,
    )

    await use_case.execute(event, attempt=1)

    assert gateway.processed_payment_ids == [event.payment_id]
    assert len(repository.recorded_outcomes) == 1
    payment_id, status, processed_at = repository.recorded_outcomes[0]
    assert payment_id == event.payment_id
    assert status == "succeeded"
    assert processed_at.tzinfo is not None
    assert webhook_client.deliveries == [
        (
            "http://webhook-sink:8080/callbacks/payments",
            WebhookNotification(
                event_id=event.event_id,
                payment_id=event.payment_id,
                status="succeeded",
                amount=Decimal("1250.00"),
                currency="RUB",
                metadata={"order_id": "42"},
                processed_at=processed_at,
            ),
        ),
    ]
    assert len(repository.recorded_deliveries) == 1
    delivered_payment_id, attempt, delivered_at = repository.recorded_deliveries[0]
    assert delivered_payment_id == event.payment_id
    assert attempt == 1
    assert delivered_at.tzinfo is not None


async def test_handle_event_delivers_webhook_for_final_payment_without_calling_gateway() -> None:
    event = payment_created_event()
    processed_at = datetime(2026, 11, 15, 12, 0, tzinfo=UTC)
    attempt = 2
    repository = StubPaymentRepository(
        handling_state(event.payment_id, status="failed", processed_at=processed_at),
    )
    gateway = RecordingGateway(GatewayOutcome.SUCCEEDED)
    webhook_client = RecordingWebhookClient()
    use_case = HandlePaymentEventUseCase(
        repository=repository,
        gateway=gateway,
        webhook_client=webhook_client,
    )

    await use_case.execute(event, attempt=attempt)

    assert gateway.processed_payment_ids == []
    assert repository.recorded_outcomes == []
    assert webhook_client.deliveries[0][1].status == "failed"
    assert webhook_client.deliveries[0][1].processed_at == processed_at
    assert repository.recorded_deliveries[0][1] == attempt


async def test_handle_event_skips_an_already_delivered_webhook() -> None:
    event = payment_created_event()
    processed_at = datetime(2026, 11, 15, 12, 0, tzinfo=UTC)
    repository = StubPaymentRepository(
        handling_state(
            event.payment_id,
            status="succeeded",
            processed_at=processed_at,
            webhook_delivered_at=processed_at,
        ),
    )
    gateway = RecordingGateway(GatewayOutcome.SUCCEEDED)
    webhook_client = RecordingWebhookClient()
    use_case = HandlePaymentEventUseCase(
        repository=repository,
        gateway=gateway,
        webhook_client=webhook_client,
    )

    await use_case.execute(event, attempt=1)

    assert gateway.processed_payment_ids == []
    assert webhook_client.deliveries == []
    assert repository.recorded_outcomes == []
    assert repository.recorded_deliveries == []


async def test_handle_event_records_webhook_failure_and_reraises_error() -> None:
    event = payment_created_event()
    processed_at = datetime(2026, 11, 15, 12, 0, tzinfo=UTC)
    final_attempt = 3
    repository = StubPaymentRepository(
        handling_state(event.payment_id, status="succeeded", processed_at=processed_at),
    )
    gateway = RecordingGateway(GatewayOutcome.SUCCEEDED)
    webhook_client = RecordingWebhookClient(WebhookDeliveryError("Webhook request failed"))
    use_case = HandlePaymentEventUseCase(
        repository=repository,
        gateway=gateway,
        webhook_client=webhook_client,
    )

    with pytest.raises(WebhookDeliveryError, match="Webhook request failed"):
        await use_case.execute(event, attempt=final_attempt)

    assert gateway.processed_payment_ids == []
    assert repository.recorded_deliveries == []
    assert len(repository.recorded_failures) == 1
    payment_id, attempt, error, exhausted_at = repository.recorded_failures[0]
    assert payment_id == event.payment_id
    assert attempt == final_attempt
    assert error == "Webhook request failed"
    assert exhausted_at is not None


async def test_handle_event_does_not_persist_a_temporary_gateway_error() -> None:
    event = payment_created_event()
    repository = StubPaymentRepository(handling_state(event.payment_id, status="pending"))
    gateway = RecordingGateway(GatewayOutcome.TEMPORARY_ERROR)
    webhook_client = RecordingWebhookClient()
    use_case = HandlePaymentEventUseCase(
        repository=repository,
        gateway=gateway,
        webhook_client=webhook_client,
    )

    with pytest.raises(GatewayTemporaryError):
        await use_case.execute(event, attempt=1)

    assert gateway.processed_payment_ids == [event.payment_id]
    assert repository.recorded_outcomes == []
    assert webhook_client.deliveries == []


async def test_lost_gateway_update_uses_the_persisted_outcome() -> None:
    event = payment_created_event()
    processed_at = datetime(2026, 11, 15, 12, 0, tzinfo=UTC)

    class ConcurrentFinalizationRepository(StubPaymentRepository):
        @override
        async def record_gateway_outcome(
            self, payment_id: UUID, status: FinalPaymentStatus, processed_at: datetime
        ) -> bool:
            self._state = handling_state(payment_id, status="failed", processed_at=stored_at)
            return False

    stored_at = processed_at
    repository = ConcurrentFinalizationRepository(handling_state(event.payment_id, status="pending"))
    webhook = RecordingWebhookClient()
    await HandlePaymentEventUseCase(repository, RecordingGateway(GatewayOutcome.SUCCEEDED), webhook).execute(event, 1)

    assert webhook.deliveries[0][1].status == "failed"
    assert webhook.deliveries[0][1].processed_at == stored_at
    assert len(repository.recorded_deliveries) == 1


async def test_duplicate_failed_attempt_does_not_repeat_webhook() -> None:
    event = payment_created_event()
    state = handling_state(event.payment_id, status="succeeded", processed_at=datetime.now(tz=UTC))
    state = replace(state, webhook_attempts=1, last_recorded_webhook_attempt=1)
    repository = StubPaymentRepository(state)
    webhook = RecordingWebhookClient()

    use_case = HandlePaymentEventUseCase(repository, RecordingGateway(GatewayOutcome.SUCCEEDED), webhook)
    with pytest.raises(WebhookDeliveryError):
        await use_case.execute(event, 1)

    assert webhook.deliveries == []
