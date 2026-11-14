from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, override
from uuid import UUID, uuid4

import pytest

from payment_processor.application.errors import GatewayTemporaryError
from payment_processor.application.events import PaymentCreatedEvent, PaymentSnapshot
from payment_processor.application.gateway import GatewayOutcome
from payment_processor.application.ports import PaymentGateway, PaymentRepository
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


class RecordingGateway(PaymentGateway):
    def __init__(self, outcome: GatewayOutcome) -> None:
        self._outcome = outcome
        self.processed_payment_ids: list[UUID] = []

    @override
    async def process(self, payment_id: UUID) -> GatewayOutcome:
        self.processed_payment_ids.append(payment_id)
        return self._outcome


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


def handling_state(payment_id: UUID, status: PaymentStatus) -> PaymentHandlingState:
    return PaymentHandlingState(
        payment_id=payment_id,
        status=status,
        processed_at=None,
        webhook_attempts=0,
        webhook_delivered_at=None,
        webhook_delivery_exhausted_at=None,
    )


async def test_handle_event_records_a_successful_gateway_outcome_for_pending_payment() -> None:
    event = payment_created_event()
    repository = StubPaymentRepository(handling_state(event.payment_id, status="pending"))
    gateway = RecordingGateway(GatewayOutcome.SUCCEEDED)
    use_case = HandlePaymentEventUseCase(repository=repository, gateway=gateway)

    await use_case.execute(event)

    assert gateway.processed_payment_ids == [event.payment_id]
    assert len(repository.recorded_outcomes) == 1
    payment_id, status, processed_at = repository.recorded_outcomes[0]
    assert payment_id == event.payment_id
    assert status == "succeeded"
    assert processed_at.tzinfo is not None


async def test_handle_event_does_not_call_gateway_for_a_final_payment() -> None:
    event = payment_created_event()
    repository = StubPaymentRepository(handling_state(event.payment_id, status="failed"))
    gateway = RecordingGateway(GatewayOutcome.SUCCEEDED)
    use_case = HandlePaymentEventUseCase(repository=repository, gateway=gateway)

    await use_case.execute(event)

    assert gateway.processed_payment_ids == []
    assert repository.recorded_outcomes == []


async def test_handle_event_does_not_persist_a_temporary_gateway_error() -> None:
    event = payment_created_event()
    repository = StubPaymentRepository(handling_state(event.payment_id, status="pending"))
    gateway = RecordingGateway(GatewayOutcome.TEMPORARY_ERROR)
    use_case = HandlePaymentEventUseCase(repository=repository, gateway=gateway)

    with pytest.raises(GatewayTemporaryError):
        await use_case.execute(event)

    assert gateway.processed_payment_ids == [event.payment_id]
    assert repository.recorded_outcomes == []
