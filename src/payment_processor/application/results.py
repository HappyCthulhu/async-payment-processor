from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from datetime import datetime
    from decimal import Decimal
    from uuid import UUID

    from payment_processor.application.events import PaymentCreatedEvent
    from payment_processor.application.payment_types import PaymentStatus


@dataclass(frozen=True, slots=True)
class PaymentResult:
    payment_id: UUID
    status: Literal["pending", "succeeded", "failed"]
    created_at: datetime


@dataclass(frozen=True, slots=True)
class PaymentDetails:
    payment_id: UUID
    amount: Decimal
    currency: Literal["RUB", "USD", "EUR"]
    description: str
    metadata: dict[str, object]
    webhook_url: str
    status: Literal["pending", "succeeded", "failed"]
    created_at: datetime
    processed_at: datetime | None


@dataclass(frozen=True, slots=True)
class ClaimedOutboxEvent:
    event: PaymentCreatedEvent
    claim_token: UUID


@dataclass(frozen=True, slots=True)
class PaymentHandlingState:
    payment_id: UUID
    status: PaymentStatus
    webhook_attempts: int
    webhook_delivered_at: datetime | None
    webhook_delivery_exhausted_at: datetime | None
    processed_at: datetime | None
