from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from datetime import datetime
    from decimal import Decimal
    from uuid import UUID


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
