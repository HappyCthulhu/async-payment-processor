from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from datetime import datetime
    from uuid import UUID


@dataclass(frozen=True, slots=True)
class PaymentResult:
    payment_id: UUID
    status: Literal["pending", "succeeded", "failed"]
    created_at: datetime
