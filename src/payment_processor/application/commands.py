from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from decimal import Decimal


@dataclass(slots=True, frozen=True)
class CreatePaymentCommand:
    amount: Decimal
    currency: Literal["RUB", "USD", "EUR"]
    description: str
    metadata: dict[str, object]
    webhook_url: str
    idempotency_key: str
