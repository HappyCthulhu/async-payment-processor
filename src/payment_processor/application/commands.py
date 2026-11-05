from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from collections.abc import Mapping
    from decimal import Decimal


@dataclass(slots=True, frozen=True)
class CreatePaymentCommand:
    amount: Decimal
    currency: Literal["RUB", "USD", "EUR"]
    description: str
    metadata: Mapping[str, object]
    webhook_url: str
    idempotency_key: str
