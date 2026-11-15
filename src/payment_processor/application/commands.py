from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping
    from decimal import Decimal

    from payment_processor.application.payment_types import Currency


@dataclass(slots=True, frozen=True)
class CreatePaymentCommand:
    amount: Decimal
    currency: Currency
    description: str
    metadata: Mapping[str, object]
    webhook_url: str
    idempotency_key: str
