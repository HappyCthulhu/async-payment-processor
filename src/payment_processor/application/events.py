from datetime import datetime  # noqa: TC003
from decimal import Decimal  # noqa: TC003
from typing import Literal
from uuid import UUID  # noqa: TC003

from pydantic import BaseModel, HttpUrl, JsonValue


class PaymentSnapshot(BaseModel):
    amount: Decimal
    currency: Literal["RUB", "USD", "EUR"]
    description: str
    metadata: dict[str, JsonValue]
    webhook_url: HttpUrl


class PaymentCreatedEvent(BaseModel):
    event_id: UUID
    event_type: Literal["payment.created"]
    event_version: Literal[1]
    payment_id: UUID
    created_at: datetime
    payment: PaymentSnapshot
