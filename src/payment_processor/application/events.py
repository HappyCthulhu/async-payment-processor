from datetime import datetime  # noqa: TC003
from decimal import Decimal  # noqa: TC003
from typing import Literal
from uuid import UUID  # noqa: TC003

from pydantic import BaseModel, ConfigDict, HttpUrl, JsonValue

from payment_processor.application.payment_types import Currency, FinalPaymentStatus  # noqa: TC001


class PaymentSnapshot(BaseModel):
    amount: Decimal
    currency: Currency
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


class WebhookNotification(BaseModel):
    event_id: UUID
    payment_id: UUID
    status: FinalPaymentStatus
    amount: Decimal
    currency: Currency
    metadata: dict[str, JsonValue]
    processed_at: datetime

    model_config = ConfigDict(frozen=True)
