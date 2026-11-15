from datetime import datetime
from decimal import Decimal
from typing import Annotated
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, JsonValue, field_validator

from payment_processor.application.payment_types import Currency, PaymentStatus  # noqa: TC001


class CreatePaymentRequest(BaseModel):
    amount: Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2)]
    currency: Currency
    metadata: dict[str, JsonValue]
    webhook_url: HttpUrl
    description: str

    model_config = ConfigDict(extra="forbid")

    @field_validator("webhook_url")
    @classmethod
    def validate_webhook_url_length(cls, value: HttpUrl) -> HttpUrl:
        if len(str(value)) > 2048:  # noqa: PLR2004
            raise ValueError("Normalized webhook URL must not exceed 2048 characters")
        return value


class CreatePaymentResponse(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    created_at: datetime

    model_config = ConfigDict(extra="forbid")


class GetPaymentByIdResponse(BaseModel):
    payment_id: UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, JsonValue]
    webhook_url: HttpUrl
    status: PaymentStatus
    created_at: datetime
    processed_at: datetime | None

    model_config = ConfigDict(from_attributes=True)
