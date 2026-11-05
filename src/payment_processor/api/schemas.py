from datetime import datetime
from decimal import Decimal
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, JsonValue


class CreatePaymentRequest(BaseModel):
    amount: Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2)]
    currency: Literal["RUB", "USD", "EUR"]
    metadata: dict[str, JsonValue]
    webhook_url: HttpUrl
    description: str

    model_config = ConfigDict(extra="forbid")


class CreatePaymentResponse(BaseModel):
    payment_id: UUID
    status: Literal["pending", "succeeded", "failed"]
    created_at: datetime

    model_config = ConfigDict(extra="forbid")


class GetPaymentByIdResponse(BaseModel):
    payment_id: UUID
    amount: Decimal
    currency: Literal["RUB", "USD", "EUR"]
    description: str
    metadata: dict[str, JsonValue]
    webhook_url: HttpUrl
    status: Literal["pending", "succeeded", "failed"]
    created_at: datetime
    processed_at: datetime | None

    model_config = ConfigDict(from_attributes=True)
