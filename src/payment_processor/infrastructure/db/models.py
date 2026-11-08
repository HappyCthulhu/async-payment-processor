from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from uuid import UUID, uuid4

from sqlalchemy import (
    JSON,
    CheckConstraint,
    Enum as SAEnum,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import DateTime

from payment_processor.infrastructure.db.base import BaseModel


class PaymentStatus(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class PaymentCurrency(StrEnum):
    RUB = "RUB"
    EUR = "EUR"
    USD = "USD"


class OutboxEventType(StrEnum):
    CREATED = "payment.created"


class OutboxStatus(StrEnum):
    PENDING = "pending"
    PROCESSING = "processing"
    PUBLISHED = "published"


class Payment(BaseModel):
    __tablename__ = "payments"
    __table_args__ = (CheckConstraint("amount > 0", name="ck_payments_amount_positive"),)

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    idempotency_key: Mapped[str] = mapped_column(String(255), unique=True)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    status: Mapped[PaymentStatus] = mapped_column(
        SAEnum(
            PaymentStatus,
            name="payment_status",
            values_callable=lambda enum_cls: [item.value for item in enum_cls],
        ),
        default=PaymentStatus.PENDING,
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    webhook_url: Mapped[str] = mapped_column(String(2048))
    webhook_attempts: Mapped[int] = mapped_column(Integer, default=0)
    webhook_delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    webhook_delivery_exhausted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    currency: Mapped[PaymentCurrency] = mapped_column(
        SAEnum(
            PaymentCurrency,
            name="payment_currency",
            values_callable=lambda enum_cls: [item.value for item in enum_cls],
        ),
    )
    description: Mapped[str] = mapped_column(Text)
    payment_metadata: Mapped[dict[str, object]] = mapped_column("metadata", JSON)
    last_error: Mapped[str | None] = mapped_column(Text)


class OutboxEvent(BaseModel):
    __tablename__ = "outbox"
    __table_args__ = (UniqueConstraint("payment_id", "event_type", name="uq_outbox_payment_event_type"),)

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    payment_id: Mapped[UUID] = mapped_column(ForeignKey("payments.id"))

    event_type: Mapped[OutboxEventType] = mapped_column(
        SAEnum(
            OutboxEventType,
            name="outbox_event_type",
            values_callable=lambda enum_cls: [item.value for item in enum_cls],
        ),
    )
    event_version: Mapped[int] = mapped_column(Integer, default=1)
    payload: Mapped[dict[str, object]] = mapped_column(JSON)
    status: Mapped[OutboxStatus] = mapped_column(
        SAEnum(
            OutboxStatus,
            name="outbox_status",
            values_callable=lambda enum_cls: [item.value for item in enum_cls],
        ),
        default=OutboxStatus.PENDING,
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    publish_attempts: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    claim_token: Mapped[UUID | None] = mapped_column()
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(Text)
