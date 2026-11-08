import pytest
from pydantic import ValidationError

from payment_processor.application.events import PaymentCreatedEvent


def payment_created_payload() -> dict[str, object]:
    return {
        "event_id": "5e9e01f1-d973-4c4d-9d39-8687d28847cb",
        "event_type": "payment.created",
        "event_version": 1,
        "payment_id": "8404d263-1a58-4580-a901-b763c3c21cce",
        "created_at": "2026-11-02T12:00:00Z",
        "payment": {
            "amount": "1250.00",
            "currency": "RUB",
            "description": "Payment for order #42",
            "metadata": {"order_id": "42"},
            "webhook_url": "http://webhook-sink:8080/callbacks/payments",
        },
    }


def test_payment_created_event_validates_and_serializes_outbox_payload() -> None:
    payload = payment_created_payload()

    event = PaymentCreatedEvent.model_validate(payload)

    assert event.model_dump(mode="json") == payload


def test_payment_created_event_rejects_an_unknown_event_type() -> None:
    payload = payment_created_payload() | {"event_type": "payment.processed"}

    with pytest.raises(ValidationError):
        PaymentCreatedEvent.model_validate(payload)
