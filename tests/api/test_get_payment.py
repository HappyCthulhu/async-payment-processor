from typing import TYPE_CHECKING
from uuid import UUID, uuid4

import pytest
from fastapi import status

from payment_processor.core.config import settings

if TYPE_CHECKING:
    from httpx import AsyncClient, Response


PAYMENTS_PATH = "/api/v1/payments"
PAYMENT_REQUEST = {
    "amount": "1250.00",
    "currency": "RUB",
    "description": "Payment for order #42",
    "metadata": {"order_id": "42"},
    "webhook_url": "http://webhook-sink:8080/callbacks/payments",
}


def api_key_headers() -> dict[str, str]:
    return {"X-API-Key": settings.api_key}


async def create_payment(client: AsyncClient) -> Response:
    response = await client.post(
        PAYMENTS_PATH,
        headers={**api_key_headers(), "Idempotency-Key": "get-payment-test-key"},
        json=PAYMENT_REQUEST,
    )
    assert response.status_code == status.HTTP_202_ACCEPTED
    return response


@pytest.mark.integration
async def test_get_payment_returns_public_payment_data(client: AsyncClient) -> None:
    create_response = await create_payment(client)
    payment_id = UUID(create_response.json()["payment_id"])

    response = await client.get(f"{PAYMENTS_PATH}/{payment_id}", headers=api_key_headers())

    assert response.status_code == status.HTTP_200_OK
    payload = response.json()
    assert set(payload) == {
        "payment_id",
        "amount",
        "currency",
        "description",
        "metadata",
        "webhook_url",
        "status",
        "created_at",
        "processed_at",
    }
    assert payload == {
        "payment_id": str(payment_id),
        "amount": "1250.00",
        "currency": "RUB",
        "description": "Payment for order #42",
        "metadata": {"order_id": "42"},
        "webhook_url": "http://webhook-sink:8080/callbacks/payments",
        "status": "pending",
        "created_at": create_response.json()["created_at"],
        "processed_at": None,
    }


@pytest.mark.integration
async def test_get_payment_returns_not_found_for_unknown_payment_id(client: AsyncClient) -> None:
    response = await client.get(f"{PAYMENTS_PATH}/{uuid4()}", headers=api_key_headers())

    assert response.status_code == status.HTTP_404_NOT_FOUND


@pytest.mark.integration
@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"X-API-Key": "incorrect"},
    ],
)
async def test_get_payment_rejects_missing_or_invalid_api_key(
    client: AsyncClient,
    headers: dict[str, str],
) -> None:
    response = await client.get(f"{PAYMENTS_PATH}/{uuid4()}", headers=headers)

    assert response.status_code == status.HTTP_401_UNAUTHORIZED
