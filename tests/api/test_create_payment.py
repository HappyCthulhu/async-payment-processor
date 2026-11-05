from collections.abc import AsyncIterator, Iterator
from typing import TYPE_CHECKING
from uuid import UUID

import pytest
import pytest_asyncio
from fastapi import status
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select

from payment_processor.api.app import create_app
from payment_processor.api.dependencies import get_session
from payment_processor.core.config import settings
from payment_processor.infrastructure.db.models import OutboxEvent, Payment

if TYPE_CHECKING:
    from fastapi import FastAPI
    from httpx import Response
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


PAYMENTS_PATH = "/api/v1/payments"
PAYMENT_REQUEST = {
    "amount": "1250.00",
    "currency": "RUB",
    "description": "Payment for order #42",
    "metadata": {"order_id": "42"},
    "webhook_url": "http://webhook-sink:8080/callbacks/payments",
}


@pytest.fixture
def app(db_session_factory: async_sessionmaker[AsyncSession]) -> Iterator[FastAPI]:
    app = create_app()

    async def get_test_session() -> AsyncIterator[AsyncSession]:
        async with db_session_factory() as session:
            yield session

    app.dependency_overrides[get_session] = get_test_session
    yield app
    app.dependency_overrides.clear()


@pytest_asyncio.fixture
async def client(app: FastAPI) -> AsyncIterator[AsyncClient]:
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://testserver") as async_client:
        yield async_client


def payment_headers(idempotency_key: str = "api-test-key") -> dict[str, str]:
    return {
        "X-API-Key": settings.api_key,
        "Idempotency-Key": idempotency_key,
    }


def assert_accepted_payment(response: Response) -> dict[str, object]:
    assert response.status_code == status.HTTP_202_ACCEPTED
    payload = response.json()
    assert set(payload) == {"payment_id", "status", "created_at"}
    assert UUID(payload["payment_id"])
    assert payload["status"] == "pending"
    assert isinstance(payload["created_at"], str)
    return payload


@pytest.mark.integration
async def test_create_payment_accepts_request_and_persists_payment_with_outbox(
    client: AsyncClient,
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    response = await client.post(PAYMENTS_PATH, headers=payment_headers(), json=PAYMENT_REQUEST)

    response_payload = assert_accepted_payment(response)

    async with db_session_factory() as session:
        payments = list((await session.scalars(select(Payment))).all())
        outbox_events = list((await session.scalars(select(OutboxEvent))).all())

    assert len(payments) == 1
    assert len(outbox_events) == 1
    assert str(payments[0].id) == response_payload["payment_id"]


@pytest.mark.integration
async def test_create_payment_replay_returns_existing_payment_without_duplicate_outbox_event(
    client: AsyncClient,
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    first_response = await client.post(PAYMENTS_PATH, headers=payment_headers(), json=PAYMENT_REQUEST)
    second_response = await client.post(PAYMENTS_PATH, headers=payment_headers(), json=PAYMENT_REQUEST)

    assert_accepted_payment(first_response)
    assert first_response.json() == second_response.json()

    async with db_session_factory() as session:
        payments = list((await session.scalars(select(Payment))).all())
        outbox_events = list((await session.scalars(select(OutboxEvent))).all())

    assert len(payments) == 1
    assert len(outbox_events) == 1


@pytest.mark.integration
async def test_create_payment_replay_with_changed_business_data_returns_conflict(client: AsyncClient) -> None:
    await client.post(PAYMENTS_PATH, headers=payment_headers(), json=PAYMENT_REQUEST)

    changed_request = {**PAYMENT_REQUEST, "amount": "1300.00"}
    response = await client.post(PAYMENTS_PATH, headers=payment_headers(), json=changed_request)

    assert response.status_code == status.HTTP_409_CONFLICT


@pytest.mark.integration
@pytest.mark.parametrize(
    "headers",
    [
        {"Idempotency-Key": "api-test-key"},
        {"X-API-Key": "incorrect", "Idempotency-Key": "api-test-key"},
    ],
)
async def test_create_payment_rejects_missing_or_invalid_api_key(
    client: AsyncClient,
    headers: dict[str, str],
) -> None:
    response = await client.post(PAYMENTS_PATH, headers=headers, json=PAYMENT_REQUEST)

    assert response.status_code == status.HTTP_401_UNAUTHORIZED


@pytest.mark.integration
async def test_create_payment_rejects_missing_idempotency_key(client: AsyncClient) -> None:
    response = await client.post(
        PAYMENTS_PATH,
        headers={"X-API-Key": settings.api_key},
        json=PAYMENT_REQUEST,
    )

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT


@pytest.mark.integration
async def test_create_payment_rejects_unknown_request_fields(client: AsyncClient) -> None:
    invalid_request = {**PAYMENT_REQUEST, "unexpected": "value"}

    response = await client.post(PAYMENTS_PATH, headers=payment_headers(), json=invalid_request)

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
