import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING
from uuid import UUID

import httpx
import pytest

from payment_processor.application.errors import WebhookDeliveryError
from payment_processor.application.events import WebhookNotification
from payment_processor.infrastructure.integrations.webhook_http import HttpWebhookClient

if TYPE_CHECKING:
    from collections.abc import Callable


WEBHOOK_URL = "https://merchant.example.test/payments"
WEBHOOK_NOTIFICATION = WebhookNotification(
    event_id=UUID("0b0dd7a6-d884-4d3b-b59b-92e7c552f3fd"),
    payment_id=UUID("c0ea542e-0a1a-49bc-8aa8-5e34858b5be6"),
    status="succeeded",
    amount=Decimal("1250.00"),
    currency="RUB",
    metadata={"order_id": "42"},
    processed_at=datetime(2026, 11, 15, 12, 0, tzinfo=UTC),
)


@pytest.mark.parametrize(
    "status_code",
    [httpx.codes.OK, httpx.codes.CREATED, httpx.codes.NO_CONTENT],
)
async def test_webhook_client_accepts_every_2xx_response(status_code: int) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(status_code=status_code)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        webhook_client = HttpWebhookClient(http_client)
        await webhook_client.deliver(WEBHOOK_URL, WEBHOOK_NOTIFICATION)

    assert len(requests) == 1
    request = requests[0]
    assert request.method == "POST"
    assert str(request.url) == WEBHOOK_URL
    assert json.loads(request.content) == {
        "event_id": str(WEBHOOK_NOTIFICATION.event_id),
        "payment_id": str(WEBHOOK_NOTIFICATION.payment_id),
        "status": "succeeded",
        "amount": "1250.00",
        "currency": "RUB",
        "metadata": {"order_id": "42"},
        "processed_at": "2026-11-15T12:00:00Z",
    }


@pytest.mark.parametrize("status_code", [httpx.codes.BAD_REQUEST, httpx.codes.INTERNAL_SERVER_ERROR])
async def test_webhook_client_wraps_non_2xx_response_in_application_error(status_code: int) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code=status_code)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        webhook_client = HttpWebhookClient(http_client)

        with pytest.raises(WebhookDeliveryError):
            await webhook_client.deliver(WEBHOOK_URL, WEBHOOK_NOTIFICATION)


@pytest.mark.parametrize(
    "request_error_factory",
    [
        lambda request: httpx.ConnectError("Connection refused", request=request),
        lambda request: httpx.ReadTimeout("Timed out", request=request),
    ],
)
async def test_webhook_client_wraps_network_errors_in_application_error(
    request_error_factory: Callable[[httpx.Request], httpx.RequestError],
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise request_error_factory(request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        webhook_client = HttpWebhookClient(http_client)

        with pytest.raises(WebhookDeliveryError):
            await webhook_client.deliver(WEBHOOK_URL, WEBHOOK_NOTIFICATION)
