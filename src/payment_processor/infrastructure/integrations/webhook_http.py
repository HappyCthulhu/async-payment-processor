from typing import TYPE_CHECKING, override

import httpx

from payment_processor.application.errors import WebhookDeliveryError
from payment_processor.application.ports import WebhookClient

if TYPE_CHECKING:
    from payment_processor.application.events import WebhookNotification


class HttpWebhookClient(WebhookClient):
    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    @override
    async def deliver(self, webhook_url: str, notification: WebhookNotification) -> None:
        try:
            response = await self._client.post(
                webhook_url,
                json=notification.model_dump(mode="json"),
            )
            response.raise_for_status()
        except httpx.HTTPError as error:
            raise WebhookDeliveryError("Webhook request failed") from error
