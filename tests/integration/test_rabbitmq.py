from urllib.parse import urlsplit

import httpx
import pytest

from payment_processor.core.config import settings


@pytest.mark.integration
async def test_rabbitmq_management_api_is_available() -> None:
    """The local broker must be reachable before relay integration tests can run."""
    connection = urlsplit(settings.rabbitmq_url)

    assert connection.username is not None
    assert connection.password is not None

    async with httpx.AsyncClient(trust_env=False) as client:
        response = await client.get(
            f"{settings.rabbitmq_management_url}/api/overview",
            auth=(connection.username, connection.password),
        )

    assert response.status_code == httpx.codes.OK
    assert "rabbitmq_version" in response.json()
