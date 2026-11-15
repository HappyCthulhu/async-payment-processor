import asyncio
import json
from typing import TYPE_CHECKING
from urllib.parse import quote, urlsplit
from uuid import uuid4

import httpx
import pytest

from payment_processor.core.config import settings
from payment_processor.infrastructure.messaging.broker import broker
from payment_processor.infrastructure.messaging.topology import (
    PAYMENTS_DLQ_QUEUE_NAME,
    PAYMENTS_EXCHANGE,
    PAYMENTS_EXCHANGE_NAME,
    PAYMENTS_NEW_QUEUE_NAME,
    PAYMENTS_RETRY_1_QUEUE_NAME,
    PAYMENTS_RETRY_2_QUEUE_NAME,
    declare_topology,
)

if TYPE_CHECKING:
    from collections.abc import Mapping


PAYMENTS_QUEUE_NAMES = (
    PAYMENTS_NEW_QUEUE_NAME,
    PAYMENTS_RETRY_1_QUEUE_NAME,
    PAYMENTS_RETRY_2_QUEUE_NAME,
    PAYMENTS_DLQ_QUEUE_NAME,
)


def rabbitmq_auth() -> tuple[str, str]:
    connection = urlsplit(settings.rabbitmq_url)
    assert connection.username is not None
    assert connection.password is not None
    return connection.username, connection.password


def queue_url(queue_name: str) -> str:
    vhost = quote("/", safe="")
    return f"{settings.rabbitmq_management_url}/api/queues/{vhost}/{queue_name}"


async def recreate_payment_topology() -> None:
    async with httpx.AsyncClient(trust_env=False) as client:
        for queue_name in PAYMENTS_QUEUE_NAMES:
            response = await client.delete(queue_url(queue_name), auth=rabbitmq_auth())
            assert response.status_code in {httpx.codes.NO_CONTENT, httpx.codes.NOT_FOUND}

    async with broker:
        await declare_topology(broker)


async def get_queue(queue_name: str) -> Mapping[str, object]:
    async with httpx.AsyncClient(trust_env=False) as client:
        response = await client.get(queue_url(queue_name), auth=rabbitmq_auth())

    assert response.status_code == httpx.codes.OK
    return response.json()


def assert_queue_arguments(queue: Mapping[str, object], expected: Mapping[str, object]) -> None:
    arguments = queue["arguments"]
    assert isinstance(arguments, dict)
    assert {name: arguments[name] for name in expected} == expected


@pytest.mark.integration
async def test_declare_topology_creates_durable_main_retry_and_dead_letter_queues() -> None:
    await recreate_payment_topology()

    connection = urlsplit(settings.rabbitmq_url)
    assert connection.username is not None
    assert connection.password is not None
    vhost = quote("/", safe="")

    async with httpx.AsyncClient(trust_env=False) as client:
        exchange_response = await client.get(
            f"{settings.rabbitmq_management_url}/api/exchanges/{vhost}/{PAYMENTS_EXCHANGE_NAME}",
            auth=(connection.username, connection.password),
        )
        bindings_response = await client.get(
            f"{settings.rabbitmq_management_url}/api/exchanges/{vhost}/{PAYMENTS_EXCHANGE_NAME}/bindings/source",
            auth=(connection.username, connection.password),
        )

    assert exchange_response.status_code == httpx.codes.OK
    assert bindings_response.status_code == httpx.codes.OK
    assert exchange_response.json()["type"] == "direct"
    assert exchange_response.json()["durable"] is True

    main_queue = await get_queue(PAYMENTS_NEW_QUEUE_NAME)
    retry_1_queue = await get_queue(PAYMENTS_RETRY_1_QUEUE_NAME)
    retry_2_queue = await get_queue(PAYMENTS_RETRY_2_QUEUE_NAME)
    dlq = await get_queue(PAYMENTS_DLQ_QUEUE_NAME)

    assert all(queue["durable"] is True for queue in (main_queue, retry_1_queue, retry_2_queue, dlq))
    assert_queue_arguments(
        main_queue,
        {
            "x-dead-letter-exchange": PAYMENTS_EXCHANGE_NAME,
            "x-dead-letter-routing-key": PAYMENTS_DLQ_QUEUE_NAME,
        },
    )
    assert_queue_arguments(
        retry_1_queue,
        {
            "x-message-ttl": 1_000,
            "x-dead-letter-exchange": PAYMENTS_EXCHANGE_NAME,
            "x-dead-letter-routing-key": PAYMENTS_NEW_QUEUE_NAME,
        },
    )
    assert_queue_arguments(
        retry_2_queue,
        {
            "x-message-ttl": 2_000,
            "x-dead-letter-exchange": PAYMENTS_EXCHANGE_NAME,
            "x-dead-letter-routing-key": PAYMENTS_NEW_QUEUE_NAME,
        },
    )
    assert_queue_arguments(dlq, {})

    bindings = bindings_response.json()
    assert {
        binding["routing_key"]
        for binding in bindings
        if binding["destination_type"] == "queue" and binding["destination"] in PAYMENTS_QUEUE_NAMES
    } == set(PAYMENTS_QUEUE_NAMES)


@pytest.mark.integration
async def test_retry_queue_dead_letters_an_expired_message_back_to_main_queue() -> None:
    await recreate_payment_topology()
    probe_id = str(uuid4())

    async with broker:
        await broker.publish(
            {"probe_id": probe_id},
            queue=PAYMENTS_RETRY_1_QUEUE_NAME,
            exchange=PAYMENTS_EXCHANGE,
            persist=True,
        )

    deadline = asyncio.get_running_loop().time() + 5
    while asyncio.get_running_loop().time() < deadline:
        async with httpx.AsyncClient(trust_env=False) as client:
            response = await client.post(
                f"{queue_url(PAYMENTS_NEW_QUEUE_NAME)}/get",
                auth=rabbitmq_auth(),
                json={"count": 1, "ackmode": "ack_requeue_false", "encoding": "auto", "truncate": 50_000},
            )

        assert response.status_code == httpx.codes.OK
        messages = response.json()
        if messages:
            assert json.loads(messages[0]["payload"]) == {"probe_id": probe_id}
            return
        await asyncio.sleep(0.1)

    pytest.fail("Retry message did not return to payments.new within five seconds")
