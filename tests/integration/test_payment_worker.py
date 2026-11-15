import asyncio
import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING
from urllib.parse import quote, urlsplit
from uuid import UUID

import httpx
import pytest
from aio_pika import connect_robust
from sqlalchemy import select

import payment_processor.worker.app as worker_app
from payment_processor.application.commands import CreatePaymentCommand
from payment_processor.application.events import PaymentCreatedEvent
from payment_processor.application.gateway import GatewayOutcome
from payment_processor.core.config import settings
from payment_processor.infrastructure.db.models import OutboxEvent, Payment, PaymentStatus
from payment_processor.infrastructure.db.repositories import SqlAlchemyPaymentRepository
from payment_processor.infrastructure.integrations.gateway_emulator import GatewayEmulator
from payment_processor.infrastructure.messaging.broker import broker
from payment_processor.infrastructure.messaging.topology import (
    PAYMENTS_DLQ_QUEUE_NAME,
    PAYMENTS_EXCHANGE,
    PAYMENTS_NEW_QUEUE,
    PAYMENTS_NEW_QUEUE_NAME,
    PAYMENTS_RETRY_1_QUEUE_NAME,
    PAYMENTS_RETRY_2_QUEUE_NAME,
    declare_topology,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


PAYMENT_QUEUE_NAMES = (
    PAYMENTS_NEW_QUEUE_NAME,
    PAYMENTS_RETRY_1_QUEUE_NAME,
    PAYMENTS_RETRY_2_QUEUE_NAME,
    PAYMENTS_DLQ_QUEUE_NAME,
)


async def clear_queue(queue_name: str) -> None:
    connection = urlsplit(settings.rabbitmq_url)
    assert connection.username is not None
    assert connection.password is not None

    vhost = quote("/", safe="")
    url = f"{settings.rabbitmq_management_url}/api/queues/{vhost}/{queue_name}/contents"
    async with httpx.AsyncClient(trust_env=False) as client:
        response = await client.delete(
            url,
            auth=(connection.username, connection.password),
        )

    assert response.status_code == httpx.codes.NO_CONTENT


async def clear_payment_queues() -> None:
    for queue_name in PAYMENT_QUEUE_NAMES:
        await clear_queue(queue_name)


async def queue_message_count(queue_name: str = PAYMENTS_NEW_QUEUE_NAME) -> int:
    async with await connect_robust(settings.rabbitmq_url) as connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(queue_name, passive=True)
        assert queue.declaration_result is not None
        assert queue.declaration_result.message_count is not None
        return queue.declaration_result.message_count


def immediate_gateway() -> GatewayEmulator:
    return GatewayEmulator(min_delay_seconds=0, max_delay_seconds=0)


def successful_webhook_http_client() -> httpx.AsyncClient:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code=httpx.codes.NO_CONTENT)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False)


async def wait_for_completed_payment(
    session_factory: async_sessionmaker[AsyncSession],
    payment_id: UUID,
) -> Payment:
    deadline = asyncio.get_running_loop().time() + 5

    while asyncio.get_running_loop().time() < deadline:
        async with session_factory() as session:
            payment = await session.get(Payment, payment_id)

        assert payment is not None
        if payment.status is not PaymentStatus.PENDING and payment.webhook_delivered_at is not None:
            return payment

        await asyncio.sleep(0.05)

    pytest.fail("Worker did not persist a completed payment within five seconds")


async def create_payment_event(
    session_factory: async_sessionmaker[AsyncSession],
) -> tuple[UUID, PaymentCreatedEvent]:
    command = CreatePaymentCommand(
        amount=Decimal("1250.00"),
        currency="RUB",
        description="Payment for worker integration test",
        idempotency_key="worker-integration-test-key",
        metadata={"order_id": "worker-test"},
        webhook_url="http://webhook-sink:8080/callbacks/payments",
    )

    async with session_factory() as session:
        repository = SqlAlchemyPaymentRepository(session)
        payment = await repository.create_or_get_with_outbox(command)
        outbox_event = (await session.scalars(select(OutboxEvent))).one()

    return payment.payment_id, PaymentCreatedEvent.model_validate(outbox_event.payload)


@pytest.mark.integration
async def test_worker_consumes_event_and_persists_gateway_outcome(
    db_session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(worker_app, "session_factory", db_session_factory)
    monkeypatch.setattr(worker_app, "GatewayEmulator", immediate_gateway)
    monkeypatch.setattr(worker_app, "create_webhook_http_client", successful_webhook_http_client)

    async with broker:
        await declare_topology(broker)
    await clear_payment_queues()

    payment_id, event = await create_payment_event(db_session_factory)
    expected_status = PaymentStatus.FAILED if payment_id.int % 10 == 0 else PaymentStatus.SUCCEEDED

    async with worker_app.app:
        await broker.publish(
            event,
            queue=PAYMENTS_NEW_QUEUE,
            exchange=PAYMENTS_EXCHANGE,
            persist=True,
        )
        payment = await wait_for_completed_payment(db_session_factory, payment_id)

    assert payment.status is expected_status
    assert payment.processed_at is not None
    assert payment.webhook_delivered_at is not None
    assert payment.processed_at <= datetime.now(tz=UTC)
    assert await queue_message_count() == 0


@pytest.mark.integration
@pytest.mark.parametrize(
    ("outcome", "responses", "expected_status", "exhausted"),
    [
        (GatewayOutcome.SUCCEEDED, [204], PaymentStatus.SUCCEEDED, False),
        (GatewayOutcome.SUCCEEDED, [500, 500, 204], PaymentStatus.SUCCEEDED, False),
        (GatewayOutcome.SUCCEEDED, [500, 500, 500], PaymentStatus.SUCCEEDED, True),
        (GatewayOutcome.FAILED, [500, 500, 500], PaymentStatus.FAILED, True),
    ],
)
async def test_webhook_retries_and_dead_letter(  # noqa: C901, PLR0913, PLR0915, PLR0917
    db_session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
    outcome: GatewayOutcome,
    responses: list[int],
    expected_status: PaymentStatus,
    exhausted: bool,
) -> None:
    calls: list[dict[str, object]] = []
    gateway_calls: list[UUID] = []
    consumed = asyncio.Event()
    execute = worker_app.HandlePaymentEventUseCase.execute
    execution_count = 0

    async def track_execution(
        use_case: worker_app.HandlePaymentEventUseCase, event: PaymentCreatedEvent, attempt: int
    ) -> None:
        nonlocal execution_count
        try:
            await execute(use_case, event, attempt)
        finally:
            execution_count += 1
            if execution_count == len(responses) + 1:
                consumed.set()

    class Gateway:
        async def process(self, payment_id: UUID) -> GatewayOutcome:
            gateway_calls.append(payment_id)
            return outcome

    def client() -> httpx.AsyncClient:
        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(json.loads(request.content))
            return httpx.Response(responses[len(calls) - 1])

        return httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False)

    monkeypatch.setattr(worker_app, "session_factory", db_session_factory)
    monkeypatch.setattr(worker_app, "GatewayEmulator", Gateway)
    monkeypatch.setattr(worker_app, "create_webhook_http_client", client)
    monkeypatch.setattr(worker_app.HandlePaymentEventUseCase, "execute", track_execution)
    async with broker:
        await declare_topology(broker)
    await clear_payment_queues()
    payment_id, event = await create_payment_event(db_session_factory)

    async with worker_app.app:
        await broker.publish(event, queue=PAYMENTS_NEW_QUEUE, exchange=PAYMENTS_EXCHANGE, persist=True)
        deadline = asyncio.get_running_loop().time() + 10
        while asyncio.get_running_loop().time() < deadline:
            async with db_session_factory() as session:
                payment = await session.get(Payment, payment_id)
            assert payment is not None
            if exhausted:
                done = (
                    payment.webhook_delivery_exhausted_at is not None
                    and await queue_message_count(PAYMENTS_DLQ_QUEUE_NAME) == 1
                )
            else:
                done = payment.webhook_delivered_at is not None
            if done:
                break
            await asyncio.sleep(0.05)
        else:
            pytest.fail("Worker did not finish webhook delivery")

        if not exhausted:
            await broker.publish(event, queue=PAYMENTS_NEW_QUEUE, exchange=PAYMENTS_EXCHANGE, persist=True)
            await asyncio.wait_for(consumed.wait(), timeout=5)

    assert payment.status is expected_status
    assert payment.webhook_attempts == len(responses)
    assert (payment.webhook_delivery_exhausted_at is not None) is exhausted
    assert (payment.webhook_delivered_at is not None) is not exhausted
    assert len(calls) == len(responses)
    assert all(call["payment_id"] == str(payment_id) and call["status"] == expected_status.value for call in calls)
    assert gateway_calls == [payment_id]
    if not exhausted:
        assert await queue_message_count(PAYMENTS_DLQ_QUEUE_NAME) == 0


@pytest.mark.integration
async def test_redelivery_after_failure_before_retry_publish(
    db_session_factory: async_sessionmaker[AsyncSession], monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[httpx.Request] = []

    def client() -> httpx.AsyncClient:
        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            return httpx.Response(204)

        return httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False)

    monkeypatch.setattr(worker_app, "session_factory", db_session_factory)
    monkeypatch.setattr(worker_app, "create_webhook_http_client", client)
    async with broker:
        await declare_topology(broker)
    await clear_payment_queues()
    payment_id, event = await create_payment_event(db_session_factory)
    async with db_session_factory() as session:
        repository = SqlAlchemyPaymentRepository(session)
        assert await repository.record_gateway_outcome(payment_id, "failed", datetime.now(tz=UTC))
        await repository.record_webhook_failure(payment_id, 1, "simulated failure", None)

    # Simulate failure saved, but process lost before publishing retry and ACK.
    async with worker_app.app:
        await broker.publish(event, queue=PAYMENTS_NEW_QUEUE, exchange=PAYMENTS_EXCHANGE, persist=True)
        payment = await wait_for_completed_payment(db_session_factory, payment_id)

    assert payment.status is PaymentStatus.FAILED
    assert payment.webhook_attempts == 2  # noqa: PLR2004
    assert len(calls) == 1
    assert await queue_message_count(PAYMENTS_DLQ_QUEUE_NAME) == 0
