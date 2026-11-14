from uuid import UUID

from payment_processor.application.gateway import GatewayOutcome
from payment_processor.infrastructure.integrations.gateway_emulator import GatewayEmulator


async def test_gateway_emulator_returns_a_deterministic_business_outcome() -> None:
    gateway = GatewayEmulator(min_delay_seconds=0, max_delay_seconds=0)

    declined = await gateway.process(payment_id=UUID(int=10))
    succeeded = await gateway.process(payment_id=UUID(int=11))

    assert declined is GatewayOutcome.FAILED
    assert succeeded is GatewayOutcome.SUCCEEDED
