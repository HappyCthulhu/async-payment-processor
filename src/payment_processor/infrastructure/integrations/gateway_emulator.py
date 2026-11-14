import asyncio
from typing import TYPE_CHECKING, override

from payment_processor.application.gateway import GatewayOutcome
from payment_processor.application.ports import PaymentGateway

if TYPE_CHECKING:
    from uuid import UUID


class GatewayEmulator(PaymentGateway):
    def __init__(self, min_delay_seconds: float = 2, max_delay_seconds: float = 5) -> None:
        if min_delay_seconds < 0 or max_delay_seconds < min_delay_seconds:
            msg = "Gateway delay range must be non-negative and ordered"
            raise ValueError(msg)

        self._min_delay_seconds = min_delay_seconds
        self._max_delay_seconds = max_delay_seconds

    @override
    async def process(self, payment_id: UUID) -> GatewayOutcome:
        await asyncio.sleep(self._delay_for(payment_id))

        if payment_id.int % 10 == 0:
            return GatewayOutcome.FAILED
        return GatewayOutcome.SUCCEEDED

    def _delay_for(self, payment_id: UUID) -> float:
        if self._min_delay_seconds == self._max_delay_seconds:
            return self._min_delay_seconds

        fraction = (payment_id.int % 1_000) / 999
        return self._min_delay_seconds + (self._max_delay_seconds - self._min_delay_seconds) * fraction
