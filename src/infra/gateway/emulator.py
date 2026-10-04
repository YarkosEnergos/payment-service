import asyncio
import logging
import random
import secrets

from src.infra.postgres.models.payments import Payment
from src.modules.payments.enums import PaymentStatus

logger = logging.getLogger(__name__)


class PaymentGatewayEmulator:
    def __init__(
        self,
        min_delay_seconds: float,
        max_delay_seconds: float,
        success_rate: float,
        rng: random.Random | None = None,
    ) -> None:
        self.min_delay_seconds = min_delay_seconds
        self.max_delay_seconds = max_delay_seconds
        self.success_rate = success_rate
        self.rng = rng or secrets.SystemRandom()

    async def charge(self, payment: Payment) -> PaymentStatus:
        delay = self.rng.uniform(self.min_delay_seconds, self.max_delay_seconds)
        await asyncio.sleep(delay)

        succeeded = self.rng.random() < self.success_rate
        status = PaymentStatus.SUCCEEDED if succeeded else PaymentStatus.FAILED
        logger.info(
            "Gateway processed payment %s in %.2fs: %s",
            payment.id,
            delay,
            status,
        )
        return status
