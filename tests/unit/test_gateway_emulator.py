import random

import pytest

from src.infra.gateway.emulator import PaymentGatewayEmulator
from src.modules.payments.enums import PaymentStatus
from tests.unit.fakes import make_payment


@pytest.mark.parametrize(
    ("success_rate", "expected"),
    [(1.0, PaymentStatus.SUCCEEDED), (0.0, PaymentStatus.FAILED)],
)
async def test_charge_respects_success_rate(
    success_rate: float,
    expected: PaymentStatus,
) -> None:
    gateway = PaymentGatewayEmulator(0, 0, success_rate)

    assert await gateway.charge(make_payment()) is expected


async def test_charge_success_ratio_is_close_to_configured() -> None:
    gateway = PaymentGatewayEmulator(0, 0, 0.9, rng=random.Random(42))
    payment = make_payment()

    results = [await gateway.charge(payment) for _ in range(1000)]

    assert 850 <= results.count(PaymentStatus.SUCCEEDED) <= 950
