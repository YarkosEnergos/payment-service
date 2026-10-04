from decimal import Decimal
from typing import Any
from uuid import uuid4

import pytest

from src.infra.rabbitmq.topology import PAYMENTS_NEW_ROUTING_KEY
from src.modules.payments.controller import PaymentController
from src.modules.payments.enums import Currency, PaymentStatus
from src.modules.payments.exceptions import (
    IdempotencyKeyReusedError,
    PaymentNotFoundError,
)
from src.modules.payments.schemas import PaymentCreateRequest
from tests.unit.fakes import FakePaymentStorage, FakeUnitOfWork, make_payment


def make_request(**overrides: object) -> PaymentCreateRequest:
    data: dict[str, Any] = {
        "amount": "100.00",
        "currency": "RUB",
        "description": "Order #1",
        "metadata": {"order_id": 1},
        "webhook_url": "http://client.test/webhooks",
    }
    data.update(overrides)
    return PaymentCreateRequest.model_validate(data)


async def test_create_payment_returns_pending_payment() -> None:
    uow = FakeUnitOfWork()
    controller = PaymentController(uow)

    result = await controller.create_payment(make_request(), "key-1")

    assert result.status is PaymentStatus.PENDING
    payment = uow.payment.payments[result.payment_id]
    assert payment.amount == Decimal("100.00")
    assert payment.currency is Currency.RUB
    assert payment.meta == {"order_id": 1}
    assert payment.idempotency_key == "key-1"
    assert payment.webhook_url == "http://client.test/webhooks"


async def test_create_payment_writes_outbox_event_in_same_commit() -> None:
    uow = FakeUnitOfWork()
    controller = PaymentController(uow)

    result = await controller.create_payment(make_request(), "key-1")

    assert len(uow.outbox.messages) == 1
    message = uow.outbox.messages[0]
    assert message.routing_key == PAYMENTS_NEW_ROUTING_KEY
    assert message.payload == {"payment_id": str(result.payment_id)}
    assert uow.commits == 1


async def test_create_payment_replay_returns_original_payment() -> None:
    uow = FakeUnitOfWork()
    controller = PaymentController(uow)
    first = await controller.create_payment(make_request(), "key-1")

    second = await controller.create_payment(make_request(), "key-1")

    assert second == first
    assert len(uow.payment.payments) == 1
    assert len(uow.outbox.messages) == 1
    assert uow.commits == 1


async def test_create_payment_replay_ignores_amount_formatting() -> None:
    uow = FakeUnitOfWork()
    controller = PaymentController(uow)
    first = await controller.create_payment(make_request(amount="100"), "key-1")

    second = await controller.create_payment(make_request(amount="100.00"), "key-1")

    assert second.payment_id == first.payment_id


async def test_create_payment_same_key_different_body_raises() -> None:
    uow = FakeUnitOfWork()
    controller = PaymentController(uow)
    await controller.create_payment(make_request(), "key-1")

    with pytest.raises(IdempotencyKeyReusedError):
        await controller.create_payment(make_request(amount="200.00"), "key-1")

    assert len(uow.payment.payments) == 1
    assert len(uow.outbox.messages) == 1


async def test_get_payment() -> None:
    payment = make_payment(status=PaymentStatus.SUCCEEDED)
    controller = PaymentController(FakeUnitOfWork(FakePaymentStorage([payment])))

    result = await controller.get_payment(payment.id)

    assert result.payment_id == payment.id
    assert result.status is PaymentStatus.SUCCEEDED
    assert result.metadata == payment.meta
    assert result.processed_at == payment.processed_at


async def test_get_payment_not_found() -> None:
    controller = PaymentController(FakeUnitOfWork())

    with pytest.raises(PaymentNotFoundError):
        await controller.get_payment(uuid4())
