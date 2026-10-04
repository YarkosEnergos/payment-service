from uuid import uuid4

import pytest
from pydantic import BaseModel

from src.infra.postgres.models.payments import Payment
from src.modules.payments.enums import PaymentStatus
from src.modules.payments.exceptions import PaymentNotFoundError
from src.modules.payments.processor import PaymentProcessor
from src.modules.payments.schemas import PaymentResponse
from tests.unit.fakes import (
    FakePaymentStorage,
    FakeUnitOfWork,
    make_payment,
    make_uow_factory,
)


class FakeGateway:
    def __init__(self, status: PaymentStatus = PaymentStatus.SUCCEEDED) -> None:
        self.status = status
        self.charged: list[Payment] = []

    async def charge(self, payment: Payment) -> PaymentStatus:
        self.charged.append(payment)
        return self.status


class FakeWebhookSender:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.sent: list[tuple[str, PaymentResponse]] = []

    async def send(self, url: str, payload: BaseModel) -> None:
        if self.error is not None:
            raise self.error
        assert isinstance(payload, PaymentResponse)
        self.sent.append((url, payload))


def make_processor(
    uow: FakeUnitOfWork,
    gateway: FakeGateway | None = None,
    webhook_sender: FakeWebhookSender | None = None,
) -> PaymentProcessor:
    return PaymentProcessor(
        uow_factory=make_uow_factory(uow),
        gateway=gateway or FakeGateway(),
        webhook_sender=webhook_sender or FakeWebhookSender(),
    )


@pytest.mark.parametrize("status", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_process_pending_payment(status: PaymentStatus) -> None:
    payment = make_payment()
    uow = FakeUnitOfWork(FakePaymentStorage([payment]))
    gateway = FakeGateway(status)
    webhook_sender = FakeWebhookSender()

    await make_processor(uow, gateway, webhook_sender).process(payment.id)

    assert gateway.charged == [payment]
    assert payment.status is status
    assert payment.processed_at is not None
    assert uow.commits == 1
    [(url, payload)] = webhook_sender.sent
    assert url == payment.webhook_url
    assert payload.payment_id == payment.id
    assert payload.status is status


async def test_process_already_completed_payment_only_resends_webhook() -> None:
    payment = make_payment(status=PaymentStatus.FAILED)
    uow = FakeUnitOfWork(FakePaymentStorage([payment]))
    gateway = FakeGateway(PaymentStatus.SUCCEEDED)
    webhook_sender = FakeWebhookSender()

    await make_processor(uow, gateway, webhook_sender).process(payment.id)

    assert gateway.charged == []
    assert payment.status is PaymentStatus.FAILED
    assert [payload.status for _, payload in webhook_sender.sent] == [
        PaymentStatus.FAILED,
    ]


async def test_process_unknown_payment_raises() -> None:
    webhook_sender = FakeWebhookSender()
    processor = make_processor(FakeUnitOfWork(), webhook_sender=webhook_sender)

    with pytest.raises(PaymentNotFoundError):
        await processor.process(uuid4())

    assert webhook_sender.sent == []


async def test_status_is_saved_before_webhook_failure() -> None:
    payment = make_payment()
    uow = FakeUnitOfWork(FakePaymentStorage([payment]))
    webhook_sender = FakeWebhookSender(error=ConnectionError("receiver is down"))
    processor = make_processor(uow, FakeGateway(), webhook_sender)

    with pytest.raises(ConnectionError):
        await processor.process(payment.id)

    assert payment.status is PaymentStatus.SUCCEEDED
    assert uow.commits == 1


async def test_concurrently_completed_payment_keeps_first_result() -> None:
    payment = make_payment()
    uow = FakeUnitOfWork(FakePaymentStorage([payment]))

    class RacingGateway(FakeGateway):
        async def charge(self, payment: Payment) -> PaymentStatus:
            payment.status = PaymentStatus.SUCCEEDED
            return PaymentStatus.FAILED

    webhook_sender = FakeWebhookSender()

    await make_processor(uow, RacingGateway(), webhook_sender).process(payment.id)

    assert payment.status is PaymentStatus.SUCCEEDED
    assert [payload.status for _, payload in webhook_sender.sent] == [
        PaymentStatus.SUCCEEDED,
    ]
