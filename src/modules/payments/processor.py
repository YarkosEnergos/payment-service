import logging
from typing import Protocol
from uuid import UUID

from pydantic import BaseModel

from src.infra.postgres.models.payments import Payment
from src.infra.postgres.uow import UnitOfWorkFactory
from src.modules.payments.enums import PaymentStatus
from src.modules.payments.exceptions import PaymentNotFoundError
from src.modules.payments.schemas import PaymentResponse

logger = logging.getLogger(__name__)


class PaymentGateway(Protocol):
    async def charge(self, payment: Payment) -> PaymentStatus: ...


class WebhookSender(Protocol):
    async def send(self, url: str, payload: BaseModel) -> None: ...


class PaymentProcessor:
    def __init__(
        self,
        uow_factory: UnitOfWorkFactory,
        gateway: PaymentGateway,
        webhook_sender: WebhookSender,
    ) -> None:
        self.uow_factory = uow_factory
        self.gateway = gateway
        self.webhook_sender = webhook_sender

    async def process(self, payment_id: UUID) -> None:
        payment = await self._get_payment(payment_id)

        if payment.status is PaymentStatus.PENDING:
            payment = await self._charge(payment)
        else:
            logger.info(
                "Payment %s is already %s, skipping gateway",
                payment.id,
                payment.status,
            )

        await self.webhook_sender.send(
            payment.webhook_url,
            PaymentResponse.from_model(payment),
        )
        logger.info("Webhook for payment %s delivered", payment.id)

    async def _get_payment(self, payment_id: UUID) -> Payment:
        async with self.uow_factory() as uow:
            payment = await uow.payment.get_by_id(payment_id)
        if payment is None:
            raise PaymentNotFoundError(payment_id)
        return payment

    async def _charge(self, payment: Payment) -> Payment:
        status = await self.gateway.charge(payment)

        async with self.uow_factory() as uow:
            completed = await uow.payment.complete(payment.id, status)
            await uow.commit()

        if completed is None:
            logger.info("Payment %s was completed concurrently", payment.id)
            return await self._get_payment(payment.id)

        logger.info("Payment %s %s", completed.id, completed.status)
        return completed
