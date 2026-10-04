import logging
from typing import Annotated
from uuid import UUID

from fastapi import Depends

from src.infra.postgres.uow import PostgresUnitOfWorkDep
from src.infra.rabbitmq.topology import PAYMENTS_NEW_ROUTING_KEY
from src.modules.base.controller import BaseController
from src.modules.payments.exceptions import (
    IdempotencyKeyReusedError,
    PaymentNotFoundError,
)
from src.modules.payments.schemas import (
    PaymentCreatedEvent,
    PaymentCreateRequest,
    PaymentCreateResponse,
    PaymentResponse,
)

logger = logging.getLogger(__name__)


class PaymentController(BaseController):
    async def create_payment(
        self,
        payment_data: PaymentCreateRequest,
        idempotency_key: str,
    ) -> PaymentCreateResponse:
        request_hash = payment_data.fingerprint()

        payment = await self.uow.payment.create(
            amount=payment_data.amount,
            currency=payment_data.currency,
            description=payment_data.description,
            meta=payment_data.metadata,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            webhook_url=str(payment_data.webhook_url),
        )
        if payment is None:
            return await self._replay(idempotency_key, request_hash)

        event = PaymentCreatedEvent(payment_id=payment.id)
        await self.uow.outbox.add(
            routing_key=PAYMENTS_NEW_ROUTING_KEY,
            payload=event.model_dump(mode="json"),
        )
        await self.uow.commit()

        logger.info("Payment %s created", payment.id)
        return PaymentCreateResponse.from_model(payment)

    async def get_payment(self, payment_id: UUID) -> PaymentResponse:
        payment = await self.uow.payment.get_by_id(payment_id)
        if payment is None:
            raise PaymentNotFoundError(payment_id)
        return PaymentResponse.from_model(payment)

    async def _replay(
        self,
        idempotency_key: str,
        request_hash: str,
    ) -> PaymentCreateResponse:
        payment = await self.uow.payment.get_by_idempotency_key(idempotency_key)
        if payment is None or payment.request_hash != request_hash:
            raise IdempotencyKeyReusedError(idempotency_key)

        logger.info(
            "Idempotency key %r replayed for payment %s",
            idempotency_key,
            payment.id,
        )
        return PaymentCreateResponse.from_model(payment)


async def get_controller(uow: PostgresUnitOfWorkDep) -> PaymentController:
    return PaymentController(uow)


PaymentControllerDep = Annotated[PaymentController, Depends(get_controller)]
