from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Header, status

from src.modules.payments.controller import PaymentControllerDep
from src.modules.payments.schemas import (
    PaymentCreateRequest,
    PaymentCreateResponse,
    PaymentResponse,
)

router = APIRouter(prefix="/payments", tags=["payments"])

IdempotencyKey = Annotated[
    str,
    Header(alias="Idempotency-Key", min_length=1, max_length=255),
]


@router.post("", status_code=status.HTTP_202_ACCEPTED)
async def create_payment(
    payment_data: PaymentCreateRequest,
    idempotency_key: IdempotencyKey,
    controller: PaymentControllerDep,
) -> PaymentCreateResponse:
    return await controller.create_payment(payment_data, idempotency_key)


@router.get("/{payment_id}")
async def get_payment(
    payment_id: UUID,
    controller: PaymentControllerDep,
) -> PaymentResponse:
    return await controller.get_payment(payment_id)
