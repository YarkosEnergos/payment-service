from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Header, status

from src.api.schemas import ErrorResponse
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


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Create a payment",
    description=(
        "Accepts a payment for asynchronous processing. Repeating the request "
        "with the same Idempotency-Key and body returns the original payment; "
        "reusing the key with a different body returns 422."
    ),
)
async def create_payment(
    payment_data: PaymentCreateRequest,
    idempotency_key: IdempotencyKey,
    controller: PaymentControllerDep,
) -> PaymentCreateResponse:
    return await controller.create_payment(payment_data, idempotency_key)


@router.get(
    "/{payment_id}",
    summary="Get payment details",
    responses={
        status.HTTP_404_NOT_FOUND: {
            "model": ErrorResponse,
            "description": "Payment not found",
        },
    },
)
async def get_payment(
    payment_id: UUID,
    controller: PaymentControllerDep,
) -> PaymentResponse:
    return await controller.get_payment(payment_id)
