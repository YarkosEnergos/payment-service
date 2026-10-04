from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from src.modules.payments.exceptions import (
    IdempotencyKeyReusedError,
    PaymentNotFoundError,
)

ERROR_STATUS_CODES: dict[type[Exception], int] = {
    PaymentNotFoundError: status.HTTP_404_NOT_FOUND,
    IdempotencyKeyReusedError: status.HTTP_422_UNPROCESSABLE_CONTENT,
}


async def domain_error_handler(_request: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(
        status_code=ERROR_STATUS_CODES[type(exc)],
        content={"detail": str(exc)},
    )


def register_exception_handlers(app: FastAPI) -> None:
    for exc_type in ERROR_STATUS_CODES:
        app.add_exception_handler(exc_type, domain_error_handler)
