from fastapi import APIRouter, Security, status

from src.api.schemas import ErrorResponse
from src.api.security import verify_api_key
from src.api.v1.router import router as v1_router

router = APIRouter(
    prefix="/api",
    dependencies=[Security(verify_api_key)],
    responses={
        status.HTTP_401_UNAUTHORIZED: {
            "model": ErrorResponse,
            "description": "Missing or invalid X-API-Key",
        },
    },
)

router.include_router(v1_router)
