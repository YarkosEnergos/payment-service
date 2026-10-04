from fastapi import APIRouter, Security

from src.api.security import verify_api_key
from src.api.v1.router import router as v1_router

router = APIRouter(prefix="/api", dependencies=[Security(verify_api_key)])

router.include_router(v1_router)
