from fastapi import APIRouter

from src.modules.payments.router import router as payment_router

router = APIRouter(prefix="/v1")
router.include_router(payment_router)
