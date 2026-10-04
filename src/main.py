from fastapi import FastAPI

from src.api.router import router as api_router
from src.logging_config import setup_logging

setup_logging()

app = FastAPI(title="Payment Service", version="0.1.0")
app.include_router(api_router)
