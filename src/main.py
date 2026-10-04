from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.api.exception_handlers import register_exception_handlers
from src.api.router import router as api_router
from src.infra.postgres.pg import engine
from src.logging_config import setup_logging

setup_logging()


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    yield
    await engine.dispose()


app = FastAPI(title="Payment Service", version="0.1.0", lifespan=lifespan)
app.include_router(api_router)
register_exception_handlers(app)
