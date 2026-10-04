from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.app_config import app_config

engine = create_async_engine(app_config.postgres.url, pool_pre_ping=True)

session_factory = async_sessionmaker(engine, expire_on_commit=False)


@asynccontextmanager
async def get_db(
    factory: async_sessionmaker[AsyncSession] = session_factory,
) -> AsyncIterator[AsyncSession]:
    async with factory() as db:
        try:
            yield db
        except Exception:
            await db.rollback()
            raise


async def get_session() -> AsyncIterator[AsyncSession]:
    async with get_db() as db:
        yield db
