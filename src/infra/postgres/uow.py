from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Annotated

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.infra.postgres.pg import get_db, get_session, session_factory
from src.infra.postgres.storage.outbox import OutboxStorage
from src.infra.postgres.storage.payments import PaymentStorage


class PostgresUnitOfWork:
    def __init__(self, db: AsyncSession) -> None:
        self.db = db
        self.payment = PaymentStorage(db=db)
        self.outbox = OutboxStorage(db=db)

    async def commit(self) -> None:
        await self.db.commit()

    async def rollback(self) -> None:
        await self.db.rollback()


type UnitOfWorkFactory = Callable[
    [],
    AbstractAsyncContextManager[PostgresUnitOfWork],
]


@asynccontextmanager
async def unit_of_work(
    factory: async_sessionmaker[AsyncSession] = session_factory,
) -> AsyncIterator[PostgresUnitOfWork]:
    async with get_db(factory) as db:
        yield PostgresUnitOfWork(db)


async def get_uow(
    db: Annotated[AsyncSession, Depends(get_session)],
) -> PostgresUnitOfWork:
    return PostgresUnitOfWork(db)


PostgresUnitOfWorkDep = Annotated[PostgresUnitOfWork, Depends(get_uow)]
