from sqlalchemy.ext.asyncio import AsyncSession


class BaseStorage:
    def __init__(self, db: AsyncSession) -> None:
        self.db: AsyncSession = db
