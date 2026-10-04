from collections.abc import Sequence
from typing import Any
from uuid import UUID

from sqlalchemy import func, select, update

from src.infra.postgres.models.outbox import OutboxMessage
from src.infra.postgres.storage.base_storage import BaseStorage


class OutboxStorage(BaseStorage):
    async def add(self, routing_key: str, payload: dict[str, Any]) -> OutboxMessage:
        message = OutboxMessage(routing_key=routing_key, payload=payload)
        self.db.add(message)
        await self.db.flush()
        return message

    async def lock_unpublished(self, limit: int) -> list[OutboxMessage]:
        command = (
            select(OutboxMessage)
            .where(OutboxMessage.published_at.is_(None))
            .order_by(OutboxMessage.created_at, OutboxMessage.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return list(await self.db.scalars(command))

    async def mark_published(self, message_ids: Sequence[UUID]) -> None:
        if not message_ids:
            return
        command = (
            update(OutboxMessage)
            .where(OutboxMessage.id.in_(message_ids))
            .values(published_at=func.now())
        )
        await self.db.execute(command)
