import asyncio
import logging
from contextlib import suppress
from typing import TYPE_CHECKING, Protocol

from src.infra.postgres.models.outbox import OutboxMessage
from src.infra.postgres.uow import UnitOfWorkFactory

if TYPE_CHECKING:
    from uuid import UUID

logger = logging.getLogger(__name__)


class OutboxPublisher(Protocol):
    async def publish(self, message: OutboxMessage) -> None: ...


class OutboxRelay:
    def __init__(
        self,
        uow_factory: UnitOfWorkFactory,
        publisher: OutboxPublisher,
        batch_size: int,
    ) -> None:
        self.uow_factory = uow_factory
        self.publisher = publisher
        self.batch_size = batch_size

    async def publish_batch(self) -> int:
        async with self.uow_factory() as uow:
            messages = await uow.outbox.lock_unpublished(self.batch_size)
            published: list[UUID] = []
            for message in messages:
                try:
                    await self.publisher.publish(message)
                except Exception as error:
                    logger.warning(
                        "Failed to publish outbox message %s, will retry: %r",
                        message.id,
                        error,
                    )
                    break
                published.append(message.id)

            await uow.outbox.mark_published(published)
            await uow.commit()

        if published:
            logger.info("Published %d outbox message(s)", len(published))
        return len(published)

    async def run(self, stop_event: asyncio.Event, poll_interval: float) -> None:
        logger.info("Outbox relay started")
        while not stop_event.is_set():
            try:
                published = await self.publish_batch()
            except Exception:
                logger.exception("Outbox relay iteration failed")
                published = 0

            if published < self.batch_size:
                with suppress(TimeoutError):
                    await asyncio.wait_for(stop_event.wait(), timeout=poll_interval)
        logger.info("Outbox relay stopped")
