import asyncio
from collections import Counter

from src.infra.postgres.models.outbox import OutboxMessage
from src.infra.postgres.uow import UnitOfWorkFactory
from src.modules.outbox.relay import OutboxRelay


class SlowPublisher:
    def __init__(self, published: Counter[str], fail: bool = False) -> None:
        self.published = published
        self.fail = fail

    async def publish(self, message: OutboxMessage) -> None:
        await asyncio.sleep(0.01)
        if self.fail:
            msg = "broker is unavailable"
            raise ConnectionError(msg)
        self.published[str(message.id)] += 1


async def add_messages(uow_factory: UnitOfWorkFactory, count: int) -> None:
    async with uow_factory() as uow:
        for number in range(count):
            await uow.outbox.add("payments.new", {"number": number})
        await uow.commit()


async def unpublished_count(uow_factory: UnitOfWorkFactory) -> int:
    async with uow_factory() as uow:
        return len(await uow.outbox.lock_unpublished(limit=1000))


async def test_concurrent_relays_publish_each_message_once(
    uow_factory: UnitOfWorkFactory,
) -> None:
    await add_messages(uow_factory, 30)
    published: Counter[str] = Counter()
    relays = [
        OutboxRelay(uow_factory, SlowPublisher(published), batch_size=30)
        for _ in range(3)
    ]

    results = await asyncio.gather(*(relay.publish_batch() for relay in relays))

    assert sum(results) == 30
    assert len(published) == 30
    assert set(published.values()) == {1}
    assert await unpublished_count(uow_factory) == 0


async def test_failed_publish_keeps_messages_in_outbox(
    uow_factory: UnitOfWorkFactory,
) -> None:
    await add_messages(uow_factory, 3)
    published: Counter[str] = Counter()

    failing = OutboxRelay(uow_factory, SlowPublisher(published, fail=True), 10)
    assert await failing.publish_batch() == 0
    assert await unpublished_count(uow_factory) == 3

    healthy = OutboxRelay(uow_factory, SlowPublisher(published), 10)
    assert await healthy.publish_batch() == 3
    assert await unpublished_count(uow_factory) == 0
