import asyncio

from src.infra.postgres.models.outbox import OutboxMessage
from src.modules.outbox.relay import OutboxRelay
from tests.unit.fakes import FakeOutboxStorage, FakeUnitOfWork, make_uow_factory


class FakePublisher:
    def __init__(self, fail_on_call: int | None = None) -> None:
        self.published: list[OutboxMessage] = []
        self.fail_on_call = fail_on_call
        self.calls = 0

    async def publish(self, message: OutboxMessage) -> None:
        self.calls += 1
        if self.calls == self.fail_on_call:
            msg = "broker is unavailable"
            raise ConnectionError(msg)
        self.published.append(message)


async def make_uow_with_messages(count: int) -> FakeUnitOfWork:
    outbox = FakeOutboxStorage()
    for number in range(count):
        await outbox.add("payments.new", {"number": number})
    return FakeUnitOfWork(outbox=outbox)


async def test_publish_batch_publishes_and_marks_messages() -> None:
    uow = await make_uow_with_messages(3)
    publisher = FakePublisher()
    relay = OutboxRelay(make_uow_factory(uow), publisher, batch_size=10)

    published = await relay.publish_batch()

    assert published == 3
    assert [m.payload["number"] for m in publisher.published] == [0, 1, 2]
    assert all(m.published_at is not None for m in uow.outbox.messages)
    assert uow.commits == 1


async def test_publish_batch_respects_batch_size() -> None:
    uow = await make_uow_with_messages(3)
    relay = OutboxRelay(make_uow_factory(uow), FakePublisher(), batch_size=2)

    assert await relay.publish_batch() == 2
    assert await relay.publish_batch() == 1
    assert await relay.publish_batch() == 0


async def test_publish_batch_stops_on_first_failure() -> None:
    uow = await make_uow_with_messages(3)
    publisher = FakePublisher(fail_on_call=2)
    relay = OutboxRelay(make_uow_factory(uow), publisher, batch_size=10)

    published = await relay.publish_batch()

    assert published == 1
    assert [m.published_at is not None for m in uow.outbox.messages] == [
        True,
        False,
        False,
    ]
    assert uow.commits == 1


async def test_failed_messages_are_published_on_next_batch() -> None:
    uow = await make_uow_with_messages(2)
    publisher = FakePublisher(fail_on_call=1)
    relay = OutboxRelay(make_uow_factory(uow), publisher, batch_size=10)

    assert await relay.publish_batch() == 0
    assert await relay.publish_batch() == 2
    assert all(m.published_at is not None for m in uow.outbox.messages)


async def test_run_drains_outbox_and_stops() -> None:
    uow = await make_uow_with_messages(5)
    publisher = FakePublisher()
    relay = OutboxRelay(make_uow_factory(uow), publisher, batch_size=2)
    stop_event = asyncio.Event()

    task = asyncio.create_task(relay.run(stop_event, poll_interval=0.01))
    await asyncio.sleep(0.05)
    stop_event.set()
    await asyncio.wait_for(task, timeout=1)

    assert len(publisher.published) == 5
