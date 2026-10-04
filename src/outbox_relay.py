import asyncio
import signal
from contextlib import suppress

from faststream.rabbit import Channel

from src.app_config import app_config
from src.infra.postgres.pg import engine
from src.infra.postgres.uow import unit_of_work
from src.infra.rabbitmq.broker import create_broker, retry_policy
from src.infra.rabbitmq.publisher import RabbitOutboxPublisher
from src.infra.rabbitmq.topology import declare_topology
from src.logging_config import setup_logging
from src.modules.outbox.relay import OutboxRelay


async def main() -> None:
    setup_logging()

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop_event.set)

    broker = create_broker(Channel(on_return_raises=True))
    await broker.connect()
    try:
        await declare_topology(broker, retry_policy)
        relay = OutboxRelay(
            uow_factory=unit_of_work,
            publisher=RabbitOutboxPublisher(broker),
            batch_size=app_config.outbox.batch_size,
        )
        await relay.run(stop_event, app_config.outbox.poll_interval_seconds)
    finally:
        await broker.stop()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
