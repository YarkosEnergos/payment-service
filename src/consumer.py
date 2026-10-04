from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from faststream import ContextRepo, FastStream
from faststream.rabbit import Channel

from src.app_config import app_config
from src.infra.gateway.emulator import PaymentGatewayEmulator
from src.infra.postgres.pg import engine
from src.infra.postgres.uow import unit_of_work
from src.infra.rabbitmq.broker import create_broker, retry_policy
from src.infra.rabbitmq.topology import declare_topology
from src.infra.webhooks.client import WebhookClient
from src.logging_config import setup_logging
from src.modules.payments.consumer import router as payments_router
from src.modules.payments.processor import PaymentProcessor

setup_logging()

broker = create_broker(Channel(prefetch_count=app_config.consumer.prefetch_count))
broker.include_router(payments_router)


@asynccontextmanager
async def lifespan(context: ContextRepo) -> AsyncIterator[None]:
    await broker.connect()
    await declare_topology(broker, retry_policy)

    async with httpx.AsyncClient(
        timeout=app_config.consumer.webhook_timeout_seconds,
    ) as http:
        gateway = PaymentGatewayEmulator(
            min_delay_seconds=app_config.gateway.min_delay_seconds,
            max_delay_seconds=app_config.gateway.max_delay_seconds,
            success_rate=app_config.gateway.success_rate,
        )
        processor = PaymentProcessor(
            uow_factory=unit_of_work,
            gateway=gateway,
            webhook_sender=WebhookClient(http),
        )
        context.set_global("processor", processor)
        yield

    await engine.dispose()


app = FastStream(broker, lifespan=lifespan)
