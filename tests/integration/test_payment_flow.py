import asyncio
import itertools
import json
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any

import httpx
import pytest
from aio_pika.abc import AbstractIncomingMessage
from faststream.rabbit import Channel, RabbitBroker

from src.infra.gateway.emulator import PaymentGatewayEmulator
from src.infra.postgres.models.payments import Payment
from src.infra.postgres.uow import UnitOfWorkFactory
from src.infra.rabbitmq.broker import retry_policy
from src.infra.rabbitmq.publisher import RabbitOutboxPublisher
from src.infra.rabbitmq.topology import (
    ATTEMPT_HEADER,
    dead_letter_queue,
    declare_topology,
    payments_new_queue,
)
from src.infra.webhooks.client import WebhookClient
from src.modules.outbox.relay import OutboxRelay
from src.modules.payments.consumer import router
from src.modules.payments.enums import PaymentStatus
from src.modules.payments.processor import PaymentProcessor
from tests.integration.conftest import wait_for


@dataclass
class WebhookReceiver:
    failures_before_success: int = 0
    calls: list[float] = field(default_factory=list)
    delivered: list[dict[str, Any]] = field(default_factory=list)

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.calls.append(time.monotonic())
        if len(self.calls) <= self.failures_before_success:
            return httpx.Response(503)
        self.delivered.append(json.loads(request.content))
        return httpx.Response(200)


class CountingGateway(PaymentGatewayEmulator):
    def __init__(self) -> None:
        super().__init__(min_delay_seconds=0, max_delay_seconds=0, success_rate=1)
        self.calls = 0

    async def charge(self, payment: Payment) -> PaymentStatus:
        self.calls += 1
        return await super().charge(payment)


@dataclass
class PaymentFlow:
    broker: RabbitBroker
    relay: OutboxRelay
    webhooks: WebhookReceiver
    gateway: CountingGateway

    async def wait_for_dead_letter(
        self,
        within: float = 10.0,
    ) -> AbstractIncomingMessage:
        queue = await self.broker.declare_queue(dead_letter_queue)
        for _ in range(int(within / 0.1)):
            message = await queue.get(no_ack=True, fail=False)
            if message is not None:
                return message
            await asyncio.sleep(0.1)
        msg = "No message was dead-lettered"
        raise TimeoutError(msg)

    async def dead_letter_count(self) -> int:
        queue = await self.broker.declare_queue(dead_letter_queue)
        declared = await queue.declare()
        return declared.message_count or 0


@pytest.fixture
async def flow(
    rabbitmq_url: str,
    uow_factory: UnitOfWorkFactory,
) -> AsyncIterator[PaymentFlow]:
    webhooks = WebhookReceiver()
    gateway = CountingGateway()
    broker = RabbitBroker(rabbitmq_url, default_channel=Channel(prefetch_count=10))
    broker.include_router(router)

    async with httpx.AsyncClient(transport=httpx.MockTransport(webhooks.handle)) as http:
        processor = PaymentProcessor(uow_factory, gateway, WebhookClient(http))
        broker.context.set_global("processor", processor)

        await broker.connect()
        await declare_topology(broker, retry_policy)
        queues = [payments_new_queue, dead_letter_queue, *retry_policy.retry_queues()]
        for queue in queues:
            declared = await broker.declare_queue(queue)
            await declared.purge()
        await broker.start()

        relay = OutboxRelay(uow_factory, RabbitOutboxPublisher(broker), batch_size=100)
        yield PaymentFlow(broker, relay, webhooks, gateway)

        await broker.stop()


async def create_payment(client: httpx.AsyncClient, idempotency_key: str) -> str:
    response = await client.post(
        "/api/v1/payments",
        json={
            "amount": "42.00",
            "currency": "EUR",
            "description": "Integration test",
            "webhook_url": "http://client.test/webhooks",
        },
        headers={"Idempotency-Key": idempotency_key},
    )
    return response.json()["payment_id"]


async def get_payment(client: httpx.AsyncClient, payment_id: str) -> dict[str, Any]:
    response = await client.get(f"/api/v1/payments/{payment_id}")
    return response.json()


async def test_payment_is_processed_and_webhook_delivered(
    client: httpx.AsyncClient,
    flow: PaymentFlow,
) -> None:
    payment_id = await create_payment(client, "flow-success")

    assert await flow.relay.publish_batch() == 1
    await wait_for(lambda: len(flow.webhooks.delivered) == 1)

    [webhook] = flow.webhooks.delivered
    assert webhook["payment_id"] == payment_id
    assert webhook["status"] == "succeeded"
    payment = await get_payment(client, payment_id)
    assert payment["status"] == "succeeded"
    assert payment["processed_at"] is not None


async def test_webhook_is_retried_with_exponential_backoff(
    client: httpx.AsyncClient,
    flow: PaymentFlow,
) -> None:
    flow.webhooks.failures_before_success = retry_policy.max_attempts - 1
    payment_id = await create_payment(client, "flow-retry")

    await flow.relay.publish_batch()
    await wait_for(lambda: len(flow.webhooks.delivered) == 1)

    calls = flow.webhooks.calls
    gaps = [later - earlier for earlier, later in itertools.pairwise(calls)]
    delays = [
        retry_policy.delay_ms(attempt) / 1000
        for attempt in range(1, retry_policy.max_attempts)
    ]
    assert len(calls) == retry_policy.max_attempts
    assert all(gap >= delay * 0.9 for gap, delay in zip(gaps, delays, strict=True))
    assert gaps == sorted(gaps)
    assert flow.gateway.calls == 1
    assert flow.webhooks.delivered[0]["payment_id"] == payment_id
    assert await flow.dead_letter_count() == 0


async def test_message_is_dead_lettered_after_max_attempts(
    client: httpx.AsyncClient,
    flow: PaymentFlow,
) -> None:
    flow.webhooks.failures_before_success = 1_000
    payment_id = await create_payment(client, "flow-dlq")

    await flow.relay.publish_batch()
    dead_letter = await flow.wait_for_dead_letter()

    assert json.loads(dead_letter.body) == {"payment_id": payment_id}
    assert dead_letter.headers[ATTEMPT_HEADER] == retry_policy.max_attempts
    assert dead_letter.headers["x-last-death-reason"] == "rejected"
    assert len(flow.webhooks.calls) == retry_policy.max_attempts
    assert flow.gateway.calls == 1
    payment = await get_payment(client, payment_id)
    assert payment["status"] == "succeeded"
