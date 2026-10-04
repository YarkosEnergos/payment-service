from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID, uuid4

import pytest
from faststream.rabbit import RabbitBroker, TestRabbitBroker
from faststream.rabbit.annotations import RabbitMessage
from pydantic import ValidationError

from src.infra.rabbitmq.broker import retry_policy
from src.infra.rabbitmq.topology import (
    ATTEMPT_HEADER,
    PAYMENTS_NEW_ROUTING_KEY,
    payments_exchange,
    retry_exchange,
)
from src.modules.payments.consumer import router
from src.modules.payments.exceptions import PaymentNotFoundError
from src.modules.payments.schemas import PaymentCreatedEvent


class FakeProcessor:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.processed: list[UUID] = []

    async def process(self, payment_id: UUID) -> None:
        self.processed.append(payment_id)
        if self.error is not None:
            raise self.error


@dataclass
class Retried:
    queue: str
    event: PaymentCreatedEvent
    headers: dict[str, Any]
    message_id: str | None


@dataclass
class ConsumerHarness:
    broker: RabbitBroker
    retried: list[Retried] = field(default_factory=list)

    def use_processor(self, processor: FakeProcessor) -> None:
        self.broker.context.set_global("processor", processor)

    async def publish(
        self,
        body: object,
        attempt: int | None = None,
        message_id: str = "message-1",
    ) -> None:
        headers = {} if attempt is None else {ATTEMPT_HEADER: attempt}
        await self.broker.publish(
            body,
            exchange=payments_exchange,
            routing_key=PAYMENTS_NEW_ROUTING_KEY,
            headers=headers,
            message_id=message_id,
        )


@pytest.fixture
async def harness() -> AsyncIterator[ConsumerHarness]:
    broker = RabbitBroker()
    broker.include_router(router)
    harness = ConsumerHarness(broker)

    def capture_into(queue_name: str) -> Callable[..., Awaitable[None]]:
        async def capture(event: PaymentCreatedEvent, message: RabbitMessage) -> None:
            harness.retried.append(
                Retried(queue_name, event, dict(message.headers), message.message_id),
            )

        return capture

    for retry_queue in retry_policy.retry_queues():
        broker.subscriber(retry_queue, retry_exchange)(capture_into(retry_queue.name))

    async with TestRabbitBroker(broker):
        yield harness


async def test_successful_processing_is_not_retried(harness: ConsumerHarness) -> None:
    processor = FakeProcessor()
    harness.use_processor(processor)
    payment_id = uuid4()

    await harness.publish(PaymentCreatedEvent(payment_id=payment_id))

    assert processor.processed == [payment_id]
    assert harness.retried == []


@pytest.mark.parametrize(
    ("attempt", "expected_queue"),
    [(None, "payments.new.retry.2000ms"), (2, "payments.new.retry.4000ms")],
)
async def test_failure_is_scheduled_for_delayed_retry(
    harness: ConsumerHarness,
    attempt: int | None,
    expected_queue: str,
) -> None:
    harness.use_processor(FakeProcessor(error=ConnectionError("receiver is down")))
    event = PaymentCreatedEvent(payment_id=uuid4())

    await harness.publish(event, attempt=attempt)

    [retried] = harness.retried
    assert retried.queue == expected_queue
    assert retried.event == event
    assert retried.headers[ATTEMPT_HEADER] == (attempt or 1) + 1
    assert retried.message_id == "message-1"


async def test_last_attempt_failure_is_rejected(harness: ConsumerHarness) -> None:
    harness.use_processor(FakeProcessor(error=ConnectionError("receiver is down")))

    with pytest.raises(ConnectionError):
        await harness.publish(
            PaymentCreatedEvent(payment_id=uuid4()),
            attempt=retry_policy.max_attempts,
        )

    assert harness.retried == []


async def test_unknown_payment_is_rejected_without_retry(
    harness: ConsumerHarness,
) -> None:
    payment_id = uuid4()
    harness.use_processor(FakeProcessor(error=PaymentNotFoundError(payment_id)))

    with pytest.raises(PaymentNotFoundError):
        await harness.publish(PaymentCreatedEvent(payment_id=payment_id))

    assert harness.retried == []


async def test_malformed_message_is_rejected_without_retry(
    harness: ConsumerHarness,
) -> None:
    processor = FakeProcessor()
    harness.use_processor(processor)

    with pytest.raises(ValidationError):
        await harness.publish({"payment_id": "not-a-uuid"})

    assert processor.processed == []
    assert harness.retried == []
