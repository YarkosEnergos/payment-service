import logging
from collections.abc import Mapping
from typing import Annotated, Any

from faststream import Context
from faststream.rabbit import RabbitRouter
from faststream.rabbit.annotations import RabbitBroker, RabbitMessage

from src.infra.rabbitmq.broker import retry_policy
from src.infra.rabbitmq.topology import (
    ATTEMPT_HEADER,
    payments_exchange,
    payments_new_queue,
    retry_exchange,
)
from src.modules.payments.exceptions import PaymentNotFoundError
from src.modules.payments.processor import PaymentProcessor
from src.modules.payments.schemas import PaymentCreatedEvent

logger = logging.getLogger(__name__)

router = RabbitRouter()


def get_attempt(headers: Mapping[str, Any]) -> int:
    try:
        return max(int(headers.get(ATTEMPT_HEADER, 1)), 1)
    except (TypeError, ValueError):
        return 1


async def schedule_retry(
    broker: RabbitBroker,
    event: PaymentCreatedEvent,
    message: RabbitMessage,
    attempt: int,
) -> None:
    queue = retry_policy.retry_queue(attempt)
    try:
        await broker.publish(
            event,
            exchange=retry_exchange,
            routing_key=queue.routing_key,
            headers={ATTEMPT_HEADER: attempt + 1},
            message_id=message.message_id,
            correlation_id=message.correlation_id,
            persist=True,
        )
    except Exception:
        logger.exception(
            "Payment %s: failed to schedule retry, requeueing message",
            event.payment_id,
        )
        await message.nack(requeue=True)
        return

    logger.warning(
        "Payment %s: attempt %d/%d failed, retrying in %d ms",
        event.payment_id,
        attempt,
        retry_policy.max_attempts,
        retry_policy.delay_ms(attempt),
    )


@router.subscriber(payments_new_queue, payments_exchange)
async def handle_payment_created(
    event: PaymentCreatedEvent,
    message: RabbitMessage,
    broker: RabbitBroker,
    processor: Annotated[PaymentProcessor, Context("processor")],
) -> None:
    attempt = get_attempt(message.headers)
    try:
        await processor.process(event.payment_id)
    except PaymentNotFoundError:
        logger.error("Payment %s not found, sending to DLQ", event.payment_id)
        raise
    except Exception:
        if not retry_policy.can_retry(attempt):
            logger.error(
                "Payment %s: attempt %d/%d failed, sending to DLQ",
                event.payment_id,
                attempt,
                retry_policy.max_attempts,
            )
            raise
        await schedule_retry(broker, event, message, attempt)
