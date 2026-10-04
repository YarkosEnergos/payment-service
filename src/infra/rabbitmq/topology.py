from dataclasses import dataclass

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

PAYMENTS_EXCHANGE = "payments"
PAYMENTS_RETRY_EXCHANGE = "payments.retry"
PAYMENTS_DEAD_LETTER_EXCHANGE = "payments.dlx"

PAYMENTS_NEW_QUEUE = "payments.new"
PAYMENTS_NEW_ROUTING_KEY = PAYMENTS_NEW_QUEUE
PAYMENTS_DEAD_LETTER_QUEUE = "payments.new.dlq"

ATTEMPT_HEADER = "x-attempt"

payments_exchange = RabbitExchange(PAYMENTS_EXCHANGE, type=ExchangeType.DIRECT)
retry_exchange = RabbitExchange(PAYMENTS_RETRY_EXCHANGE, type=ExchangeType.DIRECT)
dead_letter_exchange = RabbitExchange(
    PAYMENTS_DEAD_LETTER_EXCHANGE,
    type=ExchangeType.DIRECT,
)

payments_new_queue = RabbitQueue(
    PAYMENTS_NEW_QUEUE,
    durable=True,
    routing_key=PAYMENTS_NEW_ROUTING_KEY,
    arguments={
        "x-dead-letter-exchange": PAYMENTS_DEAD_LETTER_EXCHANGE,
        "x-dead-letter-routing-key": PAYMENTS_DEAD_LETTER_QUEUE,
    },
)

dead_letter_queue = RabbitQueue(
    PAYMENTS_DEAD_LETTER_QUEUE,
    durable=True,
    routing_key=PAYMENTS_DEAD_LETTER_QUEUE,
)


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int
    base_delay_seconds: float

    def can_retry(self, attempt: int) -> bool:
        return attempt < self.max_attempts

    def delay_ms(self, attempt: int) -> int:
        return round(self.base_delay_seconds * 2 ** (attempt - 1) * 1000)

    def retry_queue(self, attempt: int) -> RabbitQueue:
        delay_ms = self.delay_ms(attempt)
        name = f"{PAYMENTS_NEW_QUEUE}.retry.{delay_ms}ms"
        return RabbitQueue(
            name,
            durable=True,
            routing_key=name,
            arguments={
                "x-message-ttl": delay_ms,
                "x-dead-letter-exchange": PAYMENTS_EXCHANGE,
                "x-dead-letter-routing-key": PAYMENTS_NEW_ROUTING_KEY,
            },
        )

    def retry_queues(self) -> list[RabbitQueue]:
        return [self.retry_queue(attempt) for attempt in range(1, self.max_attempts)]


async def declare_topology(broker: RabbitBroker, retry_policy: RetryPolicy) -> None:
    payments = await broker.declare_exchange(payments_exchange)
    retry = await broker.declare_exchange(retry_exchange)
    dead_letter = await broker.declare_exchange(dead_letter_exchange)

    main_queue = await broker.declare_queue(payments_new_queue)
    await main_queue.bind(payments, routing_key=payments_new_queue.routing_key)

    dlq = await broker.declare_queue(dead_letter_queue)
    await dlq.bind(dead_letter, routing_key=dead_letter_queue.routing_key)

    for queue in retry_policy.retry_queues():
        declared = await broker.declare_queue(queue)
        await declared.bind(retry, routing_key=queue.routing_key)
