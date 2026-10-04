from src.infra.rabbitmq.topology import (
    PAYMENTS_EXCHANGE,
    PAYMENTS_NEW_ROUTING_KEY,
    RetryPolicy,
)
from src.modules.payments.consumer import get_attempt


def test_delays_grow_exponentially() -> None:
    policy = RetryPolicy(max_attempts=4, base_delay_seconds=2)

    assert [policy.delay_ms(attempt) for attempt in (1, 2, 3)] == [2000, 4000, 8000]


def test_can_retry_until_last_attempt() -> None:
    policy = RetryPolicy(max_attempts=3, base_delay_seconds=1)

    assert [policy.can_retry(attempt) for attempt in (1, 2, 3)] == [True, True, False]


def test_one_retry_queue_per_retry() -> None:
    policy = RetryPolicy(max_attempts=3, base_delay_seconds=2)

    queues = policy.retry_queues()

    assert [queue.name for queue in queues] == [
        "payments.new.retry.2000ms",
        "payments.new.retry.4000ms",
    ]


def test_retry_queue_dead_letters_back_to_main_queue() -> None:
    queue = RetryPolicy(max_attempts=3, base_delay_seconds=0.5).retry_queue(1)

    assert queue.arguments is not None
    assert queue.arguments["x-message-ttl"] == 500
    assert queue.arguments["x-dead-letter-exchange"] == PAYMENTS_EXCHANGE
    assert queue.arguments["x-dead-letter-routing-key"] == PAYMENTS_NEW_ROUTING_KEY


def test_get_attempt() -> None:
    assert get_attempt({}) == 1
    assert get_attempt({"x-attempt": 3}) == 3
    assert get_attempt({"x-attempt": "2"}) == 2
    assert get_attempt({"x-attempt": "garbage"}) == 1
    assert get_attempt({"x-attempt": 0}) == 1
