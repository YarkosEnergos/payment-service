from faststream.rabbit import Channel, RabbitBroker

from src.app_config import app_config
from src.infra.rabbitmq.topology import RetryPolicy

retry_policy = RetryPolicy(
    max_attempts=app_config.consumer.max_attempts,
    base_delay_seconds=app_config.consumer.retry_base_delay_seconds,
)


def create_broker(channel: Channel | None = None) -> RabbitBroker:
    return RabbitBroker(
        app_config.rabbitmq.url.get_secret_value(),
        default_channel=channel,
    )
