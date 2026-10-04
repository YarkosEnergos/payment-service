from faststream.rabbit import RabbitBroker

from src.infra.postgres.models.outbox import OutboxMessage
from src.infra.rabbitmq.topology import payments_exchange


class RabbitOutboxPublisher:
    def __init__(self, broker: RabbitBroker) -> None:
        self.broker = broker

    async def publish(self, message: OutboxMessage) -> None:
        await self.broker.publish(
            message.payload,
            exchange=payments_exchange,
            routing_key=message.routing_key,
            message_id=str(message.id),
            timestamp=message.created_at,
            persist=True,
            mandatory=True,
        )
