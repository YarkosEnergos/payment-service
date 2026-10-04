from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from src.infra.postgres.models.outbox import OutboxMessage
from src.infra.postgres.models.payments import Payment
from src.infra.postgres.uow import UnitOfWorkFactory
from src.modules.payments.enums import Currency, PaymentStatus


def make_payment(
    *,
    status: PaymentStatus = PaymentStatus.PENDING,
    idempotency_key: str | None = None,
    webhook_url: str = "http://client.test/webhooks",
) -> Payment:
    return Payment(
        id=uuid4(),
        amount=Decimal("100.00"),
        currency=Currency.RUB,
        description="Order #1",
        meta={"order_id": 1},
        status=status,
        idempotency_key=idempotency_key or str(uuid4()),
        request_hash="hash",
        webhook_url=webhook_url,
        created_at=datetime.now(UTC),
        processed_at=None if status is PaymentStatus.PENDING else datetime.now(UTC),
    )


class FakePaymentStorage:
    def __init__(self, payments: Sequence[Payment] = ()) -> None:
        self.payments: dict[UUID, Payment] = {p.id: p for p in payments}

    async def get_by_id(self, payment_id: UUID) -> Payment | None:
        return self.payments.get(payment_id)

    async def get_by_idempotency_key(self, idempotency_key: str) -> Payment | None:
        return next(
            (p for p in self.payments.values() if p.idempotency_key == idempotency_key),
            None,
        )

    async def create(
        self,
        *,
        amount: Decimal,
        currency: Currency,
        description: str,
        meta: dict[str, Any],
        idempotency_key: str,
        request_hash: str,
        webhook_url: str,
    ) -> Payment | None:
        if await self.get_by_idempotency_key(idempotency_key) is not None:
            return None
        payment = Payment(
            id=uuid4(),
            amount=amount,
            currency=currency,
            description=description,
            meta=meta,
            status=PaymentStatus.PENDING,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            webhook_url=webhook_url,
            created_at=datetime.now(UTC),
            processed_at=None,
        )
        self.payments[payment.id] = payment
        return payment

    async def complete(self, payment_id: UUID, status: PaymentStatus) -> Payment | None:
        payment = self.payments.get(payment_id)
        if payment is None or payment.status is not PaymentStatus.PENDING:
            return None
        payment.status = status
        payment.processed_at = datetime.now(UTC)
        return payment


class FakeOutboxStorage:
    def __init__(self, messages: Sequence[OutboxMessage] = ()) -> None:
        self.messages: list[OutboxMessage] = list(messages)

    async def add(self, routing_key: str, payload: dict[str, Any]) -> OutboxMessage:
        message = OutboxMessage(
            id=uuid4(),
            routing_key=routing_key,
            payload=payload,
            created_at=datetime.now(UTC),
            published_at=None,
        )
        self.messages.append(message)
        return message

    async def lock_unpublished(self, limit: int) -> list[OutboxMessage]:
        return [m for m in self.messages if m.published_at is None][:limit]

    async def mark_published(self, message_ids: Sequence[UUID]) -> None:
        for message in self.messages:
            if message.id in message_ids:
                message.published_at = datetime.now(UTC)


class FakeUnitOfWork:
    def __init__(
        self,
        payment: FakePaymentStorage | None = None,
        outbox: FakeOutboxStorage | None = None,
    ) -> None:
        self.payment = payment or FakePaymentStorage()
        self.outbox = outbox or FakeOutboxStorage()
        self.commits = 0

    async def commit(self) -> None:
        self.commits += 1

    async def rollback(self) -> None:
        return None


def make_uow_factory(uow: FakeUnitOfWork) -> UnitOfWorkFactory:
    @asynccontextmanager
    async def factory() -> AsyncIterator[Any]:
        yield uow

    return factory
