from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert

from src.infra.postgres.models.payments import Payment
from src.infra.postgres.storage.base_storage import BaseStorage
from src.modules.payments.enums import Currency, PaymentStatus


class PaymentStorage(BaseStorage):
    async def get_by_id(self, payment_id: UUID) -> Payment | None:
        return await self.db.get(Payment, payment_id)

    async def get_by_idempotency_key(self, idempotency_key: str) -> Payment | None:
        command = select(Payment).where(Payment.idempotency_key == idempotency_key)
        return await self.db.scalar(command)

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
        command = (
            insert(Payment)
            .values(
                amount=amount,
                currency=currency,
                description=description,
                meta=meta,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
                webhook_url=webhook_url,
            )
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment)
        )
        return await self.db.scalar(command)

    async def complete(self, payment_id: UUID, status: PaymentStatus) -> Payment | None:
        command = (
            update(Payment)
            .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING)
            .values(status=status, processed_at=func.now())
            .returning(Payment)
        )
        return await self.db.scalar(command)
