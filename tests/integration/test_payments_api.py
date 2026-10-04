import asyncio
from typing import Any

from fastapi import status
from httpx import AsyncClient
from sqlalchemy import func, select

from src.infra.postgres.models.outbox import OutboxMessage
from src.infra.postgres.models.payments import Payment
from src.infra.postgres.uow import UnitOfWorkFactory
from src.infra.rabbitmq.topology import PAYMENTS_NEW_ROUTING_KEY

PAYMENTS_URL = "/api/v1/payments"
BODY: dict[str, Any] = {
    "amount": "990.10",
    "currency": "RUB",
    "description": "Subscription",
    "metadata": {"user_id": 7},
    "webhook_url": "http://client.test/webhooks",
}


async def count_rows(uow_factory: UnitOfWorkFactory) -> tuple[int, int]:
    async with uow_factory() as uow:
        payments = await uow.db.scalar(select(func.count()).select_from(Payment))
        outbox = await uow.db.scalar(select(func.count()).select_from(OutboxMessage))
    return payments or 0, outbox or 0


async def test_create_payment_persists_payment_and_outbox_event(
    client: AsyncClient,
    uow_factory: UnitOfWorkFactory,
) -> None:
    response = await client.post(
        PAYMENTS_URL,
        json=BODY,
        headers={"Idempotency-Key": "key-1"},
    )

    assert response.status_code == status.HTTP_202_ACCEPTED
    payment_id = response.json()["payment_id"]
    async with uow_factory() as uow:
        [message] = await uow.outbox.lock_unpublished(limit=10)
    assert message.routing_key == PAYMENTS_NEW_ROUTING_KEY
    assert message.payload == {"payment_id": payment_id}
    assert message.published_at is None


async def test_get_payment_returns_stored_details(client: AsyncClient) -> None:
    created = await client.post(
        PAYMENTS_URL,
        json=BODY,
        headers={"Idempotency-Key": "key-1"},
    )
    payment_id = created.json()["payment_id"]

    response = await client.get(f"{PAYMENTS_URL}/{payment_id}")

    assert response.status_code == status.HTTP_200_OK
    assert response.json() | {"created_at": None} == {
        "payment_id": payment_id,
        "amount": "990.10",
        "currency": "RUB",
        "description": "Subscription",
        "metadata": {"user_id": 7},
        "status": "pending",
        "idempotency_key": "key-1",
        "webhook_url": "http://client.test/webhooks",
        "created_at": None,
        "processed_at": None,
    }


async def test_concurrent_requests_with_same_key_create_one_payment(
    client: AsyncClient,
    uow_factory: UnitOfWorkFactory,
) -> None:
    responses = await asyncio.gather(
        *(
            client.post(PAYMENTS_URL, json=BODY, headers={"Idempotency-Key": "race"})
            for _ in range(10)
        ),
    )

    assert {response.status_code for response in responses} == {
        status.HTTP_202_ACCEPTED,
    }
    assert len({response.json()["payment_id"] for response in responses}) == 1
    assert await count_rows(uow_factory) == (1, 1)


async def test_reused_key_with_different_body_is_rejected(
    client: AsyncClient,
    uow_factory: UnitOfWorkFactory,
) -> None:
    headers = {"Idempotency-Key": "key-1"}
    await client.post(PAYMENTS_URL, json=BODY, headers=headers)

    response = await client.post(
        PAYMENTS_URL,
        json={**BODY, "currency": "USD"},
        headers=headers,
    )

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    assert await count_rows(uow_factory) == (1, 1)
