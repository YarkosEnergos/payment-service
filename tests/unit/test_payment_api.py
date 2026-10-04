from collections.abc import AsyncIterator
from typing import Any
from uuid import uuid4

import pytest
from fastapi import status
from httpx import ASGITransport, AsyncClient

from src.infra.postgres.uow import get_uow
from src.main import app
from tests.unit.fakes import FakeUnitOfWork

API_KEY = "test-api-key"
PAYMENTS_URL = "/api/v1/payments"
BODY: dict[str, Any] = {
    "amount": "1500.5",
    "currency": "EUR",
    "description": "Order #42",
    "metadata": {"order_id": 42},
    "webhook_url": "http://client.test/webhooks",
}


@pytest.fixture
def uow() -> FakeUnitOfWork:
    return FakeUnitOfWork()


@pytest.fixture
async def client(uow: FakeUnitOfWork) -> AsyncIterator[AsyncClient]:
    app.dependency_overrides[get_uow] = lambda: uow
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"X-API-Key": API_KEY},
    ) as client:
        yield client
    app.dependency_overrides.clear()


@pytest.mark.parametrize("headers", [{}, {"X-API-Key": "wrong"}])
async def test_requests_without_valid_api_key_are_rejected(
    client: AsyncClient,
    headers: dict[str, str],
) -> None:
    client.headers.pop("X-API-Key")

    create = await client.post(
        PAYMENTS_URL,
        json=BODY,
        headers={"Idempotency-Key": "key-1", **headers},
    )
    get = await client.get(f"{PAYMENTS_URL}/{uuid4()}", headers=headers)

    assert create.status_code == status.HTTP_401_UNAUTHORIZED
    assert get.status_code == status.HTTP_401_UNAUTHORIZED


async def test_create_payment_returns_202(client: AsyncClient) -> None:
    response = await client.post(
        PAYMENTS_URL,
        json=BODY,
        headers={"Idempotency-Key": "key-1"},
    )

    assert response.status_code == status.HTTP_202_ACCEPTED
    body = response.json()
    assert set(body) == {"payment_id", "status", "created_at"}
    assert body["status"] == "pending"


async def test_create_payment_requires_idempotency_key(client: AsyncClient) -> None:
    response = await client.post(PAYMENTS_URL, json=BODY)

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT


async def test_create_payment_replay_returns_same_response(client: AsyncClient) -> None:
    headers = {"Idempotency-Key": "key-1"}
    first = await client.post(PAYMENTS_URL, json=BODY, headers=headers)

    second = await client.post(PAYMENTS_URL, json=BODY, headers=headers)

    assert second.status_code == status.HTTP_202_ACCEPTED
    assert second.json() == first.json()


async def test_create_payment_reused_key_with_other_body_returns_422(
    client: AsyncClient,
) -> None:
    headers = {"Idempotency-Key": "key-1"}
    await client.post(PAYMENTS_URL, json=BODY, headers=headers)

    response = await client.post(
        PAYMENTS_URL,
        json={**BODY, "amount": "1.00"},
        headers=headers,
    )

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    assert "key-1" in response.json()["detail"]


async def test_get_payment(client: AsyncClient) -> None:
    created = await client.post(
        PAYMENTS_URL,
        json=BODY,
        headers={"Idempotency-Key": "key-1"},
    )
    payment_id = created.json()["payment_id"]

    response = await client.get(f"{PAYMENTS_URL}/{payment_id}")

    assert response.status_code == status.HTTP_200_OK
    body = response.json()
    assert body["payment_id"] == payment_id
    assert body["amount"] == "1500.50"
    assert body["currency"] == "EUR"
    assert body["metadata"] == {"order_id": 42}
    assert body["status"] == "pending"
    assert body["idempotency_key"] == "key-1"
    assert body["processed_at"] is None


async def test_get_unknown_payment_returns_404(client: AsyncClient) -> None:
    response = await client.get(f"{PAYMENTS_URL}/{uuid4()}")

    assert response.status_code == status.HTTP_404_NOT_FOUND
