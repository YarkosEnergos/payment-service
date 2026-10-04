import json

import httpx
import pytest

from src.infra.webhooks.client import WebhookClient, WebhookDeliveryError
from src.modules.payments.schemas import PaymentResponse
from tests.unit.fakes import make_payment

URL = "http://client.test/webhooks"


def make_client(handler: httpx.MockTransport) -> WebhookClient:
    return WebhookClient(httpx.AsyncClient(transport=handler))


async def test_send_posts_payload_as_json() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200)

    payment = make_payment()

    await make_client(httpx.MockTransport(handler)).send(
        URL,
        PaymentResponse.from_model(payment),
    )

    [request] = requests
    assert request.method == "POST"
    assert str(request.url) == URL
    assert request.headers["content-type"] == "application/json"
    body = json.loads(request.content)
    assert body["payment_id"] == str(payment.id)
    assert body["amount"] == "100.00"


@pytest.mark.parametrize("status_code", [301, 400, 404, 500, 503])
async def test_non_2xx_response_raises(status_code: int) -> None:
    client = make_client(httpx.MockTransport(lambda _: httpx.Response(status_code)))

    with pytest.raises(WebhookDeliveryError):
        await client.send(URL, PaymentResponse.from_model(make_payment()))


async def test_network_error_raises() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        msg = "connection refused"
        raise httpx.ConnectError(msg, request=request)

    client = make_client(httpx.MockTransport(handler))

    with pytest.raises(WebhookDeliveryError, match="connection refused"):
        await client.send(URL, PaymentResponse.from_model(make_payment()))
