from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from src.modules.payments.schemas import PaymentCreateRequest


def make_data(**overrides: object) -> dict[str, Any]:
    data: dict[str, Any] = {
        "amount": "100.00",
        "currency": "USD",
        "description": "Order #1",
        "metadata": {"order_id": 1},
        "webhook_url": "https://client.test/webhooks",
    }
    data.update(overrides)
    return data


def test_valid_request() -> None:
    request = PaymentCreateRequest.model_validate(make_data())

    assert request.amount == Decimal("100.00")
    assert str(request.webhook_url) == "https://client.test/webhooks"


def test_metadata_is_optional() -> None:
    data = make_data()
    del data["metadata"]

    request = PaymentCreateRequest.model_validate(data)

    assert request.metadata == {}


def test_amount_is_normalized_to_two_decimal_places() -> None:
    request = PaymentCreateRequest.model_validate(make_data(amount="7.5"))

    assert str(request.amount) == "7.50"


@pytest.mark.parametrize("amount", ["0", "-1.00", "1.001", "NaN", "1e20"])
def test_invalid_amount(amount: str) -> None:
    with pytest.raises(ValidationError):
        PaymentCreateRequest.model_validate(make_data(amount=amount))


def test_unknown_currency() -> None:
    with pytest.raises(ValidationError):
        PaymentCreateRequest.model_validate(make_data(currency="GBP"))


@pytest.mark.parametrize("url", ["ftp://client.test/hook", "not-a-url", "http://"])
def test_invalid_webhook_url(url: str) -> None:
    with pytest.raises(ValidationError):
        PaymentCreateRequest.model_validate(make_data(webhook_url=url))


def test_empty_description() -> None:
    with pytest.raises(ValidationError):
        PaymentCreateRequest.model_validate(make_data(description=""))


def test_extra_fields_are_forbidden() -> None:
    with pytest.raises(ValidationError):
        PaymentCreateRequest.model_validate(make_data(status="succeeded"))


def test_fingerprint_does_not_depend_on_metadata_key_order() -> None:
    first = PaymentCreateRequest.model_validate(make_data(metadata={"a": 1, "b": 2}))
    second = PaymentCreateRequest.model_validate(make_data(metadata={"b": 2, "a": 1}))

    assert first.fingerprint() == second.fingerprint()


def test_fingerprint_changes_with_body() -> None:
    first = PaymentCreateRequest.model_validate(make_data())
    second = PaymentCreateRequest.model_validate(make_data(description="Order #2"))

    assert first.fingerprint() != second.fingerprint()
