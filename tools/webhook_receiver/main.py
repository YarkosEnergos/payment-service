import logging
from collections import Counter
from typing import Annotated, Any

from fastapi import FastAPI, HTTPException, Query, status

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
)
logger = logging.getLogger("webhook-receiver")

app = FastAPI(title="Webhook receiver (demo)")

received: list[dict[str, Any]] = []
attempts: Counter[str] = Counter()


def payment_key(payload: dict[str, Any]) -> str:
    return str(payload.get("payment_id"))


@app.post("/webhooks")
async def receive(payload: dict[str, Any]) -> dict[str, str]:
    received.append(payload)
    logger.info(
        "Webhook received: %s -> %s",
        payment_key(payload),
        payload.get("status"),
    )
    return {"status": "ok"}


@app.post("/webhooks/fail")
async def always_fail(payload: dict[str, Any]) -> None:
    attempts[payment_key(payload)] += 1
    logger.warning(
        "Webhook rejected (always fails): %s, attempt %d",
        payment_key(payload),
        attempts[payment_key(payload)],
    )
    raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Receiver is down")


@app.post("/webhooks/flaky")
async def flaky(
    payload: dict[str, Any],
    failures: Annotated[int, Query(ge=0)] = 2,
) -> dict[str, str]:
    key = payment_key(payload)
    attempts[key] += 1
    if attempts[key] <= failures:
        logger.warning("Webhook rejected (flaky): %s, attempt %d", key, attempts[key])
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "Try again later")
    return await receive(payload)


@app.get("/webhooks")
async def list_received() -> list[dict[str, Any]]:
    return received
