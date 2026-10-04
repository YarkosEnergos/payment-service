import httpx
from pydantic import BaseModel


class WebhookDeliveryError(Exception):
    def __init__(self, url: str, reason: str) -> None:
        super().__init__(f"Webhook delivery to {url} failed: {reason}")
        self.url = url


class WebhookClient:
    def __init__(self, http: httpx.AsyncClient) -> None:
        self.http = http

    async def send(self, url: str, payload: BaseModel) -> None:
        try:
            response = await self.http.post(
                url,
                content=payload.model_dump_json(),
                headers={"Content-Type": "application/json"},
            )
            response.raise_for_status()
        except httpx.HTTPError as error:
            reason = str(error) or type(error).__name__
            raise WebhookDeliveryError(url, reason) from error
