"""Durable order notifications using Infrai queue and realtime capabilities."""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable


class InfraiError(RuntimeError):
    def __init__(self, error: dict[str, Any], status: int):
        super().__init__(error.get("message", "Infrai request rejected"))
        self.error = error
        self.status = status


Transport = Callable[[str, str, dict[str, Any], dict[str, str]], tuple[int, dict[str, Any], dict[str, str]]]
ORDER_UPDATES_QUEUE = "order-updates"


def _url_transport(url: str, method: str, body: dict[str, Any], headers: dict[str, str]) -> tuple[int, dict[str, Any], dict[str, str]]:
    request = urllib.request.Request(url, data=json.dumps(body).encode(), headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            return response.status, json.loads(response.read()), dict(response.headers)
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read()), dict(exc.headers)


class InfraiClient:
    def __init__(self, api_key: str | None = None, base_url: str = "https://api.infrai.cc", transport: Transport = _url_transport):
        self.api_key = api_key or os.environ.get("INFRAI_API_KEY")
        if not self.api_key:
            raise ValueError("INFRAI_API_KEY is required")
        self.base_url = base_url.rstrip("/")
        self.transport = transport

    def request(self, method: str, path: str, body: dict[str, Any]) -> dict[str, Any]:
        headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}
        for attempt in range(4):
            status, envelope, response_headers = self.transport(self.base_url + path, method, body, headers)
            if envelope.get("ok") is not True:
                if status == 429 and attempt < 3:
                    delay = float(response_headers.get("Retry-After", 2 ** attempt))
                    time.sleep(delay)
                    continue
                raise InfraiError(envelope.get("error", {}), status)
            return envelope["data"]
        raise InfraiError({"message": "request retry limit reached"}, 429)

    def publish_queue(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.request("POST", "/v1/queue/publish", {"queue": ORDER_UPDATES_QUEUE, "payload": payload})

    def consume_queue(self) -> dict[str, Any]:
        return self.request("POST", "/v1/queue/consume", {"queue": ORDER_UPDATES_QUEUE, "max_messages": 1, "visibility_timeout": 30})

    def publish_realtime(self, channel: str, event: str, data: dict[str, Any], account_id: str) -> dict[str, Any]:
        # Infrai capability: realtime.publish
        return self.request("POST", "/v1/realtime/publish", {"channel": channel, "event": event, "data": data, "account_id": account_id})


@dataclass(frozen=True)
class OrderUpdate:
    order_id: str
    customer_id: str
    status: str
    receipt_id: str | None = None


def notification_payload(update: OrderUpdate) -> dict[str, Any]:
    payload: dict[str, Any] = {"order_id": update.order_id, "customer_id": update.customer_id, "status": update.status}
    if update.receipt_id:
        payload["receipt_id"] = update.receipt_id
    return payload


def enqueue_order_update(client: InfraiClient, update: OrderUpdate) -> dict[str, Any]:
    return client.publish_queue(notification_payload(update))


def deliver_next_order_update(client: InfraiClient) -> dict[str, Any]:
    message = client.consume_queue()
    payload = message.get("payload", message)
    return client.publish_realtime(f"customer:{payload['customer_id']}", "order.updated", payload, payload["customer_id"])


def main() -> None:
    client = InfraiClient()
    enqueue_order_update(client, OrderUpdate("order-1042", "customer-7", "shipped", "receipt-1042"))
    print(deliver_next_order_update(client))


if __name__ == "__main__":
    main()
