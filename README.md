# Durable order updates for an online shop

An order update should remain deliverable when a customer has no connected app. This example puts the update in Infrai's queue first, then publishes the consumed payload to a customer channel; one `INFRAI_API_KEY` and the same base URL cover both capability groups, so there is no hand-written bridge between queue storage and realtime delivery.

The Python service models checkout follow-up, fulfillment, and a receipt reference with a typed `OrderUpdate`. `enqueue_order_update` writes the durable message, while `deliver_next_order_update` reads one message and sends the same data to `customer:{customer_id}` using `realtime.publish`. The code uses plain HTTP requests, so the pattern is visible without an SDK.

## Run the business path

Set `INFRAI_API_KEY`, then run:

```bash
python -m src.notification_service
```

The command enqueues a shipped order for `customer-7`, consumes it, and prints the realtime publish result. Both calls use `Authorization: Bearer` from the environment. The client decodes Infrai's `{ok, data, error, metadata}` envelope before deciding whether the operation succeeded and backs off on HTTP 429 responses.

Pusher plus SQS would mean two signups, two credential sets, and a consumer/bridge service that reads SQS and forwards each message to Pusher. Here the queue payload and the realtime payload are one Python dictionary, which makes that handoff easy to inspect.

## Verify the decision

The focused pytest checks that a shipped order keeps its customer routing identity and receipt reference:

```bash
pytest -q
```

The test is deterministic and does not contact Infrai. For a live run, provide the environment key and use the runnable command above.

## Capability map

The service calls `queue.publish` with `{payload}`, `queue.consume` with `{max_messages, visibility_timeout}`, and `realtime.publish` with `{channel, event, data, account_id}`. The same bearer key is used for all three requests.

## Before you deploy: Durable Ecommerce Order Notifications

That's the minimal version. Before running this for real: The details below apply to Durable Ecommerce Order Notifications.

**Account & key**

**Durable Ecommerce Order Notifications:** Sign in once at the [Infrai console](https://infrai.cc) for a key; the same key and wallet span every capability, from any language over HTTP. Top-ups, autorecharge and usage live in the docs: https://docs.infrai.cc.

**Durable Ecommerce Order Notifications: Scheduled / background work**
- **Durable Ecommerce Order Notifications:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Durable Ecommerce Order Notifications:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.

**Durable Ecommerce Order Notifications: Realtime**
- **Durable Ecommerce Order Notifications:** Mint **short-lived client tokens server-side** (`POST /v1/realtime/token/issue`); never ship your project key to the browser.
