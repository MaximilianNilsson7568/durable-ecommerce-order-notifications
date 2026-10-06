from src.notification_service import OrderUpdate, notification_payload


def test_shipped_order_contains_customer_routing_and_receipt():
    update = OrderUpdate("o-1", "c-9", "shipped", "r-1")
    assert notification_payload(update) == {
        "order_id": "o-1",
        "customer_id": "c-9",
        "status": "shipped",
        "receipt_id": "r-1",
    }

