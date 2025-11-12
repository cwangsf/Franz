"""
Order Producer - Part of microservices example
Produces order messages to the orders topic.
"""

import json
import time
import random
from kafka import KafkaProducer
from config import KAFKA_BOOTSTRAP_SERVERS, TOPICS


def create_producer():
    """Create and return a Kafka producer."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def generate_order():
    """Generate a sample order."""
    order_id = f"ORD-{int(time.time() * 1000)}"
    user_id = f"user_{random.randint(1, 5)}"
    amount = round(random.uniform(10, 500), 2)

    return {
        "order_id": order_id,
        "user_id": user_id,
        "amount": amount,
        "timestamp": int(time.time()),
        "items": [
            {"product": f"Product {random.randint(1, 10)}", "quantity": random.randint(1, 5)}
        ],
    }


def produce_orders(num_orders=10, interval=2):
    """Produce orders to the orders topic."""
    producer = create_producer()

    print(f"📤 Starting to produce {num_orders} orders...\n")

    try:
        for i in range(num_orders):
            order = generate_order()
            producer.send(TOPICS["orders"], order)
            print(f"[{i+1}/{num_orders}] Order sent: {order['order_id']} - ${order['amount']}")
            time.sleep(interval)

        producer.flush()
        print("\n✓ All orders sent!")

    except KeyboardInterrupt:
        print("\n⚠️  Producer interrupted by user")
    finally:
        producer.close()


if __name__ == "__main__":
    import sys

    num_orders = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    interval = int(sys.argv[2]) if len(sys.argv) > 2 else 2

    try:
        produce_orders(num_orders, interval)
    except Exception as e:
        print(f"❌ Error: {e}")
