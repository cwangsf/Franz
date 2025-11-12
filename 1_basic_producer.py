"""
Basic Kafka Producer Example
Demonstrates how to send messages to a Kafka topic.
"""

import json
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError
from config import KAFKA_BOOTSTRAP_SERVERS, TOPICS


def create_producer():
    """Create and return a Kafka producer."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def on_send_success(record_metadata):
    """Callback for successful message send."""
    print(
        f"✓ Message sent successfully to partition {record_metadata.partition} "
        f"at offset {record_metadata.offset}"
    )


def on_send_error(exc):
    """Callback for failed message send."""
    print(f"✗ Error sending message: {exc}")


def produce_user_events():
    """Produce sample user events to the user_events topic."""
    producer = create_producer()

    sample_events = [
        {
            "user_id": "user_001",
            "event_type": "login",
            "timestamp": int(time.time()),
            "metadata": {"ip": "192.168.1.1"},
        },
        {
            "user_id": "user_002",
            "event_type": "purchase",
            "timestamp": int(time.time()),
            "metadata": {"amount": 99.99, "product": "Laptop"},
        },
        {
            "user_id": "user_001",
            "event_type": "logout",
            "timestamp": int(time.time()),
            "metadata": {"session_duration": 3600},
        },
        {
            "user_id": "user_003",
            "event_type": "signup",
            "timestamp": int(time.time()),
            "metadata": {"email": "user3@example.com"},
        },
    ]

    print("📤 Producing user events...\n")

    for event in sample_events:
        # Send message with optional callback
        future = producer.send(TOPICS["user_events"], event)
        future.add_callback(on_send_success)
        future.add_errback(on_send_error)

        print(f"Sending: {json.dumps(event)}")
        time.sleep(0.5)  # Small delay between messages

    # Wait for all messages to be sent
    producer.flush()
    print("\n✓ All messages sent!")
    producer.close()


if __name__ == "__main__":
    try:
        produce_user_events()
    except KeyboardInterrupt:
        print("\n⚠️  Producer interrupted by user")
    except Exception as e:
        print(f"❌ Error: {e}")
