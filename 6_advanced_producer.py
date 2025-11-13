"""
Advanced Producer Example
Demonstrates partitioning, key-based routing, and batching strategies.
"""

import json
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError
from config import KAFKA_BOOTSTRAP_SERVERS, TOPICS


def create_producer(batch_size=100, linger_ms=10):
    """Create producer with batching configuration."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        # Batching configuration
        batch_size=batch_size,  # Wait for this many bytes before sending
        linger_ms=linger_ms,  # Wait this many ms for batch to fill up
        # Compression
        compression_type="gzip",  # Compress messages: 'gzip', 'snappy', 'lz4', 'zstd'
    )


def produce_with_keys():
    """
    Produce messages with keys for partitioning.
    Messages with the same key go to the same partition.
    """
    producer = create_producer()

    print("📤 Producing messages with keys (key-based partitioning)...\n")

    # Messages from different users
    user_events = [
        {"user_id": "user_001", "action": "login", "timestamp": int(time.time())},
        {"user_id": "user_002", "action": "purchase", "timestamp": int(time.time())},
        {"user_id": "user_001", "action": "view_product", "timestamp": int(time.time())},
        {"user_id": "user_003", "action": "login", "timestamp": int(time.time())},
        {"user_id": "user_002", "action": "add_to_cart", "timestamp": int(time.time())},
        {"user_id": "user_001", "action": "logout", "timestamp": int(time.time())},
    ]

    for event in user_events:
        user_id = event["user_id"]
        # Use user_id as the key - all events for same user go to same partition
        future = producer.send(TOPICS["user_events"], value=event, key=user_id.encode())

        try:
            record_metadata = future.get(timeout=10)
            print(
                f"✓ Message sent to partition {record_metadata.partition} "
                f"for user {user_id}: {event['action']}"
            )
        except KafkaError as e:
            print(f"✗ Failed to send message: {e}")

    producer.flush()
    print("\n✓ All keyed messages sent!")
    producer.close()


def produce_with_timestamps():
    """
    Produce messages with custom timestamps.
    Useful for replaying events based on event time vs. processing time.
    """
    producer = create_producer()

    print("📤 Producing messages with custom timestamps...\n")

    base_time = int(time.time()) - 3600  # 1 hour ago

    events = [
        {
            "event_id": "evt_001",
            "type": "user_signup",
            "data": {"email": "user1@example.com"},
        },
        {
            "event_id": "evt_002",
            "type": "user_purchase",
            "data": {"user_id": "user_001", "amount": 99.99},
        },
        {
            "event_id": "evt_003",
            "type": "user_review",
            "data": {"user_id": "user_001", "rating": 5},
        },
    ]

    for i, event in enumerate(events):
        event_timestamp = base_time + (i * 600)  # 10 minutes apart

        future = producer.send(
            TOPICS["user_events"],
            value=event,
            timestamp_ms=event_timestamp * 1000,  # Convert to milliseconds
        )

        try:
            record_metadata = future.get(timeout=10)
            print(f"✓ Event {event['event_id']} sent with timestamp: {event_timestamp}")
        except KafkaError as e:
            print(f"✗ Failed to send event: {e}")

    producer.flush()
    print("\n✓ All timestamped messages sent!")
    producer.close()


def produce_with_headers():
    """
    Produce messages with headers for metadata.
    Headers are useful for tracing, versioning, and other metadata.
    """
    producer = create_producer()

    print("📤 Producing messages with headers...\n")

    events = [
        {"user_id": "user_001", "action": "click"},
        {"user_id": "user_002", "action": "scroll"},
        {"user_id": "user_003", "action": "submit"},
    ]

    for event in events:
        headers = [
            ("source", b"mobile_app"),
            ("version", b"1.0"),
            ("trace_id", f"trace_{int(time.time())}".encode()),
            ("user_agent", b"iOS/14.5"),
        ]

        future = producer.send(
            TOPICS["user_events"],
            value=event,
            headers=headers,
        )

        try:
            record_metadata = future.get(timeout=10)
            print(
                f"✓ Message with headers sent to partition {record_metadata.partition}: {event['action']}"
            )
        except KafkaError as e:
            print(f"✗ Failed to send message: {e}")

    producer.flush()
    print("\n✓ All messages with headers sent!")
    producer.close()


def produce_in_batches():
    """
    Demonstrate efficient batch sending.
    The producer will automatically batch messages based on batch_size and linger_ms.
    """
    producer = create_producer(batch_size=500, linger_ms=100)

    print("📤 Producing messages in batches (auto-batching enabled)...\n")
    print("This will batch messages automatically based on batch_size and linger_ms\n")

    # Generate many messages
    batch_count = 0
    start_time = time.time()

    for i in range(100):
        event = {
            "batch_id": i // 10,
            "message_num": i,
            "data": f"Batch message {i}",
            "timestamp": int(time.time()),
        }

        producer.send(TOPICS["user_events"], value=event)

        if (i + 1) % 10 == 0:
            batch_count += 1
            print(f"  Queued {i + 1} messages...")

    # Flush to ensure all messages are sent
    producer.flush()
    elapsed = time.time() - start_time

    print(f"\n✓ Sent 100 messages in {elapsed:.2f} seconds")
    print(f"  Average: {100 / elapsed:.0f} messages/sec")
    producer.close()


if __name__ == "__main__":
    import sys

    mode = sys.argv[1] if len(sys.argv) > 1 else "keys"

    try:
        if mode == "keys":
            produce_with_keys()
        elif mode == "timestamps":
            produce_with_timestamps()
        elif mode == "headers":
            produce_with_headers()
        elif mode == "batches":
            produce_in_batches()
        else:
            print("Usage:")
            print("  python 6_advanced_producer.py keys        - Send with partition keys")
            print("  python 6_advanced_producer.py timestamps  - Send with custom timestamps")
            print("  python 6_advanced_producer.py headers     - Send with message headers")
            print("  python 6_advanced_producer.py batches     - Send in batches (auto-batching)")
    except Exception as e:
        print(f"❌ Error: {e}")
