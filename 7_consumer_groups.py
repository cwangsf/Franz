"""
Consumer Groups Example
Demonstrates how multiple consumers in a group share partitions.
"""

import json
import time
from kafka import KafkaConsumer
from config import KAFKA_BOOTSTRAP_SERVERS, TOPICS, CONSUMER_GROUPS


def consume_with_group_id():
    """
    Consume with a specific group ID.
    Multiple consumers with same group_id will share partitions.
    """
    consumer = KafkaConsumer(
        TOPICS["user_events"],
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUPS["analytics"],
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        enable_auto_commit=True,
        # Additional options
        session_timeout_ms=30000,
        heartbeat_interval_ms=10000,
    )

    print(f"📥 Consumer in group '{CONSUMER_GROUPS['analytics']}' started...\n")
    print("(Press Ctrl+C to stop)\n")

    message_count = 0

    try:
        for message in consumer:
            message_count += 1
            print(f"Message #{message_count}:")
            print(f"  Partition: {message.partition}")
            print(f"  Offset: {message.offset}")
            print(f"  Timestamp: {message.timestamp}")
            print(f"  Value: {json.dumps(message.value, indent=4)}")
            print()

    except KeyboardInterrupt:
        print(f"\n⚠️  Consumer stopped. Consumed {message_count} messages.")
    finally:
        consumer.close()


def consume_with_manual_commit():
    """
    Consume with manual offset commits.
    Gives more control over when offsets are committed.
    """
    consumer = KafkaConsumer(
        TOPICS["user_events"],
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="manual_commit_group",
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        enable_auto_commit=False,  # Disable auto-commit
    )

    print("📥 Consumer with manual commit started...\n")
    print("(Press Ctrl+C to stop)\n")

    message_count = 0

    try:
        for message in consumer:
            message_count += 1

            # Process the message
            print(f"Processing message #{message_count}: {message.value.get('event_type', 'unknown')}")

            # Manually commit offset after processing
            try:
                consumer.commit()
                print(f"  ✓ Offset committed: {message.offset}")
            except Exception as e:
                print(f"  ✗ Failed to commit offset: {e}")

            print()

    except KeyboardInterrupt:
        print(f"\n⚠️  Consumer stopped. Processed {message_count} messages.")
    finally:
        consumer.close()


def consume_specific_partition():
    """
    Consume from a specific partition.
    Useful for direct partition access without consumer groups.
    """
    from kafka import TopicPartition

    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
    )

    # Assign specific partition (not using consumer group)
    partition = TopicPartition(TOPICS["user_events"], 0)
    consumer.assign([partition])

    print(f"📥 Consuming from {TOPICS['user_events']} partition 0...\n")
    print("(Press Ctrl+C to stop)\n")

    message_count = 0

    try:
        for message in consumer:
            message_count += 1
            print(f"Message #{message_count}:")
            print(f"  Offset: {message.offset}")
            print(f"  Value: {json.dumps(message.value, indent=4)}")
            print()

    except KeyboardInterrupt:
        print(f"\n⚠️  Consumer stopped. Consumed {message_count} messages.")
    finally:
        consumer.close()


def consume_with_seek():
    """
    Consume from a specific offset using seek.
    Useful for replaying messages from a specific point.
    """
    from kafka import TopicPartition

    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    partition = TopicPartition(TOPICS["user_events"], 0)
    consumer.assign([partition])

    # Seek to offset 0 (beginning)
    consumer.seek(partition, 0)

    print(f"📥 Consuming from offset 0 (beginning)...\n")
    print("(Will consume 5 messages then stop)\n")

    message_count = 0
    max_messages = 5

    try:
        for message in consumer:
            message_count += 1
            print(f"Message #{message_count} (offset {message.offset}):")
            print(f"  Value: {json.dumps(message.value, indent=4)}")
            print()

            if message_count >= max_messages:
                print(f"✓ Consumed {max_messages} messages as requested")
                break

    except KeyboardInterrupt:
        print(f"\n⚠️  Consumer stopped.")
    finally:
        consumer.close()


def consume_with_filter():
    """
    Consume messages but only process specific ones (filtering).
    """
    consumer = KafkaConsumer(
        TOPICS["user_events"],
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="filter_group",
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        enable_auto_commit=True,
    )

    print("📥 Consumer with filtering (only 'login' events)...\n")
    print("(Press Ctrl+C to stop)\n")

    login_count = 0
    total_count = 0

    try:
        for message in consumer:
            total_count += 1
            event = message.value

            # Filter: only process login events
            if event.get("event_type") == "login":
                login_count += 1
                print(f"✓ Login event #{login_count}: {event.get('user_id')} logged in")
            else:
                print(f"  (Skipped {event.get('event_type')} event)")

    except KeyboardInterrupt:
        print(f"\n⚠️  Consumer stopped.")
        print(f"  Total messages: {total_count}")
        print(f"  Login events: {login_count}")
    finally:
        consumer.close()


if __name__ == "__main__":
    import sys

    mode = sys.argv[1] if len(sys.argv) > 1 else "group"

    try:
        if mode == "group":
            consume_with_group_id()
        elif mode == "manual":
            consume_with_manual_commit()
        elif mode == "partition":
            consume_specific_partition()
        elif mode == "seek":
            consume_with_seek()
        elif mode == "filter":
            consume_with_filter()
        else:
            print("Usage:")
            print("  python 7_consumer_groups.py group     - Consume with consumer group")
            print("  python 7_consumer_groups.py manual    - Manual offset commits")
            print("  python 7_consumer_groups.py partition - Consume from specific partition")
            print("  python 7_consumer_groups.py seek      - Seek to specific offset")
            print("  python 7_consumer_groups.py filter    - Filter messages while consuming")
    except Exception as e:
        print(f"❌ Error: {e}")
