"""
Basic Kafka Consumer Example
Demonstrates how to receive messages from a Kafka topic.
"""

import json
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from config import KAFKA_BOOTSTRAP_SERVERS, TOPICS, CONSUMER_GROUPS


def create_consumer(topic, group_id):
    """Create and return a Kafka consumer."""
    return KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=group_id,
        auto_offset_reset="earliest",  # Start from beginning if no offset exists
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        enable_auto_commit=True,  # Auto commit offsets
    )


def consume_messages(topic, group_id, max_messages=None):
    """Consume messages from a Kafka topic."""
    consumer = create_consumer(topic, group_id)
    message_count = 0

    print(f"📥 Consuming messages from topic '{topic}' with group '{group_id}'...")
    print(f"(Press Ctrl+C to stop)\n")

    try:
        for message in consumer:
            message_count += 1
            print(f"Message #{message_count}:")
            print(f"  Partition: {message.partition}")
            print(f"  Offset: {message.offset}")
            print(f"  Key: {message.key}")
            print(f"  Value: {json.dumps(message.value, indent=4)}")
            print()

            if max_messages and message_count >= max_messages:
                print(f"✓ Consumed {max_messages} messages. Stopping.")
                break

    except KeyboardInterrupt:
        print(f"\n⚠️  Consumer interrupted. Consumed {message_count} messages total.")
    finally:
        consumer.close()


if __name__ == "__main__":
    import sys

    # Read topic name from command line or use default
    topic = sys.argv[1] if len(sys.argv) > 1 else TOPICS["user_events"]
    group_id = sys.argv[2] if len(sys.argv) > 2 else "basic_consumer_group"

    try:
        consume_messages(topic, group_id)
    except Exception as e:
        print(f"❌ Error: {e}")
