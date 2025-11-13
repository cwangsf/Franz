"""
Error Handling and Retry Example
Demonstrates how to handle errors and implement retry logic.
"""

import json
import time
import random
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
from config import KAFKA_BOOTSTRAP_SERVERS, TOPICS


class RetryProducer:
    """Producer with retry logic."""

    def __init__(self, max_retries=3, retry_backoff_ms=100):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",  # Wait for all replicas to acknowledge
            retries=max_retries,
            retry_backoff_ms=retry_backoff_ms,
        )
        self.max_retries = max_retries

    def send_with_retry(self, topic, message, key=None):
        """Send message with retry logic."""
        retry_count = 0

        while retry_count < self.max_retries:
            try:
                future = self.producer.send(topic, value=message, key=key)
                record_metadata = future.get(timeout=10)

                print(
                    f"✓ Message sent successfully to partition {record_metadata.partition} "
                    f"at offset {record_metadata.offset}"
                )
                return True

            except KafkaError as e:
                retry_count += 1
                wait_time = 2 ** retry_count  # Exponential backoff

                print(f"✗ Send failed (attempt {retry_count}/{self.max_retries}): {e}")

                if retry_count < self.max_retries:
                    print(f"  Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    print(f"  Max retries exceeded. Giving up.")
                    return False

        return False

    def close(self):
        self.producer.flush()
        self.producer.close()


class ErrorHandlingConsumer:
    """Consumer with error handling."""

    def __init__(self, topic, group_id, max_errors=3):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=group_id,
            auto_offset_reset="earliest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            enable_auto_commit=False,  # Manual commit for better error handling
        )
        self.max_errors = max_errors
        self.error_count = 0

    def process_message(self, message):
        """
        Process a message with error handling.
        Simulate processing that might fail randomly.
        """
        try:
            # Simulate processing
            event = message.value

            # Randomly fail some messages for demonstration
            if random.random() < 0.3:  # 30% failure rate
                raise Exception(f"Simulated processing error for {event.get('id')}")

            print(f"✓ Processed message: {event.get('id')}")
            return True

        except Exception as e:
            self.error_count += 1
            print(f"✗ Error processing message: {e}")

            if self.error_count >= self.max_errors:
                print(f"  Max errors ({self.max_errors}) reached. Stopping.")
                return None

            return False

    def consume_with_error_handling(self, max_messages=10):
        """Consume with error handling and retry."""
        message_count = 0
        processed_count = 0

        try:
            for message in self.consumer:
                message_count += 1
                print(f"\nProcessing message #{message_count} (offset {message.offset}):")

                # Try to process
                result = self.process_message(message)

                if result is None:  # Max errors reached
                    break

                if result:  # Successfully processed
                    processed_count += 1
                    # Commit offset only after successful processing
                    try:
                        self.consumer.commit()
                        print(f"  ✓ Offset committed")
                    except Exception as e:
                        print(f"  ✗ Failed to commit offset: {e}")

                if message_count >= max_messages:
                    break

            print(f"\n✓ Finished consuming")
            print(f"  Total messages: {message_count}")
            print(f"  Successfully processed: {processed_count}")
            print(f"  Failed: {self.error_count}")

        except KeyboardInterrupt:
            print(f"\n⚠️  Consumer interrupted")
        finally:
            self.consumer.close()


def produce_with_error_handling():
    """Demonstrate producer with error handling."""
    producer = RetryProducer(max_retries=3, retry_backoff_ms=500)

    print("📤 Producing messages with error handling...\n")

    messages = [
        {"id": "msg_001", "data": "First message"},
        {"id": "msg_002", "data": "Second message"},
        {"id": "msg_003", "data": "Third message"},
        {"id": "msg_004", "data": "Fourth message"},
    ]

    success_count = 0

    for msg in messages:
        print(f"\nSending message: {msg['id']}")
        if producer.send_with_retry(TOPICS["user_events"], msg):
            success_count += 1

    print(f"\n✓ Successfully sent {success_count}/{len(messages)} messages")
    producer.close()


def consume_with_error_handling():
    """Demonstrate consumer with error handling."""
    consumer = ErrorHandlingConsumer(
        TOPICS["user_events"],
        group_id="error_handling_group",
        max_errors=3,
    )

    print("📥 Consuming messages with error handling...\n")
    consumer.consume_with_error_handling(max_messages=20)


def produce_with_callback():
    """Demonstrate async production with callbacks."""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print("📤 Producing messages with callbacks...\n")

    def on_success(metadata):
        print(f"✓ Success: Message sent to partition {metadata.partition} at offset {metadata.offset}")

    def on_error(exc):
        print(f"✗ Error: {exc}")

    messages = [
        {"id": "async_001", "data": "Async message 1"},
        {"id": "async_002", "data": "Async message 2"},
        {"id": "async_003", "data": "Async message 3"},
    ]

    for msg in messages:
        print(f"Sending (async): {msg['id']}")
        future = producer.send(TOPICS["user_events"], value=msg)
        future.add_callback(on_success)
        future.add_errback(on_error)

    # Wait for all messages to be sent
    producer.flush()
    print("\n✓ All async messages sent!")
    producer.close()


if __name__ == "__main__":
    import sys

    mode = sys.argv[1] if len(sys.argv) > 1 else "produce"

    try:
        if mode == "produce":
            produce_with_error_handling()
        elif mode == "consume":
            consume_with_error_handling()
        elif mode == "callback":
            produce_with_callback()
        else:
            print("Usage:")
            print("  python 8_error_handling.py produce  - Producer with retry logic")
            print("  python 8_error_handling.py consume  - Consumer with error handling")
            print("  python 8_error_handling.py callback - Producer with async callbacks")
    except Exception as e:
        print(f"❌ Error: {e}")
