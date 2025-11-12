"""
Kafka Stream Processing Example
Demonstrates stateful stream processing patterns.
"""

import json
import time
from kafka import KafkaConsumer, KafkaProducer
from config import KAFKA_BOOTSTRAP_SERVERS, TOPICS, CONSUMER_GROUPS


class StreamProcessor:
    """Process streams of data from Kafka topics."""

    def __init__(self):
        self.consumer = KafkaConsumer(
            TOPICS["user_events"],
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=CONSUMER_GROUPS["analytics"],
            auto_offset_reset="earliest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            enable_auto_commit=True,
        )

        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        # In-memory store for aggregations (in production, use state stores)
        self.user_event_counts = {}
        self.event_type_counts = {}

    def process_event(self, event):
        """Process a single event."""
        user_id = event.get("user_id")
        event_type = event.get("event_type")

        # Update counters
        self.user_event_counts[user_id] = self.user_event_counts.get(user_id, 0) + 1
        self.event_type_counts[event_type] = (
            self.event_type_counts.get(event_type, 0) + 1
        )

        # Create enriched event with statistics
        enriched_event = {
            **event,
            "user_total_events": self.user_event_counts[user_id],
            "event_type_total": self.event_type_counts[event_type],
        }

        return enriched_event

    def run(self):
        """Run the stream processor."""
        print(
            f"🔄 Stream Processor started, consuming from '{TOPICS['user_events']}'...\n"
        )
        message_count = 0

        try:
            for message in self.consumer:
                event = message.value
                message_count += 1

                # Process the event
                enriched_event = self.process_event(event)

                # Send to output topic
                self.producer.send(TOPICS["order_confirmation"], enriched_event)

                # Display statistics
                print(f"[Message #{message_count}] Processed event:")
                print(f"  User: {event['user_id']}")
                print(f"  Type: {event['event_type']}")
                print(f"  User total events: {enriched_event['user_total_events']}")
                print(
                    f"  Event type total: {enriched_event['event_type_total']}"
                )
                print()

        except KeyboardInterrupt:
            print(f"\n⚠️  Processor stopped. Processed {message_count} messages.")
        finally:
            self.producer.flush()
            self.consumer.close()
            self.producer.close()


class WindowedAggregation:
    """Demonstrate time-windowed aggregation."""

    def __init__(self, window_size=5):
        self.window_size = window_size  # seconds
        self.consumer = KafkaConsumer(
            TOPICS["user_events"],
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id="windowed_aggregation",
            auto_offset_reset="earliest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            enable_auto_commit=True,
        )
        self.current_window = {}
        self.window_start = time.time()

    def emit_window(self):
        """Emit aggregated window results."""
        if self.current_window:
            print(f"\n📊 Window Results ({self.window_size}s):")
            print(f"  Event types in window: {self.current_window}")
            self.current_window = {}
            self.window_start = time.time()

    def run(self):
        """Run windowed aggregation."""
        print(f"📊 Windowed Aggregation (window size: {self.window_size}s)...\n")

        try:
            for message in self.consumer:
                event = message.value
                event_type = event.get("event_type")

                # Add to current window
                self.current_window[event_type] = (
                    self.current_window.get(event_type, 0) + 1
                )

                # Check if window should close
                if time.time() - self.window_start >= self.window_size:
                    self.emit_window()

        except KeyboardInterrupt:
            print("\n⚠️  Windowed aggregation stopped.")
        finally:
            self.consumer.close()


if __name__ == "__main__":
    import sys

    mode = sys.argv[1] if len(sys.argv) > 1 else "stateful"

    try:
        if mode == "windowed":
            processor = WindowedAggregation(window_size=5)
            processor.run()
        else:
            processor = StreamProcessor()
            processor.run()
    except Exception as e:
        print(f"❌ Error: {e}")
