"""
Notification Service - Microservice that sends notifications
Consumes from payments and orders topics.
"""

import json
from kafka import KafkaConsumer
from config import KAFKA_BOOTSTRAP_SERVERS, TOPICS, CONSUMER_GROUPS


class NotificationService:
    """Send notifications based on payment events."""

    def __init__(self):
        # Consume from multiple topics
        self.consumer = KafkaConsumer(
            TOPICS["payments"],
            TOPICS["orders"],
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=CONSUMER_GROUPS["notification_service"],
            auto_offset_reset="earliest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            enable_auto_commit=True,
        )

        self.notification_count = 0

    def send_notification(self, message_type, data):
        """Simulate sending a notification."""
        self.notification_count += 1

        if message_type == "order_created":
            print(f"📧 Notification #{self.notification_count}: Order Confirmation")
            print(f"   To: {data['user_id']}")
            print(f"   Content: Your order {data['order_id']} for ${data['amount']} has been created")

        elif message_type == "payment_processed":
            if data["status"] == "success":
                print(
                    f"📧 Notification #{self.notification_count}: Payment Confirmation"
                )
                print(f"   To: {data['user_id']}")
                print(
                    f"   Content: Payment of ${data['amount']} (txn: {data['transaction_id']}) received!"
                )
            else:
                print(f"📧 Notification #{self.notification_count}: Payment Failed")
                print(f"   To: {data['user_id']}")
                print(
                    f"   Content: Payment of ${data['amount']} failed. Please retry or use another method."
                )

        print()

    def run(self):
        """Run the notification service."""
        print(
            f"📧 Notification Service started, consuming from '{TOPICS['payments']}' and '{TOPICS['orders']}'...\n"
        )

        try:
            for message in self.consumer:
                if message.topic == TOPICS["orders"]:
                    self.send_notification("order_created", message.value)

                elif message.topic == TOPICS["payments"]:
                    self.send_notification("payment_processed", message.value)

        except KeyboardInterrupt:
            print(f"\n⚠️  Notification service stopped.")
            print(f"  Total notifications sent: {self.notification_count}")
        finally:
            self.consumer.close()


if __name__ == "__main__":
    try:
        service = NotificationService()
        service.run()
    except Exception as e:
        print(f"❌ Error: {e}")
