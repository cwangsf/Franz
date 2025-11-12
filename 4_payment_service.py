"""
Payment Service - Microservice that processes payments
Consumes from orders topic and produces to payments topic.
"""

import json
import time
import random
from kafka import KafkaConsumer, KafkaProducer
from config import KAFKA_BOOTSTRAP_SERVERS, TOPICS, CONSUMER_GROUPS


class PaymentService:
    """Process payment requests from orders."""

    def __init__(self):
        self.consumer = KafkaConsumer(
            TOPICS["orders"],
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=CONSUMER_GROUPS["payment_service"],
            auto_offset_reset="earliest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            enable_auto_commit=True,
        )

        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        self.processed_count = 0
        self.success_count = 0

    def process_payment(self, order):
        """Process a payment for an order."""
        # Simulate payment processing
        time.sleep(0.5)

        # Randomly succeed or fail (90% success rate)
        success = random.random() < 0.9

        payment = {
            "order_id": order["order_id"],
            "user_id": order["user_id"],
            "amount": order["amount"],
            "status": "success" if success else "failed",
            "transaction_id": f"TXN-{int(time.time() * 1000)}",
            "timestamp": int(time.time()),
        }

        return payment

    def run(self):
        """Run the payment service."""
        print(
            f"💳 Payment Service started, consuming from '{TOPICS['orders']}'...\n"
        )

        try:
            for message in self.consumer:
                order = message.value
                self.processed_count += 1

                # Process payment
                payment = self.process_payment(order)

                # Send payment result
                self.producer.send(TOPICS["payments"], payment)

                if payment["status"] == "success":
                    self.success_count += 1
                    status_emoji = "✓"
                else:
                    status_emoji = "✗"

                print(f"{status_emoji} Payment #{self.processed_count}:")
                print(f"  Order ID: {payment['order_id']}")
                print(f"  Amount: ${payment['amount']}")
                print(f"  Status: {payment['status']}")
                print(f"  Transaction ID: {payment['transaction_id']}")
                print()

        except KeyboardInterrupt:
            print(f"\n⚠️  Payment service stopped.")
            print(f"  Total processed: {self.processed_count}")
            print(f"  Successful: {self.success_count}")
            print(
                f"  Success rate: {(self.success_count / self.processed_count * 100):.1f}%"
            )
        finally:
            self.producer.flush()
            self.consumer.close()
            self.producer.close()


if __name__ == "__main__":
    try:
        service = PaymentService()
        service.run()
    except Exception as e:
        print(f"❌ Error: {e}")
