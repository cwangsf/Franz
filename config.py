"""Configuration for Kafka learning project."""

import os
from dotenv import load_dotenv

load_dotenv()

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
).split(",")

# Topic names
TOPICS = {
    "orders": "orders",
    "payments": "payments",
    "notifications": "notifications",
    "user_events": "user_events",
    "order_confirmation": "order_confirmation",
}

# Consumer group names
CONSUMER_GROUPS = {
    "payment_service": "payment_service",
    "notification_service": "notification_service",
    "analytics": "analytics",
}

# Application settings
DEBUG = os.getenv("DEBUG", "True").lower() == "true"
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
