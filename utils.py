"""
Utility functions for Kafka topic and consumer management.
"""

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, KafkaError
from config import KAFKA_BOOTSTRAP_SERVERS, TOPICS, CONSUMER_GROUPS


def create_topics():
    """Create all required topics for the learning project."""
    admin_client = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        client_id="topic_admin",
    )

    try:
        # Define topics to create
        new_topics = [
            NewTopic(
                name=TOPICS["orders"],
                num_partitions=2,
                replication_factor=1,
            ),
            NewTopic(
                name=TOPICS["payments"],
                num_partitions=2,
                replication_factor=1,
            ),
            NewTopic(
                name=TOPICS["notifications"],
                num_partitions=1,
                replication_factor=1,
            ),
            NewTopic(
                name=TOPICS["user_events"],
                num_partitions=1,
                replication_factor=1,
            ),
            NewTopic(
                name=TOPICS["order_confirmation"],
                num_partitions=1,
                replication_factor=1,
            ),
        ]

        # Create topics
        fs = admin_client.create_topics(new_topics=new_topics, validate_only=False)

        # Wait for operation to complete
        for topic, f in fs.items():
            try:
                f.result()  # Block until topic creation is complete
                print(f"✓ Topic '{topic}' created successfully")
            except TopicAlreadyExistsError:
                print(f"ℹ Topic '{topic}' already exists")
            except Exception as e:
                print(f"✗ Error creating topic '{topic}': {e}")

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        admin_client.close()


def list_topics():
    """List all available topics."""
    admin_client = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        client_id="topic_admin",
    )

    try:
        metadata = admin_client.describe_cluster()
        topics = admin_client.list_topics()

        print("📚 Available Topics:")
        for topic in sorted(topics.keys()):
            print(f"  - {topic}")

        print(f"\n📊 Cluster Info:")
        print(f"  Brokers: {metadata[0]}")
        print(f"  Controller: {metadata[1]}")
        print(f"  Topics: {len(topics)}")

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        admin_client.close()


def describe_topic(topic_name):
    """Describe a specific topic."""
    admin_client = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        client_id="topic_admin",
    )

    try:
        topics = admin_client.describe_topics([topic_name])
        topic_desc = topics[topic_name]

        print(f"📋 Topic: {topic_name}")
        print(f"  Partitions: {len(topic_desc.partitions)}")
        print(f"  Replication Factor: {topic_desc.partitions[0].replicas if topic_desc.partitions else 'N/A'}")
        print()

        for i, partition in enumerate(topic_desc.partitions):
            print(f"  Partition {i}:")
            print(f"    Leader: {partition.leader}")
            print(f"    Replicas: {partition.replicas}")
            print(f"    ISR: {partition.isr}")

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        admin_client.close()


def delete_topic(topic_name):
    """Delete a topic."""
    admin_client = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        client_id="topic_admin",
    )

    try:
        fs = admin_client.delete_topics([topic_name])
        fs[topic_name].result()
        print(f"✓ Topic '{topic_name}' deleted successfully")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        admin_client.close()


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python utils.py create              - Create all required topics")
        print("  python utils.py list                - List all topics")
        print("  python utils.py describe <topic>    - Describe a topic")
        print("  python utils.py delete <topic>      - Delete a topic")
        sys.exit(1)

    command = sys.argv[1].lower()

    if command == "create":
        create_topics()
    elif command == "list":
        list_topics()
    elif command == "describe" and len(sys.argv) > 2:
        describe_topic(sys.argv[2])
    elif command == "delete" and len(sys.argv) > 2:
        delete_topic(sys.argv[2])
    else:
        print("❌ Invalid command or missing arguments")
