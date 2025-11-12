# Apache Kafka Learning Project

A comprehensive Python project to learn Apache Kafka from basics to advanced microservices architecture. This project covers producers, consumers, stream processing, and distributed systems patterns.

## Project Structure

```
├── 1_basic_producer.py          # Basic producer example
├── 2_basic_consumer.py          # Basic consumer example
├── 3_stream_processing.py       # Stateful and windowed aggregations
├── 3b_producer_orders.py        # Order producer for microservices
├── 4_payment_service.py         # Microservice: processes payments
├── 5_notification_service.py    # Microservice: sends notifications
├── config.py                    # Configuration
├── requirements.txt             # Python dependencies
├── .env.example                 # Example environment variables
└── README.md                    # This file
```

## Prerequisites

- Python 3.8+
- Java 8+ (for running Kafka locally)
- Kafka 2.13+ (for local installation)

## Installation

### 1. Install Kafka Locally

#### Option A: Using Homebrew (macOS)
```bash
brew install kafka

# Start Kafka in the background
brew services start kafka
```

#### Option B: Manual Installation

**Download Kafka:**
```bash
# Download latest version from https://kafka.apache.org/downloads
# Extract it
tar -xzf kafka_2.13-3.x.x.tgz
cd kafka_2.13-3.x.x
```

**Start Zookeeper:**
```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
```

**In another terminal, start Kafka broker:**
```bash
bin/kafka-server-start.sh config/server.properties
```

#### Verify Kafka is running:
```bash
# This should return successfully
jps | grep Kafka
```

### 2. Set up Python Environment

```bash
# Clone/enter the project directory
cd ~/Documents/Coding/Franz

# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Copy environment variables
cp .env.example .env
```

## Learning Path

### Level 1: Basic Concepts

#### 1. Basic Producer
Sends sample user events to Kafka.

```bash
python 1_basic_producer.py
```

**Concepts covered:**
- Creating a KafkaProducer
- Serializing messages
- Sending messages synchronously and asynchronously
- Callbacks for success/error handling

#### 2. Basic Consumer
Receives messages from a Kafka topic.

```bash
# Consume from user_events topic
python 2_basic_consumer.py

# Or specify a different topic and group
python 2_basic_consumer.py orders my_group
```

**Concepts covered:**
- Creating a KafkaConsumer
- Consumer groups
- Auto offset reset behavior
- Message deserialization
- Partition and offset tracking

### Level 2: Stream Processing

#### 3. Stream Processing with Aggregations
Stateful stream processing with real-time aggregations.

```bash
# Stateful aggregation mode (default)
python 3_stream_processing.py

# Windowed aggregation mode (5-second windows)
python 3_stream_processing.py windowed
```

**Concepts covered:**
- Stateful processing (maintaining counters)
- Stream enrichment
- Windowed aggregations
- Time-based event processing

### Level 3: Microservices Architecture

This demonstrates a complete e-commerce system with multiple services communicating via Kafka.

#### Architecture:
```
Order Producer → Orders Topic → Payment Service → Payments Topic → Notification Service
                                                                  ↓
                                                          (sends notifications)
```

#### Setup Instructions:

**Terminal 1 - Start the Payment Service:**
```bash
python 4_payment_service.py
```

**Terminal 2 - Start the Notification Service:**
```bash
python 5_notification_service.py
```

**Terminal 3 - Produce Orders:**
```bash
# Produce 10 orders with 2-second intervals
python 3b_producer_orders.py 10 2

# Or customize:
# python 3b_producer_orders.py <num_orders> <interval_seconds>
python 3b_producer_orders.py 20 1
```

**What happens:**
1. Order producer creates orders and sends them to the `orders` topic
2. Payment service consumes orders, processes payments, and publishes results to `payments` topic
3. Notification service consumes from both `orders` and `payments` topics and sends notifications

## Kafka Concepts Explained

### Topics
- A named stream of events/messages
- Immutable and append-only
- Messages are stored with their offset (position in the log)

### Partitions
- Topics are split into partitions for parallelism
- Each partition maintains message ordering
- Different partitions can be processed in parallel

### Consumer Groups
- Multiple consumers can form a group
- Each partition is consumed by exactly one consumer in a group
- Enables horizontal scaling

### Offsets
- Position of a consumer in a partition
- Tracks which messages have been processed
- Can be reset to replay messages

### Brokers
- Kafka servers that store and serve messages
- Usually run as a cluster for fault tolerance

### Replication
- Messages are replicated across multiple brokers
- Provides durability and fault tolerance

## Useful Kafka CLI Commands

```bash
# Create a topic
kafka-topics --bootstrap-server localhost:9092 \
  --create --topic my_topic --partitions 3 --replication-factor 1

# List topics
kafka-topics --bootstrap-server localhost:9092 --list

# Describe a topic
kafka-topics --bootstrap-server localhost:9092 --describe --topic orders

# Delete a topic
kafka-topics --bootstrap-server localhost:9092 --delete --topic my_topic

# View messages in a topic (from the beginning)
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic orders --from-beginning

# Monitor consumer group
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group payment_service --describe

# Reset consumer group offset
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group analytics --reset-offsets --to-earliest \
  --topic user_events --execute
```

## Key Python Libraries Used

- **kafka-python**: Python client for Kafka
- **pydantic**: Data validation (for future enhanced examples)
- **python-dotenv**: Environment configuration

## Common Issues & Troubleshooting

### "Connection refused" error
- Ensure Kafka is running: `brew services list | grep kafka`
- Restart Kafka if needed: `brew services restart kafka`
- Check `localhost:9092` is accessible

### "Topic does not exist"
- Create the topic using kafka-topics command (see above)
- Or use `auto.create.topics.enable=true` in broker config (not recommended for production)

### Consumer doesn't receive messages
- Check the consumer group offset: `kafka-consumer-groups --bootstrap-server localhost:9092 --group <group_name> --describe`
- Reset offset if needed: use `--reset-offsets` flag

### No messages from `--from-beginning`
- Ensure producer has sent messages to the topic first
- Check topic has partitions and replicas

## Next Steps for Learning

1. **Explore Offset Management**: Modify consumers to commit offsets manually vs. automatically
2. **Implement Dead Letter Queues**: Handle failed message processing
3. **Add Error Handling**: Implement retry logic with exponential backoff
4. **Message Compression**: Enable compression for large payloads
5. **Exactly-Once Semantics**: Implement idempotent producers
6. **Schema Registry**: Use Avro or Protobuf for schema evolution
7. **Kafka Streams**: Migrate stream processing to Kafka Streams library
8. **Docker Compose**: Run Kafka in containers for easier setup

## Resources

- [Apache Kafka Official Documentation](https://kafka.apache.org/documentation/)
- [kafka-python Documentation](https://kafka-python.readthedocs.io/)
- [Kafka Design Principles](https://kafka.apache.org/intro)
- [Confluent Kafka Courses](https://developer.confluent.io/)

## License

MIT
