---
title: "Advanced Kafka Concepts - Mastering Production Kafka"
date: "2025-10-17"
excerpt: "Let's dive deep into advanced concepts that separate hobby projects from production systems."
tags: ["EDA", "Kafka", "Production-Ready", "Event-Driven", "Architecture"]
coverImage: "/images/blog/advanced-kafka.jpg"
---

# Part 5: Advanced Kafka Concepts - Mastering Production Kafka

## Introduction

In Part 4, you built a working Kafka application. Now let's dive deep into advanced concepts that separate hobby projects from production systems.

We'll cover:
- Message delivery semantics (exactly-once)
- Log compaction for state management
- Kafka Streams for real-time processing
- Schema Registry for evolution
- Performance tuning
- Security and multi-datacenter replication

```mermaid
graph TB
    subgraph "Advanced Kafka Topics"
        A[Exactly-Once<br/>Semantics]
        B[Log<br/>Compaction]
        C[Kafka<br/>Streams]
        D[Schema<br/>Registry]
        E[Performance<br/>Tuning]
        F[Security &<br/>Multi-DC]
    end
    
    Basic[Basic Kafka] --> A
    Basic --> B
    Basic --> C
    Basic --> D
    Basic --> E
    Basic --> F
    
    style Basic fill:#95e1d3
    style A fill:#74b9ff
    style B fill:#a29bfe
    style C fill:#fd79a8
    style D fill:#fdcb6e
    style E fill:#00b894
    style F fill:#ff7675
```

## Message Delivery Semantics

### The Three Guarantees

```mermaid
graph TB
    subgraph "At-Most-Once"
        A1[Producer] -->|Fire & Forget| B1[Broker]
        B1 -.->|May be lost| X1[❌ Possible Loss]
        
        Note1[Fast but unsafe<br/>acks=0]
    end
    
    subgraph "At-Least-Once"
        A2[Producer] -->|Wait for ACK| B2[Broker]
        B2 -->|ACK| A2
        A2 -.->|Retry| B2
        C2[Consumer] -->|Process| D2[Business Logic]
        D2 -.->|May duplicate| X2[⚠️ Possible Duplicates]
        
        Note2[Safe but may duplicate<br/>acks=all, retries>0]
    end
    
    subgraph "Exactly-Once"
        A3[Producer] -->|Idempotent| B3[Broker]
        A3 -->|Transactional| B3
        C3[Consumer] -->|Read Committed| D3[Business Logic]
        D3 -->|Exactly Once| E3[✅ Guaranteed Once]
        
        Note3[Safe and no duplicates<br/>enable_idempotence=True]
    end
    
    style X1 fill:#ff7675
    style X2 fill:#fdcb6e
    style E3 fill:#00b894
```

### At-Most-Once (Fire and Forget)

**Configuration:**
```python
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    acks=0,  # Don't wait for acknowledgment
    retries=0  # Don't retry
)

# Send message without waiting
producer.send('orders', value=b'order-data')
# Message might be lost, but we don't care!
```

**Use cases:**
- Metrics collection (losing a few data points is OK)
- Log aggregation (some logs can be lost)
- High-throughput, low-importance data

**Pros:** ⚡ Extremely fast  
**Cons:** ❌ Messages can be lost

### At-Least-Once (Default)

**Producer Configuration:**
```python
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    acks='all',  # Wait for all replicas
    retries=3,  # Retry on failure
    max_in_flight_requests_per_connection=5
)

# Message guaranteed to be written
future = producer.send('orders', value=order_data)
record_metadata = future.get(timeout=10)  # Block until written
```

**Consumer Challenge: Duplicates!**

```python
# Problem: Consumer might process twice
for message in consumer:
    try:
        process_order(message.value)  # Processes the order
        consumer.commit()  # Commits offset
    except Exception as e:
        # If process_order succeeds but commit fails,
        # message will be reprocessed on restart!
        logger.error(f"Error: {e}")
```

**Solution 1: Idempotent Consumer**

```python
import redis

redis_client = redis.Redis()

def process_message_idempotently(message):
    event = message.value
    event_id = event['event_id']
    
    # Check if already processed
    if redis_client.exists(f"processed:{event_id}"):
        logger.info(f"Skipping duplicate: {event_id}")
        return
    
    # Process the event
    process_order(event)
    
    # Mark as processed
    redis_client.setex(
        f"processed:{event_id}",
        86400,  # 24 hour TTL
        '1'
    )
    
    logger.info(f"Processed: {event_id}")

# Consumer loop
for message in consumer:
    process_message_idempotently(message)
    consumer.commit()
```

**Solution 2: Database Deduplication**

```python
from sqlalchemy import Column, String, DateTime, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class ProcessedEvent(Base):
    __tablename__ = 'processed_events'
    
    event_id = Column(String(50), primary_key=True)
    processed_at = Column(DateTime, default=datetime.utcnow)
    order_id = Column(String(50))

engine = create_engine('postgresql://localhost/mydb')
Session = sessionmaker(bind=engine)

def process_with_db_deduplication(message):
    event = message.value
    event_id = event['event_id']
    
    session = Session()
    
    try:
        # Check if processed
        if session.query(ProcessedEvent).filter_by(event_id=event_id).first():
            logger.info(f"Duplicate detected: {event_id}")
            return
        
        # Process in transaction
        process_order(event)
        
        # Mark as processed
        session.add(ProcessedEvent(
            event_id=event_id,
            order_id=event['order_id']
        ))
        session.commit()
        
    except Exception as e:
        session.rollback()
        raise
    finally:
        session.close()
```

### Exactly-Once Semantics (EOS)

The holy grail: messages delivered exactly once, no duplicates, no losses.

```mermaid
sequenceDiagram
    participant Producer
    participant Broker
    participant Consumer
    participant Database
    
    Note over Producer: Idempotent Producer<br/>PID + Sequence Number
    
    Producer->>Broker: Message (PID=1, Seq=0)
    Broker->>Broker: Store with sequence
    Broker-->>Producer: ACK
    
    Producer->>Broker: Message (PID=1, Seq=1)
    Note over Broker: Already has Seq=1<br/>Deduplicate!
    Broker-->>Producer: ACK (already stored)
    
    Note over Producer,Broker: Transactional Writes
    
    Producer->>Broker: Begin Transaction
    Producer->>Broker: Write Message 1
    Producer->>Broker: Write Message 2
    Producer->>Broker: Commit Transaction
    
    Note over Consumer: Read Committed Only
    
    Consumer->>Broker: Read (isolation=read_committed)
    Broker->>Consumer: Only committed messages
    
    Consumer->>Database: Process in transaction
    Consumer->>Broker: Commit offsets (transactionally)
    
    Note over Producer,Database: End-to-End Exactly-Once!
```

**Idempotent Producer:**

```python
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    
    # Enable idempotence
    enable_idempotence=True,
    
    # Required for idempotence
    acks='all',
    retries=2147483647,  # Max retries
    max_in_flight_requests_per_connection=5,
    
    # Serialization
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Producer automatically handles deduplication
# Each producer instance gets a unique PID (Producer ID)
# Each message gets a sequence number
# Broker deduplicates based on (PID, Sequence)

for i in range(10):
    producer.send('orders', {'order_id': f'ORD-{i}'})

producer.flush()
producer.close()
```

**How it works:**

1. **Producer ID (PID)**: Each producer gets unique ID
2. **Sequence Number**: Messages numbered per partition
3. **Broker Deduplication**: Rejects duplicates

```python
# Behind the scenes:
# Message 1: PID=123, Partition=0, Sequence=0
# Message 2: PID=123, Partition=0, Sequence=1
# Message 3: PID=123, Partition=0, Sequence=2

# If network fails and producer retries message 2:
# PID=123, Partition=0, Sequence=1 (again)
# Broker sees: "Already have sequence 1, ignore"
```

**Transactional Producer:**

```python
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    transactional_id='my-transactional-producer',  # Must be unique
    enable_idempotence=True,
    acks='all'
)

# Initialize transactions
producer.init_transactions()

try:
    # Begin transaction
    producer.begin_transaction()
    
    # Send multiple messages atomically
    producer.send('orders', {'order_id': 'ORD-1'})
    producer.send('inventory', {'product_id': 'PROD-1', 'qty': -1})
    producer.send('analytics', {'event': 'order_placed'})
    
    # All or nothing - commit transaction
    producer.commit_transaction()
    
    print("✅ Transaction committed - all messages written atomically")
    
except Exception as e:
    # Rollback on error
    producer.abort_transaction()
    print(f"❌ Transaction aborted: {e}")
```

**Transactional Consumer:**

```python
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'orders',
    bootstrap_servers=['localhost:9092'],
    group_id='order-processor',
    
    # Only read committed transactions
    isolation_level='read_committed',
    
    # Disable auto-commit for manual control
    enable_auto_commit=False
)

for message in consumer:
    event = message.value
    
    try:
        # Process message
        process_order(event)
        
        # Manually commit offset
        consumer.commit()
        
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        # Don't commit - message will be reprocessed
```

**End-to-End Exactly-Once:**

```python
from kafka import KafkaProducer, KafkaConsumer
import psycopg2

class ExactlyOnceProcessor:
    def __init__(self):
        # Consumer
        self.consumer = KafkaConsumer(
            'input-topic',
            bootstrap_servers=['localhost:9092'],
            group_id='processor',
            isolation_level='read_committed',
            enable_auto_commit=False
        )
        
        # Producer
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            transactional_id='processor-producer',
            enable_idempotence=True
        )
        
        # Database
        self.db = psycopg2.connect("dbname=mydb")
        
        self.producer.init_transactions()
    
    def process_exactly_once(self):
        for message in self.consumer:
            try:
                self.producer.begin_transaction()
                
                # 1. Process message
                result = self.process_message(message.value)
                
                # 2. Write to database
                cursor = self.db.cursor()
                cursor.execute(
                    "INSERT INTO orders VALUES (%s, %s)",
                    (result['order_id'], result['amount'])
                )
                self.db.commit()
                
                # 3. Produce output event
                self.producer.send('output-topic', result)
                
                # 4. Commit consumer offsets (within transaction!)
                self.producer.send_offsets_to_transaction(
                    {
                        TopicPartition('input-topic', message.partition): 
                        OffsetAndMetadata(message.offset + 1, None)
                    },
                    self.consumer.config['group_id']
                )
                
                # 5. Commit transaction
                self.producer.commit_transaction()
                
                print(f"✅ Processed exactly once: {result['order_id']}")
                
            except Exception as e:
                self.producer.abort_transaction()
                self.db.rollback()
                print(f"❌ Transaction aborted: {e}")
```

**Performance Impact:**

```mermaid
graph LR
    subgraph "Throughput Comparison"
        A1[At-Most-Once<br/>1M msgs/sec]
        A2[At-Least-Once<br/>800K msgs/sec]
        A3[Exactly-Once<br/>600K msgs/sec]
    end
    
    Note[EOS adds ~20-40% overhead<br/>but guarantees correctness]
    
    style A1 fill:#00b894
    style A2 fill:#fdcb6e
    style A3 fill:#ff7675
```

## Log Compaction

Traditional retention deletes old messages after time/size limits. Log compaction keeps the **latest value for each key** indefinitely.

```mermaid
graph TB
    subgraph without["Without Compaction (Time-based Retention)"]
        A1["t=0: user1 name: Alice"]
        A2["t=1: user2 name: Bob"]
        A3["t=2: user1 name: Alice Updated"]
        A4["t=3: user3 name: Charlie"]
        A5["t=7 days: All deleted ❌"]
    end
    
    subgraph with["With Log Compaction"]
        B1["user1 name: Alice"]
        B2["user2 name: Bob"]
        B3["user1 name: Alice Updated"]
        B4["user3 name: Charlie"]
        
        B5["After Compaction:<br/>user1: Alice Updated<br/>user2: Bob<br/>user3: Charlie"]
        
        B1 --> B5
        B2 --> B5
        B3 --> B5
        B4 --> B5
    end
    
    style A5 fill:#ff7675,stroke:#d63031,color:#fff
    style B5 fill:#00b894,stroke:#00a383,color:#fff
```

### Creating a Compacted Topic

```bash
kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic user-profiles \
  --partitions 3 \
  --replication-factor 3 \
  --config cleanup.policy=compact \
  --config min.cleanable.dirty.ratio=0.5 \
  --config segment.ms=86400000
```

### Using Compacted Topics

**Producer (State Updates):**

```python
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    key_serializer=lambda k: k.encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Update user profile
producer.send(
    'user-profiles',
    key='user-123',  # Key determines which record to keep
    value={
        'user_id': 'user-123',
        'name': 'Alice Smith',
        'email': 'alice@example.com',
        'preferences': {
            'theme': 'dark',
            'notifications': True
        }
    }
)

# Later update
producer.send(
    'user-profiles',
    key='user-123',
    value={
        'user_id': 'user-123',
        'name': 'Alice Smith',
        'email': 'alice.new@example.com',  # Updated email
        'preferences': {
            'theme': 'light',  # Updated theme
            'notifications': True
        }
    }
)

# After compaction, only latest value for 'user-123' remains
```

**Consumer (Rebuild State):**

```python
from kafka import KafkaConsumer, TopicPartition
import json

def rebuild_user_state():
    """Rebuild entire user database from compacted log"""
    
    consumer = KafkaConsumer(
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',  # Read from beginning
        key_deserializer=lambda k: k.decode('utf-8'),
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    
    # Get all partitions
    partitions = consumer.partitions_for_topic('user-profiles')
    topic_partitions = [
        TopicPartition('user-profiles', p) for p in partitions
    ]
    consumer.assign(topic_partitions)
    
    # Rebuild state
    user_state = {}
    
    for message in consumer:
        user_id = message.key
        user_data = message.value
        
        # Latest value wins
        user_state[user_id] = user_data
        
        # Stop when caught up
        if all(consumer.position(tp) >= consumer.end_offsets([tp])[tp] 
               for tp in topic_partitions):
            break
    
    consumer.close()
    
    print(f"✅ Rebuilt state for {len(user_state)} users")
    return user_state

# Rebuild user database
users = rebuild_user_state()
```

**Deleting Keys (Tombstone):**

```python
# Send null value to delete a key
producer.send(
    'user-profiles',
    key='user-123',
    value=None  # Tombstone - deletes the key
)

# After compaction, user-123 is removed from the log
```

### Use Cases for Log Compaction

1. **User Profiles** - Latest profile for each user
2. **Configuration** - Current config for each service
3. **Cache Invalidation** - Latest cache entries
4. **Database Change Data Capture (CDC)** - Latest row state
5. **Materialized Views** - Latest computed results

### Compaction Process

```mermaid
sequenceDiagram
    participant Producer
    participant Active Segment
    participant Older Segments
    participant Compaction Thread
    participant Compacted Segment
    
    Producer->>Active Segment: Write messages
    
    Note over Active Segment: When segment fills<br/>or time expires
    
    Active Segment->>Older Segments: Close & move
    
    Note over Compaction Thread: Periodically scans<br/>older segments
    
    Compaction Thread->>Older Segments: Read all messages
    Compaction Thread->>Compaction Thread: Keep latest per key
    Compaction Thread->>Compacted Segment: Write compacted version
    
    Note over Older Segments: Delete old segments
    
    Note over Compacted Segment: Only latest values remain
```

## Kafka Streams

Kafka Streams is a library for building real-time stream processing applications directly on Kafka.

```mermaid
graph LR
    subgraph "Traditional Architecture"
        K1[Kafka] --> App1[Consume] --> Process1[Process] --> Produce1[Produce] --> K2[Kafka]
        Note1[Complex to manage]
    end
    
    subgraph "Kafka Streams"
        K3[Kafka] --> Streams[Kafka Streams App<br/>Consume + Process + Produce] --> K4[Kafka]
        Note2[Simple & powerful]
    end
    
    style Streams fill:#00b894
```

### Simple Kafka Streams Example (Java)

```java
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import java.util.Properties;

public class OrderProcessingStream {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "order-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        
        StreamsBuilder builder = new StreamsBuilder();
        
        // Input stream
        KStream<String, Order> orders = builder.stream("orders");
        
        // Filter high-value orders
        KStream<String, Order> highValueOrders = orders.filter(
            (key, order) -> order.getAmount() > 1000.0
        );
        
        // Transform
        KStream<String, OrderAlert> alerts = highValueOrders.mapValues(
            order -> new OrderAlert(
                order.getOrderId(),
                order.getAmount(),
                "High value order alert!"
            )
        );
        
        // Output stream
        alerts.to("high-value-order-alerts");
        
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        
        // Graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
```

### Stateless Operations

```java
StreamsBuilder builder = new StreamsBuilder();
KStream<String, Order> orders = builder.stream("orders");

// Filter
orders.filter((key, order) -> order.getStatus().equals("PENDING"));

// Map values
orders.mapValues(order -> order.getAmount());

// FlatMap
orders.flatMapValues(order -> order.getItems());

// Branch (split stream)
KStream<String, Order>[] branches = orders.branch(
    (key, order) -> order.getAmount() < 100,    // Small orders
    (key, order) -> order.getAmount() < 1000,   // Medium orders
    (key, order) -> true                         // Large orders
);
```

### Stateful Operations

**Aggregation:**

```java
// Count orders per customer
KTable<String, Long> orderCountsByCustomer = orders
    .groupBy((key, order) -> order.getCustomerId())
    .count();

// Sum revenue per customer
KTable<String, Double> revenueByCustomer = orders
    .groupBy((key, order) -> order.getCustomerId())
    .aggregate(
        () -> 0.0,  // Initializer
        (customerId, order, aggregate) -> aggregate + order.getAmount()  // Adder
    );
```

**Windowed Aggregation:**

```java
import org.apache.kafka.streams.kstream.TimeWindows;
import java.time.Duration;

// Count orders per customer in 5-minute windows
KTable<Windowed<String>, Long> windowedCounts = orders
    .groupBy((key, order) -> order.getCustomerId())
    .windowedBy(TimeWindows.of(Duration.ofMinutes(5)))
    .count();

// Sum revenue in 1-hour tumbling windows
KTable<Windowed<String>, Double> hourlyRevenue = orders
    .groupBy((key, order) -> order.getCustomerId())
    .windowedBy(TimeWindows.of(Duration.ofHours(1)))
    .aggregate(
        () -> 0.0,
        (customerId, order, sum) -> sum + order.getAmount()
    );
```

```mermaid
graph TB
    subgraph "Tumbling Windows (Non-overlapping)"
        T1[Window 1<br/>00:00-05:00]
        T2[Window 2<br/>05:00-10:00]
        T3[Window 3<br/>10:00-15:00]
        
        T1 --> T2 --> T3
    end
    
    subgraph "Hopping Windows (Overlapping)"
        H1[Window 1<br/>00:00-10:00]
        H2[Window 2<br/>05:00-15:00]
        H3[Window 3<br/>10:00-20:00]
        
        H1 -.->|Overlap| H2
        H2 -.->|Overlap| H3
    end
    
    subgraph "Session Windows (Dynamic)"
        S1[Session 1<br/>User active<br/>10:00-10:15]
        S2[Gap: Inactive]
        S3[Session 2<br/>User active<br/>11:00-11:30]
        
        S1 --> S2 --> S3
    end
    
    style T1 fill:#74b9ff
    style H1 fill:#a29bfe
    style S1 fill:#fd79a8
```

**Joins:**

```java
// Stream-Stream Join
KStream<String, Order> orders = builder.stream("orders");
KStream<String, Payment> payments = builder.stream("payments");

KStream<String, OrderWithPayment> enriched = orders.join(
    payments,
    (order, payment) -> new OrderWithPayment(order, payment),
    JoinWindows.of(Duration.ofMinutes(5))  // Join within 5-minute window
);

// Stream-Table Join (enrichment)
KTable<String, Customer> customers = builder.table("customers");

KStream<String, EnrichedOrder> enrichedOrders = orders.join(
    customers,
    (order, customer) -> new EnrichedOrder(order, customer)
);

// Table-Table Join
KTable<String, Customer> customers = builder.table("customers");
KTable<String, Address> addresses = builder.table("addresses");

KTable<String, CustomerWithAddress> joined = customers.join(
    addresses,
    (customer, address) -> new CustomerWithAddress(customer, address)
);
```

### Complete Kafka Streams Example

```java
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import java.time.Duration;
import java.util.Properties;

public class RealTimeAnalytics {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "analytics");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        
        StreamsBuilder builder = new StreamsBuilder();
        
        // Input streams
        KStream<String, Order> orders = builder.stream("orders");
        KTable<String, Product> products = builder.table("products");
        
        // 1. Enrich orders with product information
        KStream<String, EnrichedOrder> enrichedOrders = orders
            .selectKey((key, order) -> order.getProductId())  // Rekey by product
            .join(
                products,
                (order, product) -> new EnrichedOrder(order, product)
            );
        
        // 2. Calculate revenue per product in 1-hour windows
        KTable<Windowed<String>, Double> revenueByProduct = enrichedOrders
            .groupBy((key, enrichedOrder) -> enrichedOrder.getProductName())
            .windowedBy(TimeWindows.of(Duration.ofHours(1)))
            .aggregate(
                () -> 0.0,
                (productName, enrichedOrder, total) -> 
                    total + enrichedOrder.getAmount()
            );
        
        // 3. Detect high-value customers (>$10,000 in 24 hours)
        KTable<Windowed<String>, Double> customerSpending = enrichedOrders
            .groupBy((key, enrichedOrder) -> enrichedOrder.getCustomerId())
            .windowedBy(TimeWindows.of(Duration.ofHours(24)))
            .aggregate(
                () -> 0.0,
                (customerId, enrichedOrder, total) -> 
                    total + enrichedOrder.getAmount()
            );
        
        KStream<Windowed<String>, Double> highValueCustomers = customerSpending
            .toStream()
            .filter((windowedCustomerId, spending) -> spending > 10000.0);
        
        // 4. Output results
        revenueByProduct
            .toStream()
            .map((windowedProduct, revenue) -> 
                KeyValue.pair(
                    windowedProduct.key(),
                    new RevenueReport(
                        windowedProduct.key(),
                        windowedProduct.window().start(),
                        windowedProduct.window().end(),
                        revenue
                    )
                )
            )
            .to("product-revenue-reports");
        
        highValueCustomers
            .map((windowedCustomer, spending) -> 
                KeyValue.pair(
                    windowedCustomer.key(),
                    new VIPAlert(windowedCustomer.key(), spending)
                )
            )
            .to("vip-customer-alerts");
        
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
```

### Kafka Streams State Stores

```java
// Create state store for caching
StreamsBuilder builder = new StreamsBuilder();

// Add state store
StoreBuilder<KeyValueStore<String, Long>> storeBuilder = 
    Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore("customer-stats"),
        Serdes.String(),
        Serdes.Long()
    );

builder.addStateStore(storeBuilder);

// Use state store in processor
KStream<String, Order> orders = builder.stream("orders");

orders.process(
    () -> new CustomerStatsProcessor("customer-stats"),
    "customer-stats"
);
```

## Schema Registry

Schema Registry manages and validates schemas for Kafka messages.

```mermaid
graph TB
    subgraph "Without Schema Registry"
        P1[Producer v1<br/>Schema A] --> K1[Kafka]
        P2[Producer v2<br/>Schema B] --> K1
        K1 --> C1[Consumer<br/>❌ Schema mismatch!]
        
        Note1[No validation<br/>Runtime errors]
    end
    
    subgraph "With Schema Registry"
        P3[Producer] -->|Register schema| SR[Schema Registry]
        SR -->|Schema ID| P3
        P3 -->|Message + Schema ID| K2[Kafka]
        
        K2 --> C2[Consumer]
        C2 -->|Fetch schema by ID| SR
        SR -->|Return schema| C2
        C2 -->|Validate & deserialize| C2
        
        Note2[Validated<br/>Versioned<br/>Compatible]
    end
    
    style C1 fill:#ff7675
    style SR fill:#00b894
```

### Setting Up Schema Registry

**Docker Compose:**

```yaml
schema-registry:
  image: confluentinc/cp-schema-registry:7.5.0
  depends_on:
    - kafka-1
    - kafka-2
    - kafka-3
  ports:
    - "8081:8081"
  environment:
    SCHEMA_REGISTRY_HOST_NAME: schema-registry
    SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: 'kafka-1:29092'
    SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081
```

### Avro Schema Example

**Define schema (`order-v1.avsc`):**

```json
{
  "type": "record",
  "name": "Order",
  "namespace": "com.example.ecommerce",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "customer_id", "type": "string"},
    {"name": "amount", "type": "double"},
    {"name": "currency", "type": "string", "default": "USD"},
    {"name": "order_date", "type": "long", "logicalType": "timestamp-millis"}
  ]
}
```

**Producer with Schema Registry:**

```python
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

# Load schema
value_schema = avro.load("order-v1.avsc")

# Create producer
producer = AvroProducer({
    'bootstrap.servers': 'localhost:9092',
    'schema.registry.url': 'http://localhost:8081'
}, default_value_schema=value_schema)

# Produce message
producer.produce(
    topic='orders',
    value={
        'order_id': 'ORD-123',
        'customer_id': 'CUST-456',
        'amount': 99.99,
        'currency': 'USD',
        'order_date': int(time.time() * 1000)
    }
)

producer.flush()
```

**Consumer with Schema Registry:**

```python
from confluent_kafka.avro import AvroConsumer

consumer = AvroConsumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'order-processor',
    'schema.registry.url': 'http://localhost:8081'
})

consumer.subscribe(['orders'])

while True:
    msg = consumer.poll(1.0)
    
    if msg is None:
        continue
    
    if msg.error():
        print(f"Error: {msg.error()}")
        continue
    
    # Automatically deserialized using schema from registry
    order = msg.value()
    print(f"Order: {order['order_id']}, Amount: {order['amount']}")
```

### Schema Evolution

**Backward Compatible (v2):**

```json
{
  "type": "record",
  "name": "Order",
  "namespace": "com.example.ecommerce",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "customer_id", "type": "string"},
    {"name": "amount", "type": "double"},
    {"name": "currency", "type": "string", "default": "USD"},
    {"name": "order_date", "type": "long", "logicalType": "timestamp-millis"},
    {"name": "customer_email", "type": ["null", "string"], "default": null}
  ]
}
```

New field with default value - old consumers still work!

**Compatibility Modes:**

```python
import requests

# Set compatibility mode
requests.put(
    'http://localhost:8081/config/orders-value',
    json={'compatibility': 'BACKWARD'}
)

# Compatibility modes:
# - BACKWARD: New schema can read old data
# - FORWARD: Old schema can read new data
# - FULL: Both backward and forward compatible
# - NONE: No compatibility checking
```

## Performance Tuning

### Producer Tuning

```python
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    
    # Batching for throughput
    batch_size=32768,      # 32KB batches
    linger_ms=10,          # Wait up to 10ms to fill batch
    
    # Compression
    compression_type='snappy',  # Fast compression
    # Options: 'gzip', 'snappy', 'lz4', 'zstd'
    
    # Buffer size
    buffer_memory=67108864,  # 64MB buffer
    
    # Network
    max_in_flight_requests_per_connection=5,
    
    # Reliability
    acks='all',
    retries=3
)
```

**Compression Comparison:**

```mermaid
graph TB
    subgraph "Compression Algorithms"
        None[None<br/>1000 KB<br/>Ratio: 1.0<br/>Fast]
        Snappy[Snappy<br/>400 KB<br/>Ratio: 2.5<br/>Very Fast]
        LZ4[LZ4<br/>380 KB<br/>Ratio: 2.6<br/>Very Fast]
        GZIP[GZIP<br/>300 KB<br/>Ratio: 3.3<br/>Slow]
        ZSTD[ZSTD<br/>280 KB<br/>Ratio: 3.6<br/>Medium]
    end
    
    Note[Choose based on:<br/>CPU vs Network tradeoff]
    
    style Snappy fill:#00b894
    style LZ4 fill:#00b894
```

### Consumer Tuning

```python
consumer = KafkaConsumer(
    'orders',
    bootstrap_servers=['localhost:9092'],
    group_id='order-processor',
    
    # Fetch settings
    fetch_min_bytes=1024,           # Wait for 1KB minimum
    fetch_max_wait_ms=500,          # But no more than 500ms
    max_partition_fetch_bytes=1048576,  # 1MB per partition
    
    # Processing
    max_poll_records=500,           # Fetch 500 records per poll
    max_poll_interval_ms=300000,    # 5 minutes max processing time
    
    # Session management
    session_timeout_ms=10000,       # 10 second timeout
    heartbeat_interval_ms=3000,     # Heartbeat every 3 seconds
    
    # Auto-commit
    enable_auto_commit=True,
    auto_commit_interval_ms=5000
)
```

### Parallel Processing

```python
from concurrent.futures import ThreadPoolExecutor
import threading

consumer = KafkaConsumer('orders', ...)
executor = ThreadPoolExecutor(max_workers=10)

def process_message(message):
    """Process message in thread pool"""
    try:
        order = message.value
        process_order(order)
        return True
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        return False

for message in consumer:
    # Submit to thread pool
    future = executor.submit(process_message, message)
    
    # Don't wait - continue consuming
```

### Broker Tuning

**server.properties:**

```properties
# Network threads
num.network.threads=8

# I/O threads
num.io.threads=16

# Replication
replica.lag.time.max.ms=30000

# Log settings
log.segment.bytes=1073741824  # 1GB segments
log.retention.hours=168  # 7 days

# Compression
compression.type=producer  # Use producer's compression

# Flush settings (usually leave to OS)
log.flush.interval.messages=10000
log.flush.interval.ms=1000
```

## Security

### SSL Encryption

**Generate certificates:**

```bash
# Create CA
openssl req -new -x509 -keyout ca-key -out ca-cert -days 365

# Create broker keystore
keytool -keystore kafka.server.keystore.jks -alias localhost \
  -validity 365 -genkey -keyalg RSA

# Sign certificate
keytool -keystore kafka.server.keystore.jks -alias localhost \
  -certreq -file cert-file

openssl x509 -req -CA ca-cert -CAkey ca-key -in cert-file \
  -out cert-signed -days 365 -CAcreateserial

# Import to keystore
keytool -keystore kafka.server.keystore.jks -alias CARoot \
  -import -file ca-cert

keytool -keystore kafka.server.keystore.jks -alias localhost \
  -import -file cert-signed
```

**Broker configuration:**

```properties
listeners=SSL://localhost:9093
ssl.keystore.location=/var/ssl/kafka.server.keystore.jks
ssl.keystore.password=password
ssl.key.password=password
ssl.truststore.location=/var/ssl/kafka.server.truststore.jks
ssl.truststore.password=password
```

**Producer/Consumer:**

```python
producer = KafkaProducer(
    bootstrap_servers=['localhost:9093'],
    security_protocol='SSL',
    ssl_cafile='/path/to/ca-cert',
    ssl_certfile='/path/to/client-cert',
    ssl_keyfile='/path/to/client-key'
)
```

### SASL Authentication

**Broker:**

```properties
listeners=SASL_SSL://localhost:9093
security.inter.broker.protocol=SASL_SSL
sasl.mechanism.inter.broker.protocol=PLAIN
sasl.enabled.mechanisms=PLAIN
```

**Client:**

```python
producer = KafkaProducer(
    bootstrap_servers=['localhost:9093'],
    security_protocol='SASL_SSL',
    sasl_mechanism='PLAIN',
    sasl_plain_username='alice',
    sasl_plain_password='password',
    ssl_cafile='/path/to/ca-cert'
)
```

### ACLs (Access Control Lists)

```bash
# Grant read access
kafka-acls --bootstrap-server localhost:9092 \
  --add \
  --allow-principal User:alice \
  --operation Read \
  --topic orders

# Grant write access
kafka-acls --bootstrap-server localhost:9092 \
  --add \
  --allow-principal User:bob \
  --operation Write \
  --topic orders

# List ACLs
kafka-acls --bootstrap-server localhost:9092 \
  --list \
  --topic orders
```

## Multi-Datacenter Replication

### MirrorMaker 2

```mermaid
graph LR
    subgraph "DC1 (US-East)"
        P1[Producers] --> K1[Kafka Cluster]
        K1 --> C1[Consumers]
    end
    
    subgraph "DC2 (US-West)"
        K2[Kafka Cluster] --> C2[Consumers]
        P2[Producers] --> K2
    end
    
    MM[MirrorMaker 2]
    K1 <-->|Replicate| MM
    MM <-->|Replicate| K2
    
    style MM fill:#95e1d3
```

**Configuration:**

```properties
# mm2.properties
clusters = us-east, us-west

us-east.bootstrap.servers = east-kafka:9092
us-west.bootstrap.servers = west-kafka:9092

# Replication flows
us-east->us-west.enabled = true
us-west->us-east.enabled = true

# Topics to replicate
us-east->us-west.topics = orders, payments
us-west->us-east.topics = orders, payments

# Heartbeats and checkpoints
emit.heartbeats.enabled = true
emit.checkpoints.enabled = true
```

**Start MirrorMaker:**

```bash
connect-mirror-maker.sh mm2.properties
```

## Key Takeaways

✅ **Exactly-Once Semantics** - Guaranteed no duplicates with idempotence and transactions  
✅ **Log Compaction** - Keep latest state per key indefinitely  
✅ **Kafka Streams** - Build real-time processing apps with stateful operations  
✅ **Schema Registry** - Manage schema evolution safely  
✅ **Performance Tuning** - Optimize for your workload  
✅ **Security** - SSL, SASL, ACLs for production  
✅ **Multi-DC** - Replicate across datacenters  

## Next Steps

In [Part 6](part-06-advanced-patterns), we'll explore advanced event-driven patterns:
- CQRS (Command Query Responsibility Segregation)
- Event Sourcing implementations
- Saga Pattern for distributed transactions
- Outbox and Inbox patterns
- Building a complete system

You now have the tools for production Kafka! 🚀
