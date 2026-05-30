# 🍛 Tadka Kafka Demo - Complete File Explanation

## Overview

The **Tadka Kafka Demo** teaches Apache Kafka through a fictional food delivery app. Instead of theory, you see real producer-consumer patterns with order events flowing through notification, analytics, restaurant, and search services—all decoupled via Kafka.

---

## 🏗️ Architecture Overview

```
Order Service (Producer)
        ↓ (publishes order events to Kafka)
    ┌───────────────┐
    │   KAFKA       │
    │ order-events  │
    │ (3 partitions)│
    └───────────────┘
    ↙  ↓  ↓  ↓  ↘
Notification Analytics Restaurant Search Indexer
   Service    Service    Service     Service
```

**Key Principle**: One producer publishes → Multiple independent consumers process at their own pace → Each maintains its own offset.

---

## 📁 Files by Category

### **Category 1: Infrastructure & Setup**

#### `admin.js` — Topic Setup
- **Purpose**: Creates Kafka topics and partitions before running demos
- **Concept**: Topic administration
- **Run**: `npm run setup`
- **Output**: Confirms topics created (`order-events` with 3 partitions, plus isolated delivery guarantee demo topics)
- **Production**: DevOps runs this once during infrastructure setup

#### `docker-compose.yml` — Local Kafka Cluster
- **Purpose**: Spins up a Kafka broker and Kafka UI in Docker
- **Concept**: Kafka deployment, broker configuration
- **Run**: `docker-compose up` (in background)
- **Features**: 
  - Kafka broker on port 9092
  - Kafka UI dashboard on http://localhost:8080
  - Health checks and proper configuration
- **Production**: Use AWS MSK, Confluent Cloud, or bare-metal clusters

---

### **Category 2: Core Producer & Consumers (Basic Demo)**

These 5 files demonstrate event-driven decoupled architecture.

#### `producer.js` — Order Service (Producer)
- **Purpose**: Generates order events continuously
- **Concept**: Producers, partition keys, fire-and-forget publishing
- **Run**: `npm run producer`
- **Output**: Prints order #ID | city | amount every 1 second
- **Key Detail**: Uses `city` as partition key → same city always goes to same partition → ordering guaranteed per city
- **Production**: Your Order API after saving order to database

#### `notification.js` — SMS Consumer
- **Purpose**: Sends SMS confirmations when orders arrive
- **Concept**: Consumer groups, independent consumption
- **Run**: `npm run notification`
- **Output**: "📱 SMS sent: Order #X confirmed! ₹Y"
- **Consumer Group**: `notification-service`
- **Production**: Integrates with Twilio, AWS SNS, or similar

#### `analytics.js` — Revenue Tracker
- **Purpose**: Aggregates orders by city for business intelligence
- **Concept**: Independent consumer group, state aggregation
- **Run**: `npm run analytics`
- **Output**: "📊 [ClickHouse] mumbai revenue: ₹450"
- **Consumer Group**: `analytics-service` (independent from notification)
- **Production**: Sends to ClickHouse, Redshift, or data warehouse

#### `restaurant.js` — Restaurant Alert Service
- **Purpose**: Alerts restaurants when orders arrive
- **Concept**: Multiple consumers on same topic
- **Run**: `npm run restaurant`
- **Output**: "🍽 Restaurant alert: Order #X | city | restaurantId"
- **Consumer Group**: `restaurant-service`
- **Production**: Pushes to restaurant app via WebSocket or push notifications

#### `search-indexer.js` — Popularity Tracker
- **Purpose**: Boosts restaurant search scores based on orders
- **Concept**: Read-side projections, CQRS pattern
- **Run**: `npm run search-indexer`
- **Output**: "🔍 [Elasticsearch] Boosting score: rest_23"
- **Consumer Group**: `search-indexer`
- **Pattern**: Eventual consistency—search index updates after order event processed
- **Production**: Updates Elasticsearch or similar search engine

---

### **Category 3: Partition & Load Balancing**

#### `partition-demo.js` — Partition Key & Ordering
- **Purpose**: Shows how partition keys determine message routing
- **Concept**: Hash(key) % partitions, message ordering guarantees
- **Run**: `npm run partition-demo`
- **Part 1 Output**: 8 orders with key="mumbai" all go to same partition
  ```
  Order #1 | key=mumbai → Partition 1 | Offset 0
  Order #2 | key=mumbai → Partition 1 | Offset 1
  ```
- **Part 2 Output**: Different cities distributed across partitions
- **Key Formula**: `partition = hash(key) % numPartitions`
- **Learning**: Partition count = max consumers you'll ever need

#### `hot-partition-demo.js` — Load Balancing Problem & Solution
- **Purpose**: Demonstrates uneven load and how to fix it
- **Concept**: Hot partition problem, compound keys
- **Run**: `npm run hot-partition`
- **Part 1**: 70% orders from Mumbai
  ```
  Partition 0: 70 messages (HOT! 🔥)
  Partition 1: 15 messages
  Partition 2: 15 messages
  ```
- **Part 2**: Uses bucketed Mumbai keys instead of just city
  ```
  Partition 0: 34 messages (BALANCED ✅)
  Partition 1: 33 messages
  Partition 2: 33 messages
  ```
- **Solution**: Change key strategy based on distribution skew
- **Real-world**: Celebrity restaurant overload? Use a key that spreads load while preserving the ordering your business actually needs

---

### **Category 4: Delivery Guarantees**

These demonstrate tradeoffs between message loss and duplication.

#### `at-most-once.js` — Commit Before Processing
- **Purpose**: Demonstrates losing messages during processing
- **Concept**: Delivery guarantees, offset commits
- **Run**: `npm run at-most-once`
- **Flow**: 
  1. Commit offset FIRST
  2. Process message
  3. If crash during processing → Message lost forever
- **Output Shows**: 
  ```
  ✅ Offset committed
  💥 CRASH while processing!
     Offset was committed — message is LOST
  ```
- **Trade-off**: Never duplicate, but may lose
- **Use Cases**: Analytics (slight inaccuracy acceptable), metrics, logging
- **❌ Don't use for**: Payments, orders, critical transactions

#### `at-least-once.js` — Process Before Commit
- **Purpose**: Demonstrates message duplication
- **Concept**: Manual offset management, duplicate handling
- **Run**: `npm run at-least-once`
- **Flow**:
  1. Process message
  2. Commit offset AFTER
  3. If crash between processing and commit → Message reprocessed (duplicate)
- **Output Shows**: Demonstrates crash scenario → message gets reprocessed
- **Trade-off**: May duplicate, but never loses
- **Use Cases**: Notifications (duplicate SMS is okay), orders, important events
- **⚠️ Requires**: Idempotent downstream processing (see next)

#### `idempotent-consumer.js` — Deduplication Pattern
- **Purpose**: Handles duplicates gracefully
- **Concept**: Idempotent processing, deduplication
- **Run**: `npm run idempotent`
- **Technique**: Maintain set of processed IDs; skip if already seen
- **Input**: 7 messages with 3 intentional duplicates
  ```
  Orders: 201, 202, 201 (dup), 203, 202 (dup), 204, 201 (dup!)
  ```
- **Output Shows**:
  ```
  ✅ Processing Order #201
  ✅ Processing Order #202
  ⏭️  Order #201 already processed — SKIPPED
  ✅ Processing Order #203
  ⏭️  Order #202 already processed — SKIPPED
  ✅ Processing Order #204
  ⏭️  Order #201 already processed — SKIPPED
  
  Result: 4 unique processed, 3 duplicates skipped
  ```
- **Key Code**:
  ```javascript
  if (processedIds.has(id)) return; // Skip duplicate
  processedIds.add(id);             // Mark as seen
  ```
- **Production**: Use Redis or database instead of in-memory Set
- **Combines**: at-least-once delivery + idempotent consumer = exactly-once semantics
- **Critical For**: Payments, inventory, financial operations

---

### **Category 5: Advanced Operations**

#### `offset-reset-demo.js` — Replay Messages
- **Purpose**: Shows Kafka's ability to replay messages
- **Concept**: Offset management, log immutability, replay capability
- **Run**: `npm run offset-reset`
- **Flow**:
  - Phase 1: Consume 10 messages normally (committed offsets move forward per partition)
  - Phase 2: Reset offset back to 0
  - Phase 3: Replay same 10 messages from beginning
- **Key Insight**: Kafka doesn't delete data; it just remembers where you are
- **Comparison**: 
  - RabbitMQ: Deletes messages after consuming
  - Kafka: Keeps messages; consumer remembers position
- **Use Cases**: 
  - Disaster recovery (new service version crashes? Replay to recompute)
  - Analytics rebuild (recompute all revenue reports)
  - Debugging (did we lose an order? Replay to check)

---

## 🎯 Recommended Execution Order

**Session 1: Basic Architecture (30 minutes)**
1. `docker-compose up` — Start Kafka
2. `npm run setup` — Create topics
3. `npm run producer` (Terminal 1) — Start orders
4. `npm run notification` (Terminal 2) — First consumer
5. Add consumers one by one: `analytics`, `restaurant`, `search-indexer`
6. Visit `http://localhost:8080` — Watch Kafka UI show message flow
7. Stop everything gracefully

**Session 2: Partitioning (20 minutes)**
1. `npm run partition-demo` — Understand key → partition routing
2. `npm run hot-partition` — Learn compound key fix
3. Analyze the output to understand load distribution

**Session 3: Delivery Guarantees (20 minutes)**
1. `npm run at-most-once` — See message loss scenario
2. `npm run at-least-once` — See duplicate scenario
3. `npm run idempotent` — See how dedup handles duplicates

**Session 4: Advanced (10 minutes)**
1. `npm run offset-reset` — Understand replay capability

---

## 📊 Quick Reference

| File | Command | Concept | Time |
|------|---------|---------|------|
| admin.js | `setup` | Topic creation | 2s |
| producer.js | `producer` | Producers, keys | ∞ |
| notification.js | `notification` | Consumers | ∞ |
| analytics.js | `analytics` | Independent consumers | ∞ |
| restaurant.js | `restaurant` | Multiple consumers | ∞ |
| search-indexer.js | `search-indexer` | Decoupled architecture | ∞ |
| partition-demo.js | `partition-demo` | Partition routing | 5s |
| hot-partition-demo.js | `hot-partition` | Load balancing | 3s |
| at-most-once.js | `at-most-once` | Delivery guarantee | 3s |
| at-least-once.js | `at-least-once` | Duplicate handling | 3s |
| idempotent-consumer.js | `idempotent` | Deduplication | 3s |
| offset-reset-demo.js | `offset-reset` | Replay capability | 5s |

---

## 🎓 After Running These Demos, You Can Explain:

✅ Why Kafka decouples services (notification failure won't crash orders)  
✅ How partition keys ensure ordering within a logical stream  
✅ Why compound keys solve hot partition problems  
✅ The tradeoff between message loss (at-most-once) vs duplication (at-least-once)  
✅ How idempotent consumers achieve exactly-once semantics  
✅ Why Kafka is different from RabbitMQ (immutable log, replaying)  
✅ Real-world patterns: producer-consumer, event-driven architecture, CQRS  

You're now ready to architect event-driven systems! 🚀
