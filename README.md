# 🍛 Tadka Kafka Demo

**Learn Apache Kafka by building a food delivery app.**

This is the companion code for [The Desi Architect](https://youtube.com/@TheDesiArchitect) YouTube video on Kafka (Paper to Production, Episode 3). Every concept from the video has a runnable demo here. Clone it, run it, break it, learn it.

No prior Kafka experience needed. Just Docker and Node.js.

---

## What You'll Learn

| # | Demo | Kafka Concept |
|---|------|---------------|
| 1 | Topic Setup | Topics, partitions, replication factor |
| 2 | Producer + 4 Consumers | Producers, consumers, event-driven architecture |
| 3 | Consumer Groups | Independent consumption, offset tracking, lag |
| 4 | Partition Key & Ordering | `hash(key) % partitions`, message ordering guarantees |
| 5 | Consumer Scaling | Golden rule: max consumers = num partitions |
| 6 | Hot Partition | Skewed load, compound key fix |
| 7 | Delivery Guarantees | At-most-once, at-least-once, idempotent consumer |
| 8 | Offset Reset & Replay | Replay old messages, Kafka vs RabbitMQ |

All demos use a fictional **Tadka** food delivery app (think Swiggy/Zomato) with orders from cities like Mumbai, Delhi, Bangalore, Pune.

---

## Prerequisites

You need 3 things installed on your machine. That's it.

### 1. Docker Desktop

Kafka runs inside Docker. No manual Kafka installation needed.

- **Windows**: [Download Docker Desktop](https://docs.docker.com/desktop/install/windows-install/) (requires WSL2)
- **Mac**: [Download Docker Desktop](https://docs.docker.com/desktop/install/mac-install/)
- **Linux**: [Install Docker Engine](https://docs.docker.com/engine/install/) + [Docker Compose](https://docs.docker.com/compose/install/)

After install, make sure Docker is **running**. You can verify with:

```bash
docker --version
docker compose version
```

> Docker Compose v2 is required. If `docker compose` doesn't work, you might have the older v1. Upgrade Docker Desktop.

### 2. Node.js (v18 or higher)

All demo scripts are in JavaScript using the [KafkaJS](https://kafka.js.org/) library.

- **Download**: [https://nodejs.org](https://nodejs.org) (pick the LTS version)
- Verify:

```bash
node --version    # should show v18.x.x or higher
npm --version     # should show 9.x.x or higher
```

### 3. A Terminal

- **Windows**: PowerShell (comes pre-installed) or Windows Terminal
- **Mac/Linux**: Any terminal app

Several demos need **multiple terminals open at the same time** (e.g., producer in one, consumers in others). Keep 4-5 terminal tabs ready.

### Ports Used

Make sure these ports are free on your machine:

| Port | Used By |
|------|---------|
| `9092` | Kafka broker |
| `8080` | Kafka UI (web dashboard) |

If port 8080 is taken (common with other apps), edit `docker-compose.yml` and change `"8080:8080"` to something like `"8888:8080"`, then open `http://localhost:8888` instead.

---

## Quick Start

```bash
# Clone the repo
git clone https://github.com/desiarchitect/tadka-kafka-demo.git
cd tadka-kafka-demo

# Start Kafka + Kafka UI (runs in background)
docker compose up -d

# Install Node.js dependencies
npm install

# Create the Kafka topics
npm run setup

# Open Kafka UI in browser
# http://localhost:8080
```

Wait **15-20 seconds** after `docker compose up -d` for Kafka to fully start. The health check ensures Kafka UI won't start until Kafka is ready, but the topics setup (`npm run setup`) might fail if you run it too quickly. If it fails, just wait a few seconds and try again.

### Interactive Runner (Optional)

Instead of running individual commands, use the interactive menu:

**Windows (PowerShell):**
```powershell
.\run-demo.ps1
```

**Mac/Linux:**
```bash
chmod +x run-demo.sh
./run-demo.sh
```

This starts Kafka, installs dependencies, and gives you a menu to pick any demo.

---

## Demo-by-Demo Guide

### Demo 1: Topic & Partition Setup

Creates two topics: `order-events` (3 partitions) and `delivery-guarantee-demo` (1 partition).

```bash
npm run setup
```

**What to check:** Open http://localhost:8080 → Topics. You'll see both topics with their partition count and replication factor.

---

### Demo 2: Producer + Consumer Basics

This is the core demo. A producer sends food orders, and 4 independent consumers process them.

**Terminal 1 - Start the producer (sends 1 random order/second):**
```bash
npm run producer
```

**Terminal 2 - Notification service (sends SMS):**
```bash
npm run notification
```

**Terminal 3 - Analytics service (tracks city-wise revenue):**
```bash
npm run analytics
```

**Terminal 4 - Restaurant service (alerts restaurants):**
```bash
npm run restaurant
```

**Terminal 5 - Search indexer (boosts Elasticsearch scores):**
```bash
npm run search-indexer
```

All 4 consumers receive **every** order because they belong to **different consumer groups**. This is event-driven architecture in action. No service knows about the others.

Stop any consumer with `Ctrl+C`, let orders pile up, restart it. It catches up automatically.

---

### Demo 3: Consumer Groups in Action

No separate script needed. While Demo 2 is running:

1. Open http://localhost:8080 → **Consumer Groups**
2. You'll see `notification-service`, `analytics-service`, `restaurant-service`, `search-indexer`
3. Each group tracks its own offset independently
4. Stop one consumer, watch its **lag** increase
5. Restart it, watch it catch up

---

### Demo 4: Partition Key & Ordering

Shows how Kafka routes messages to partitions using `hash(key) % numPartitions`.

```bash
npm run partition-demo
```

**What you'll see:**
- Part 1: 8 messages with key `mumbai` all go to the **same partition**
- Part 2: Different city keys get distributed across partitions
- Part 3: Partition-wise message count

**Key takeaway:** Ordering is guaranteed **within** a partition, not across partitions.

---

### Demo 5: Consumer Scaling (Golden Rule)

Demonstrates that max useful consumers in a group = number of partitions.

Make sure the producer is running first (`npm run producer` in a separate terminal).

**Terminal 1:**
```bash
node scaling-demo.js 1
```

**Terminal 2:**
```bash
node scaling-demo.js 2
```

**Terminal 3:**
```bash
node scaling-demo.js 3
```

Each instance gets assigned 1 partition. Now try a 4th:

**Terminal 4:**
```bash
node scaling-demo.js 4
```

Instance 4 gets **zero partitions** and sits idle. That's the golden rule: with 3 partitions, the 4th consumer is just wasting resources.

Stop any instance with `Ctrl+C` and watch the others **rebalance**.

---

### Demo 6: Hot Partition Problem

70% of orders come from Mumbai. Since all Mumbai orders share the same key, one partition gets hammered.

```bash
npm run hot-partition
```

**What you'll see:**
- Part 1: A bar chart showing skewed load (one partition at ~70%)
- Part 2: The fix using compound keys (`mumbai_1`, `mumbai_2`, `mumbai_3`) spreads the load evenly

**Key takeaway:** Compound keys trade per-city ordering for better throughput.

---

### Demo 7: Delivery Guarantees

Three sub-demos showing the trade-offs.

#### 7a. At-Most-Once (message may be lost)
```bash
npm run at-most-once
```
Commits offset BEFORE processing. If a crash happens mid-processing, the message is lost forever.

#### 7b. At-Least-Once (message may be duplicated)
```bash
npm run at-least-once
```
Processes BEFORE committing. If a crash happens after processing but before commit, the message gets re-delivered. Customer gets charged twice? Not ideal.

#### 7c. Idempotent Consumer (the fix)
```bash
npm run idempotent
```
Uses `orderId` as a dedup key (in-memory Set for this demo). Skips duplicate messages. In production, you'd use Redis or a database unique constraint for this.

> **Note:** Run these demos one at a time. Each creates its own consumer group, so they won't interfere with each other.

---

### Demo 8: Offset Reset & Replay

Kafka's superpower. Unlike RabbitMQ where messages are gone after acknowledgment, Kafka lets you replay.

Make sure you've run the producer for a while so there are enough messages in `order-events`.

```bash
npm run offset-reset
```

**What you'll see:**
- Phase 1: Consumes 10 messages normally
- Phase 2: Resets offset to 0 via Admin API
- Phase 3: Replays all messages from the beginning

**Use cases:** Bug fix re-processing, building new analytics pipelines, audit trails.

---

## Kafka UI

Open http://localhost:8080 during any demo. It's your visual window into Kafka.

What you can see:
- **Topics** → partition count, replication, config
- **Messages** → browse by partition and offset, see actual JSON payloads
- **Consumer Groups** → active consumers, lag per partition, committed offsets
- **Brokers** → cluster health, configuration

---

## Cleanup

```bash
# Stop Kafka and Kafka UI containers
docker compose down

# Stop AND remove all Kafka data (topics, messages, offsets)
docker compose down -v
```

---

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `npm run setup` fails with connection error | Kafka isn't ready yet. Wait 15-20 seconds after `docker compose up -d` and retry. |
| `docker compose up` fails | Make sure Docker Desktop is running. On Windows, ensure WSL2 is installed. |
| Port 9092 already in use | Another Kafka or service is using it. Stop it or change the port in `docker-compose.yml`. |
| Port 8080 already in use | Change `"8080:8080"` to `"8888:8080"` in `docker-compose.yml`. Access UI at `localhost:8888`. |
| Consumer not receiving messages | Check if the topic exists (`npm run setup`). Check if the producer is running. |
| `node: command not found` | Install Node.js from [nodejs.org](https://nodejs.org). |
| `docker: command not found` | Install Docker Desktop from [docker.com](https://www.docker.com/products/docker-desktop/). |
| Kafka UI shows no consumers | Consumer groups only appear after a consumer has connected at least once. Run a consumer first. |
| Scaling demo: 4th instance gets no data | That's the expected behavior! Max consumers = number of partitions (3). |
| Messages in Kafka UI look empty | Click on a message to expand it. The value is a JSON string. |
| `Error: getaddrinfo ENOTFOUND localhost` | If running inside Docker/WSL, try `host.docker.internal` instead of `localhost`. |

---

## Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Message Broker | Apache Kafka (KRaft mode) | No ZooKeeper needed, single broker setup |
| Kafka Client | [KafkaJS](https://kafka.js.org/) v2.2.4 | Node.js client for Kafka |
| Dashboard | [Kafka UI](https://github.com/provectus/kafka-ui) | Visual inspection of topics, messages, consumer groups |
| Runtime | Node.js 18+ | Runs all demo scripts |
| Infra | Docker Compose | One command to start everything |

---

## File Structure

```
.
├── docker-compose.yml          # Kafka (KRaft mode) + Kafka UI
├── package.json                # Dependencies & npm scripts
├── .gitignore                  # Ignores node_modules, logs
│
├── admin.js                    # Demo 1: Topic creation
├── producer.js                 # Demo 2: Tadka order producer (1 order/sec)
│
├── notification.js             # Demo 2: Consumer - SMS notifications
├── analytics.js                # Demo 2: Consumer - City revenue tracking
├── restaurant.js               # Demo 2: Consumer - Restaurant alerts
├── search-indexer.js           # Demo 2: Consumer - Elasticsearch boost
│
├── partition-demo.js           # Demo 4: Partition key routing & ordering
├── scaling-demo.js             # Demo 5: Consumer scaling (golden rule)
├── hot-partition-demo.js       # Demo 6: Hot partition + compound key fix
│
├── at-most-once.js             # Demo 7a: Commit first, process later
├── at-least-once.js            # Demo 7b: Process first, commit later
├── idempotent-consumer.js      # Demo 7c: Dedup with orderId
│
├── offset-reset-demo.js        # Demo 8: Offset reset & replay
│
├── run-demo.ps1                # Interactive runner (Windows PowerShell)
├── run-demo.sh                 # Interactive runner (Mac/Linux)
└── README.md                   # You are here
```

---

## Contributing

Found a bug? Want to add a demo? PRs are welcome. Keep the code simple, this is meant for learning.

---

## License

MIT. Use it however you want. If it helped you learn Kafka, consider subscribing to [The Desi Architect](https://youtube.com/@TheDesiArchitect).

---

**Built with chai and Kafka by [The Desi Architect](https://youtube.com/@TheDesiArchitect) ☕**
