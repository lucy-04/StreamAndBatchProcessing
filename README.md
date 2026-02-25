# Unified Batch & Streaming Transaction Pipeline

A production-grade **Apache Spark** application (Scala) that seamlessly bridges real-time and historical data processing in a single, long-lived JVM process. Uses **Apache Kafka** for streaming ingestion, **Delta Lake** for ACID-compliant storage, and **Pekko HTTP** for a REST API — all sharing one `SparkSession` with **FAIR Scheduler** for concurrent resource management.

---

## Architecture Overview

```
Kafka (transactions-stream)
        │
        ▼
┌──────────────────────────────────────────┐
│         Spark Driver (JVM Process)       │
│                                          │
│  ┌──────────────────────────────────┐    │
│  │   Pekko HTTP Server (:8080)      │    │
│  │   /stream/* /batch/* /health     │    │
│  └──────────┬───────────────────────┘    │
│             │                            │
│  ┌──────────▼───────────────────────┐    │
│  │   SparkSession (FAIR Scheduler)  │    │
│  │                                  │    │
│  │  Streaming Query    Batch Jobs   │    │
│  │  Kafka → Delta      Delta → Agg  │    │
│  │  (streaming-pool)   (batch-pool) │    │
│  └──────────┬──────────┬────────────┘    │
│             │          │                 │
│  ┌──────────▼──────────▼────────────┐    │
│  │       Delta Lake (ACID)          │    │
│  │  stream/transactions/  (source)  │    │
│  │  batches/{id}/raw/     (results) │    │
│  │  batches/{id}/aggregated/        │    │
│  └──────────────────────────────────┘    │
└──────────────────────────────────────────┘
```

**Key design**: The streaming Delta table (`stream/transactions/`) is the **single source of truth** — streaming writes to it, batch reads from it.

---

## Prerequisites

| Tool | Version | Install |
|------|---------|---------|
| Docker | 24+ | [docs.docker.com/get-docker](https://docs.docker.com/get-docker/) |
| Docker Compose | v2+ | Included with Docker Desktop; or `apt install docker-compose-plugin` |
| uv | Latest | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| Java | 17+ | Only needed for local (non-Docker) development |
| sbt | 1.11+ | Only needed for local (non-Docker) development |

---

## Project Structure

```
StreamAndBatchProcessing/
├── build.sbt                              ← SBT build (Spark, Pekko, Delta, Circe)
├── project/
│   ├── build.properties                   ← sbt.version=1.11.6
│   └── plugins.sbt                        ← sbt-assembly 2.1.5
├── src/main/
│   ├── scala/com/pipeline/
│   │   ├── Main.scala                     ← Entry point: SparkSession + HTTP server
│   │   ├── SparkManager.scala             ← Singleton SparkSession (FAIR + Delta)
│   │   ├── config/AppConfig.scala         ← Typesafe Config wrapper
│   │   ├── models/
│   │   │   ├── Schemas.scala              ← TransactionEvent StructType (30 fields)
│   │   │   └── BatchJobRecord.scala       ← Batch job lifecycle tracking
│   │   ├── registry/BatchRegistry.scala   ← ConcurrentHashMap for batch tracking
│   │   ├── analytics/AnalysisAggregator.scala ← 9 Spark aggregation functions
│   │   ├── jobs/
│   │   │   ├── StreamingJob.scala         ← Kafka → Delta Lake streaming
│   │   │   └── BatchJob.scala             ← Delta → Analysis → Delta batch
│   │   └── routes/
│   │       ├── StreamRoutes.scala         ← /stream/start, /stream/stop, /stream/status
│   │       ├── BatchRoutes.scala          ← /batch/run, /batch/status, /batch/data, /batch/list
│   │       └── ControlRoutes.scala        ← /health, /stop
│   └── resources/application.conf         ← Default config
├── conf/
│   ├── application.conf                   ← Production config (env var overrides)
│   └── fairscheduler.xml                  ← FAIR scheduler pools
├── scripts/
│   ├── pyproject.toml                     ← UV Python project config
│   ├── kafka_stream_generator.py          ← Realistic Kafka event producer
│   └── delta_lake_seeder.py               ← Historical data seeder (PySpark)
├── docker/Dockerfile                      ← Production container image
├── docker-compose.yml                     ← Full stack: Kafka + ZK + Kafka UI + Pipeline
└── AGENTS.md                              ← Full architecture specification
```

---

## Quick Start with Docker (Recommended)

This is the fastest way to get the full stack running. Docker Compose brings up Kafka, Zookeeper, Kafka UI, and the Spark pipeline in one command.

### 1. Build the Fat JAR

```bash
sbt clean assembly
```

This produces `target/scala-2.13/UnifiedTransactionPipeline-assembly-1.0.0.jar` (~111 MB).

### 2. Start the Full Stack

```bash
docker compose up -d --build
```

This builds the pipeline Docker image and starts all services:

| Service | Port | Description |
|---------|------|-------------|
| **pipeline** | [localhost:8080](http://localhost:8080) | REST API (Pekko HTTP) |
| **pipeline** | [localhost:4040](http://localhost:4040) | Spark UI |
| **kafka** | `localhost:9092` | Kafka broker (host access) |
| **kafka-ui** | [localhost:8090](http://localhost:8090) | Kafka topic browser |
| **zookeeper** | `localhost:2181` | Kafka coordination |

Verify everything is healthy:

```bash
docker compose ps
```

You should see all containers in `Up` or `Up (healthy)` state.

### 3. Seed the Kafka Topic with Events

The `transactions-stream` topic is auto-created when the first message arrives. Start the generator in a **separate terminal**:

```bash
cd scripts/
uv sync
uv run python kafka_stream_generator.py
```

This produces ~10 events/sec. Let it run for 30+ seconds to accumulate data, then proceed. Customize with:

```bash
EVENTS_PER_SECOND=50 uv run python kafka_stream_generator.py
```

> **Important:** The Kafka topic must exist (have at least one message) before starting the streaming queries. If you start streaming before any messages are produced, the queries will fail with `UnknownTopicOrPartitionException`. Simply restart streaming after the generator has sent its first batch.

### 4. (Optional) Seed Delta Lake with Historical Data

For batch processing to return results immediately (without waiting for streaming to accumulate data), seed Delta Lake with historical records:

```bash
cd scripts/
uv run python delta_lake_seeder.py
```

This writes ~1M records spanning 90 days to `data/delta/stream/transactions/`. Custom settings:

```bash
TOTAL_RECORDS=5000000 DATE_RANGE_DAYS=180 uv run python delta_lake_seeder.py
```

### 5. Interact with the API

#### Monitoring Dashboards (Quick Reference)

Before interacting with the API, you can access these UIs for real-time monitoring:

| Dashboard | URL | Purpose |
|---|---|---|
| **Spark UI** | [http://localhost:4040](http://localhost:4040) | Monitor all Spark jobs (streaming + batch), FAIR scheduler pools, task execution, SQL query plans |
| **Kafka UI** | [http://localhost:8090](http://localhost:8090) | Monitor Kafka topics, consumer lag, message throughput, partition distribution |
| **REST API Health** | [http://localhost:8080/health](http://localhost:8080/health) | Application health check and uptime |

💡 **Tip**: Keep the Spark UI open in a browser tab while running batch queries to see them execute in real-time in the `batch-pool` scheduler while streaming continues in the `streaming-pool`.

---

**Start streaming** (Kafka → Delta Lake):
```bash
curl -X POST http://localhost:8080/stream/start \
  -H "Content-Type: application/json" \
  -d '{"topic": "transactions-stream"}'
```

**Check streaming status:**
```bash
curl -s http://localhost:8080/stream/status | python3 -m json.tool
```

**Submit a batch job** (reads from Delta Lake):
```bash
curl -X POST http://localhost:8080/batch/run \
  -H "Content-Type: application/json" \
  -d '{
    "startDate": "2025-01-01",
    "endDate": "2025-07-01",
    "analysisType": "revenue_by_category"
  }'
# Returns: {"batchId": "batch-...", "status": "PENDING"}
```

**Submit with filters:**
```bash
curl -X POST http://localhost:8080/batch/run \
  -H "Content-Type: application/json" \
  -d '{
    "startDate": "2025-01-01",
    "endDate": "2025-07-01",
    "analysisType": "customer_segmentation",
    "filters": {
      "category": "electronics",
      "region": "west"
    }
  }'
```

**Check batch status:**
```bash
curl -s http://localhost:8080/batch/status/<batchId> | python3 -m json.tool
```

**Get raw batch data** (paginated):
```bash
curl -s "http://localhost:8080/batch/data/<batchId>?limit=50&offset=0" | python3 -m json.tool
```

**Get aggregated results:**
```bash
curl -s http://localhost:8080/batch/data/<batchId>/aggregated | python3 -m json.tool
```

**List all batches:**
```bash
curl -s "http://localhost:8080/batch/list?status=COMPLETED" | python3 -m json.tool
```

**Health check:**
```bash
curl -s http://localhost:8080/health | python3 -m json.tool
```

**Stop streaming:**
```bash
curl -X POST http://localhost:8080/stream/stop
```

**Graceful shutdown:**
```bash
curl -X POST http://localhost:8080/stop
```

---

## Example Batch Queries (Based on Seeded Data)

After running the `delta_lake_seeder.py` script (which seeds 90 days of historical data by default), you can run these realistic batch queries that will return non-zero results:

### 1. Revenue Analysis by Category (High Volume)

Query the **last 30 days** of all transactions:

```bash
curl -X POST http://localhost:8080/batch/run \
  -H "Content-Type: application/json" \
  -d "{
    \"startDate\": \"$(date -u -d '30 days ago' +%Y-%m-%d)\",
    \"endDate\": \"$(date -u +%Y-%m-%d)\",
    \"analysisType\": \"revenue_by_category\"
  }"
```

Expected result: ~333,000+ records across all 10 categories (electronics, clothing, grocery, home_garden, sports, beauty, automotive, books, toys, pharmacy).

### 2. Electronics in West Region (Filtered Query)

Query **electronics** sales in the **west region** (highest weight: 28%) for the last 60 days:

```bash
curl -X POST http://localhost:8080/batch/run \
  -H "Content-Type: application/json" \
  -d "{
    \"startDate\": \"$(date -u -d '60 days ago' +%Y-%m-%d)\",
    \"endDate\": \"$(date -u +%Y-%m-%d)\",
    \"analysisType\": \"revenue_by_category\",
    \"filters\": {
      \"category\": \"electronics\",
      \"region\": \"west\"
    }
  }"
```

Expected result: ~37,000+ electronics transactions from Mumbai, Pune, Ahmedabad, and Goa.

### 3. Customer Segmentation in South Region

Segment customers in the **south region** (25% weight) for the last 45 days:

```bash
curl -X POST http://localhost:8080/batch/run \
  -H "Content-Type: application/json" \
  -d "{
    \"startDate\": \"$(date -u -d '45 days ago' +%Y-%m-%d)\",
    \"endDate\": \"$(date -u +%Y-%m-%d)\",
    \"analysisType\": \"customer_segmentation\",
    \"filters\": {
      \"region\": \"south\"
    }
  }"
```

Expected result: ~125,000+ transactions from Bangalore, Chennai, Hyderabad, and Kochi.

### 4. Fraud Analysis (Last 90 Days)

Analyze fraudulent transactions (2% fraud rate) across the full seeded dataset:

```bash
curl -X POST http://localhost:8080/batch/run \
  -H "Content-Type: application/json" \
  -d "{
    \"startDate\": \"$(date -u -d '90 days ago' +%Y-%m-%d)\",
    \"endDate\": \"$(date -u +%Y-%m-%d)\",
    \"analysisType\": \"fraud_analysis\"
  }"
```

Expected result: ~20,000+ fraudulent transactions (2% of 1M records).

### 5. Payment Method Analysis for Credit Cards

Analyze **credit card** payment trends (30% of all payments):

```bash
curl -X POST http://localhost:8080/batch/run \
  -H "Content-Type: application/json" \
  -d "{
    \"startDate\": \"$(date -u -d '90 days ago' +%Y-%m-%d)\",
    \"endDate\": \"$(date -u +%Y-%m-%d)\",
    \"analysisType\": \"payment_analysis\",
    \"filters\": {
      \"payment_method\": \"credit_card\"
    }
  }"
```

Expected result: ~300,000+ credit card transactions.

### 6. Mobile App Channel Performance

Analyze **mobile_app** channel (35% weight - highest channel):

```bash
curl -X POST http://localhost:8080/batch/run \
  -H "Content-Type: application/json" \
  -d "{
    \"startDate\": \"$(date -u -d '90 days ago' +%Y-%m-%d)\",
    \"endDate\": \"$(date -u +%Y-%m-%d)\",
    \"analysisType\": \"channel_performance\",
    \"filters\": {
      \"channel\": \"mobile_app\"
    }
  }"
```

Expected result: ~350,000+ mobile app transactions.

### 7. Grocery Category Analysis (Highest Weight: 22%)

Query the most common category - **grocery**:

```bash
curl -X POST http://localhost:8080/batch/run \
  -H "Content-Type: application/json" \
  -d "{
    \"startDate\": \"$(date -u -d '90 days ago' +%Y-%m-%d)\",
    \"endDate\": \"$(date -u +%Y-%m-%d)\",
    \"analysisType\": \"inventory_velocity\",
    \"filters\": {
      \"category\": \"grocery\"
    }
  }"
```

Expected result: ~220,000+ grocery transactions across subcategories (dairy, snacks, beverages, frozen, organic, bakery).

### 8. Hourly Trends for Last Week

Analyze time-of-day patterns for the **last 7 days**:

```bash
curl -X POST http://localhost:8080/batch/run \
  -H "Content-Type: application/json" \
  -d "{
    \"startDate\": \"$(date -u -d '7 days ago' +%Y-%m-%d)\",
    \"endDate\": \"$(date -u +%Y-%m-%d)\",
    \"analysisType\": \"hourly_trends\"
  }"
```

Expected result: ~77,000+ transactions with business hours (8 AM - 10 PM) having 80% of volume.

### 9. Multi-Filter Query: Gold Tier Customers in Electronics

Target **gold tier** customers (15% of customers) buying **electronics** in the **north region**:

```bash
curl -X POST http://localhost:8080/batch/run \
  -H "Content-Type: application/json" \
  -d "{
    \"startDate\": \"$(date -u -d '90 days ago' +%Y-%m-%d)\",
    \"endDate\": \"$(date -u +%Y-%m-%d)\",
    \"analysisType\": \"customer_segmentation\",
    \"filters\": {
      \"category\": \"electronics\",
      \"region\": \"north\",
      \"customer_tier\": \"gold\"
    }
  }"
```

Expected result: ~6,600+ transactions (20% category × 22% region × 15% tier).

### 10. Revenue by Region and City (Geographic Heatmap)

Generate a complete geographic revenue breakdown:

```bash
curl -X POST http://localhost:8080/batch/run \
  -H "Content-Type: application/json" \
  -d "{
    \"startDate\": \"$(date -u -d '90 days ago' +%Y-%m-%d)\",
    \"endDate\": \"$(date -u +%Y-%m-%d)\",
    \"analysisType\": \"revenue_by_region\"
  }"
```

Expected result: 1M+ transactions across 6 regions and 24 cities with detailed revenue breakdown.

### Checking Results

After submitting a batch job, capture the `batchId` from the response:

```bash
# Submit and capture batchId
BATCH_ID=$(curl -s -X POST http://localhost:8080/batch/run \
  -H "Content-Type: application/json" \
  -d "{
    \"startDate\": \"$(date -u -d '30 days ago' +%Y-%m-%d)\",
    \"endDate\": \"$(date -u +%Y-%m-%d)\",
    \"analysisType\": \"revenue_by_category\"
  }" | python3 -c "import sys, json; print(json.load(sys.stdin)['batchId'])")

echo "Batch ID: $BATCH_ID"

# Poll status until completed
watch -n 2 "curl -s http://localhost:8080/batch/status/$BATCH_ID | python3 -m json.tool"

# Once COMPLETED, retrieve aggregated results
curl -s "http://localhost:8080/batch/data/$BATCH_ID/aggregated" | python3 -m json.tool

# Or get raw data with pagination
curl -s "http://localhost:8080/batch/data/$BATCH_ID?limit=10&offset=0" | python3 -m json.tool
```

### Expected Data Volumes (Per 1M Seeded Records)

| Filter Dimension | Expected Volume | Percentage |
|---|---|---|
| **All data** | ~1,000,000 | 100% |
| **Category: electronics** | ~200,000 | 20% |
| **Category: grocery** | ~220,000 | 22% |
| **Category: clothing** | ~180,000 | 18% |
| **Region: west** | ~280,000 | 28% |
| **Region: south** | ~250,000 | 25% |
| **Region: north** | ~220,000 | 22% |
| **Channel: mobile_app** | ~350,000 | 35% |
| **Channel: pos_in_store** | ~250,000 | 25% |
| **Payment: credit_card** | ~300,000 | 30% |
| **Payment: upi** | ~250,000 | 25% |
| **Customer tier: gold** | ~150,000 | 15% |
| **Fraudulent transactions** | ~20,000 | 2% |
| **Transaction status: completed** | ~920,000 | 92% |

---

## Docker Logs & Debugging

### View Pipeline Logs (API calls, Spark activity, errors)

```bash
# Follow logs in real time (Ctrl+C to stop)
docker compose logs -f pipeline

# Last 100 lines
docker compose logs --tail 100 pipeline

# Since a specific time
docker compose logs --since 5m pipeline
```

### View Kafka Logs

```bash
docker compose logs -f kafka
```

### View All Services

```bash
docker compose logs -f
```

### Filter for Specific Events

```bash
# Only errors and warnings
docker compose logs pipeline 2>&1 | grep -E "ERROR|WARN"

# API request/response activity
docker compose logs pipeline 2>&1 | grep -E "POST|GET|route|request"

# Streaming query progress
docker compose logs pipeline 2>&1 | grep -i "microbatch\|streaming\|query"

# Batch job lifecycle
docker compose logs pipeline 2>&1 | grep -i "batch\|PENDING\|RUNNING\|COMPLETED\|FAILED"
```

### Interactive Shell in Pipeline Container

```bash
docker compose exec pipeline bash

# Then inside the container:
ls -la /opt/spark/data/delta/    # Inspect Delta Lake tables
ls -la /opt/spark/jars/          # Verify app jar is present
cat /opt/spark/conf/application.conf  # Check active config
```

### Spark UI — Monitoring Streaming & Batch Jobs

Open [http://localhost:4040](http://localhost:4040) in your browser for comprehensive visibility into all Spark operations.

#### What You Can Monitor:

**Jobs Tab**
- All streaming micro-batches and batch jobs appear as separate Spark jobs
- Job duration, stage count, and completion status
- Identify which scheduler pool each job is running in (streaming-pool vs batch-pool)
- See the FAIR scheduler in action: batch jobs run concurrently with streaming without blocking

**Stages Tab**
- Task-level execution details for each job stage
- Shuffle read/write metrics (critical for join operations)
- Task distribution across executors
- Skew detection: identify if some tasks take much longer than others

**Streaming Tab** (Real-Time Metrics)
- **Input Rate**: Events/second consumed from Kafka
- **Processing Time**: How long each micro-batch takes to process
- **Batch Duration**: Total time including scheduling overhead
- **Input Rows vs Processed Rows**: Detect data loss or filtering
- **Watermark**: Current event-time watermark for late data handling
- **State Store Metrics**: Memory used by stateful operations (windowing, deduplication)
- **Query Progress Timeline**: Visual representation of micro-batch execution over time

**SQL Tab**
- Visual query plans for all DataFrame transformations
- Physical and logical plans showing partition pruning, predicate pushdown
- Execution time breakdown per operator (scan, filter, aggregate, join)
- Useful for optimizing batch queries reading from Delta Lake

**Executors Tab**
- Active executor count (dynamic allocation in action)
- CPU and memory usage per executor
- Task distribution: ensure workload is balanced
- GC time: detect memory pressure
- Shuffle read/write per executor

**Environment Tab**
- All Spark configuration settings (verify FAIR scheduler, Delta Lake extensions)
- JVM properties and classpath
- Useful for debugging configuration issues

#### Key Metrics to Watch:

| Metric | Location | What to Look For |
|---|---|---|
| **Streaming lag** | Streaming tab → Input Rate vs Processing Time | Processing time should be < trigger interval (5 minutes) |
| **Batch concurrency** | Jobs tab | Multiple batch jobs should show "RUNNING" simultaneously while streaming continues |
| **Scheduler pool fairness** | Jobs tab → Job details | Batch jobs in "batch-pool", streaming in "streaming-pool" |
| **Delta partition pruning** | SQL tab → Query details | "PartitionFilters" should show event_date filters being applied |
| **Task failures** | Stages tab | Should be 0 or rare. High failure rate indicates executor instability |
| **Executor scaling** | Executors tab | Executors should scale up during batch jobs, scale down when idle |

#### Troubleshooting with Spark UI:

**Problem**: Batch jobs are slow
- **Check**: SQL tab → Look for "FileScan" operations without partition filters
- **Fix**: Ensure your batch queries include date range filters to leverage partition pruning

**Problem**: Streaming query is lagging (processing time > 5 minutes)
- **Check**: Streaming tab → Processing time and input rate
- **Fix**: Increase executor count or reduce micro-batch size

**Problem**: Batch jobs are blocking streaming
- **Check**: Jobs tab → Verify both pools show active jobs
- **Fix**: Ensure `spark.scheduler.mode=FAIR` and `fairscheduler.xml` is loaded

**Problem**: Out of memory errors
- **Check**: Executors tab → GC time (should be < 10% of task time)
- **Fix**: Increase `spark.executor.memory` or reduce batch job concurrency

#### Practical Example: Monitoring a Batch Query

After submitting a batch job (e.g., `revenue_by_category` for 30 days of electronics in the west region):

**What You'll See in Spark UI:**

1. **Jobs Tab** will show:
   ```
   Job ID: 5    Description: "save at BatchJob.scala:78"    Pool: batch-pool    Status: RUNNING
   Job ID: 4    Description: "collect at AnalysisAggregator.scala:42"    Pool: batch-pool    Status: RUNNING
   Job ID: 3    Description: "start at StreamingJob.scala:56"    Pool: streaming-pool    Status: RUNNING (continuous)
   ```
   
2. **SQL Tab** will show the Delta Lake read with partition pruning:
   ```
   FileScan delta [transaction_id, event_timestamp, ...] 
   Batched: true
   PartitionFilters: [isnotnull(event_date), (event_date >= 2024-12-15), (event_date <= 2025-01-14)]
   PushedFilters: [IsNotNull(category), IsNotNull(region), EqualTo(category, electronics), EqualTo(region, west)]
   ReadSchema: struct<...>
   ```
   This confirms your date range and filters are being pushed down for efficiency.

3. **Streaming Tab** shows the streaming query continuing without interruption:
   ```
   Input Rate: 10.2 rows/sec
   Processing Time: 2.3 seconds
   Batch Duration: 2.5 seconds
   Total Input Rows: 612
   ```

4. **Executors Tab** shows dynamic allocation:
   ```
   Executor 0: 8 cores, 4 GB memory, 2 active tasks (streaming-pool)
   Executor 1: 8 cores, 4 GB memory, 6 active tasks (batch-pool)
   Executor 2: 8 cores, 4 GB memory, 6 active tasks (batch-pool)  ← scaled up for batch
   ```

This demonstrates the FAIR scheduler allowing both workloads to run concurrently.

### Kafka UI — Monitoring Event Streams

Open [http://localhost:8090](http://localhost:8090) in your browser for Kafka cluster monitoring.

#### What You Can Monitor:

**Topics Overview**
- Topic list with partition count and replication factor
- `transactions-stream` topic health and configuration
- Message throughput (messages/sec)
- Total message count and retention settings

**Topic Details for `transactions-stream`**
- **Messages Tab**: Browse actual transaction events (JSON payloads)
- **Consumers Tab**: View active consumer groups and their lag
  - Consumer group: `unified-pipeline-stream` (your streaming job)
  - Current offset vs end offset per partition
  - Lag: how many messages behind the stream is (should be near 0)
- **Partitions Tab**: Message distribution across 12 partitions
- **Settings Tab**: Retention policy (168 hours = 7 days), compression, max message size

**Consumer Groups**
- **Lag Monitoring**: Critical metric — high lag means streaming can't keep up
- **Offset Progress**: Visualize how the consumer is moving through the topic
- **Member Details**: Which Spark executors are consuming from which partitions

**Brokers**
- Broker health and uptime
- Disk usage and network I/O
- Leader/replica distribution

#### Key Metrics to Watch:

| Metric | Location | What to Look For |
|---|---|---|
| **Consumer lag** | Consumers tab → `unified-pipeline-stream` | Should be < 1000 messages. High lag = streaming can't keep up |
| **Partition balance** | Partitions tab | Messages should be roughly evenly distributed across 12 partitions |
| **Message throughput** | Topics → `transactions-stream` | Match the producer rate (10 events/sec default for generator) |
| **Retention warnings** | Topics → Settings | Messages older than 7 days are auto-deleted |

#### Useful Operations:

**Browse Recent Messages**
1. Go to Topics → `transactions-stream` → Messages
2. Set offset to "Latest" or choose a timestamp
3. Inspect JSON payloads to verify schema correctness

**Check Streaming Health**
1. Go to Consumers → `unified-pipeline-stream`
2. Verify lag is near 0 for all 12 partitions
3. If lag is increasing: streaming query is falling behind

**Verify Kafka Stream Generator**
1. Go to Topics → `transactions-stream` → Messages
2. Click "Live mode" to see messages arriving in real-time
3. Should see ~10 messages/second (default generator rate)

#### Troubleshooting with Kafka UI:

**Problem**: No messages in `transactions-stream` topic
- **Check**: Topics tab → `transactions-stream` should show > 0 messages
- **Fix**: Run `python scripts/kafka_stream_generator.py` to start producing events

**Problem**: High consumer lag (> 10,000 messages)
- **Check**: Consumers tab → Lag column
- **Fix**: Increase Spark executor resources or reduce Kafka producer rate

**Problem**: Uneven partition distribution
- **Check**: Partitions tab → Message count per partition
- **Fix**: Kafka producer may not be using proper partitioning key (`transaction_id`)

---

## Docker Lifecycle Commands

```bash
# Start the full stack (build image if needed)
docker compose up -d --build

# Restart only the pipeline (after sbt assembly)
docker compose up -d --build pipeline

# Stop everything (preserves data volumes)
docker compose down

# Stop everything AND delete data (clean slate)
docker compose down -v

# Rebuild from scratch (no cache)
docker compose build --no-cache pipeline
docker compose up -d
```

### Persisted Data

The `docker-compose.yml` mounts `./data` on the host to `/opt/spark/data` in the pipeline container. This means:

- **Delta Lake tables** (`data/delta/`) survive container restarts
- **Checkpoints** (`data/checkpoints/`) survive container restarts — streaming resumes from the last committed offset
- **Kafka data** is stored in a named Docker volume (`kafka-data`)

To fully reset all data:

```bash
docker compose down -v
rm -rf data/
docker compose up -d --build
```

---

## Local Development (Without Docker for the Pipeline)

If you prefer running the Spark application directly on your machine (e.g., for faster iteration with `sbt run`), use Docker only for infrastructure:

### 1. Start Kafka Infrastructure Only

```bash
docker compose up -d zookeeper kafka kafka-ui
```

### 2. Build and Run

```bash
sbt assembly

spark-submit \
  --class com.pipeline.Main \
  --master "local[*]" \
  --driver-memory 4g \
  target/scala-2.13/UnifiedTransactionPipeline-assembly-1.0.0.jar
```

In this mode, `localhost:9092` connects directly to Kafka (the `PLAINTEXT_HOST` listener). The pipeline uses `data/` in the project root for Delta Lake tables and checkpoints.

---

## API Reference

### Streaming Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/stream/start` | Start Kafka streaming (body: `{"topic": "transactions-stream"}`) |
| `POST` | `/stream/stop` | Stop all streaming queries |
| `GET` | `/stream/status` | Status of active streaming queries |

### Batch Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/batch/run` | Submit async batch job (returns `batchId`) |
| `GET` | `/batch/status/:batchId` | Poll batch job lifecycle status |
| `GET` | `/batch/data/:batchId` | Retrieve raw batch data (paginated via `?limit=&offset=`) |
| `GET` | `/batch/data/:batchId/aggregated` | Retrieve aggregated analysis results |
| `GET` | `/batch/list` | List all batch jobs (filter by `?status=`) |

### Control Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/health` | Liveness probe (Spark status + uptime) |
| `POST` | `/stop` | Graceful shutdown |

### Error Responses

All errors follow a consistent format:

```json
{
  "error": "BATCH_NOT_FOUND",
  "message": "No batch job found with ID: batch-xyz",
  "timestamp": "2025-01-15T14:32:07.123Z"
}
```

| HTTP Status | Error Code | When |
|-------------|------------|------|
| 400 | `INVALID_REQUEST` | Malformed JSON, missing fields, unknown `analysisType` |
| 404 | `BATCH_NOT_FOUND` | `batchId` not in registry |
| 409 | `BATCH_NOT_READY` | Requesting data for a batch still `PENDING` or `RUNNING` |
| 500 | `INTERNAL_ERROR` | Unexpected Spark/Delta/Kafka exception |
| 503 | `SERVICE_UNAVAILABLE` | SparkSession is stopped or unhealthy |

---

## Analysis Types

Use these values in the `analysisType` field of `POST /batch/run`:

| Value | Description |
|-------|-------------|
| `revenue_by_category` | Revenue, AOV, transaction count by product category |
| `revenue_by_region` | Geographic revenue heatmap by region and city |
| `payment_analysis` | Payment method trends and revenue share |
| `customer_segmentation` | Customer lifetime value and tier segmentation |
| `fraud_analysis` | Fraud hotspot analysis by category, region, channel |
| `hourly_trends` | Time-series hourly transaction breakdown |
| `channel_performance` | Sales channel comparison with fraud rates |
| `inventory_velocity` | Product sell-through rate and velocity |
| `full_report` | Runs **all** of the above |

---

## Configuration

Configuration uses [Typesafe Config](https://github.com/lightbend/config) (HOCON format). The pipeline reads `conf/application.conf` at startup.

| Config Path | Default | Env Override | Description |
|-------------|---------|--------------|-------------|
| `pipeline.http.port` | `8080` | — | HTTP server port |
| `pipeline.spark.master` | `local[*]` | — | Spark master URL |
| `pipeline.kafka.bootstrap-servers` | `localhost:9092` | `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address |
| `pipeline.kafka.stream-topic` | `transactions-stream` | — | Kafka topic for streaming |
| `pipeline.delta.base-path` | `data/delta` | — | Delta Lake root directory |
| `pipeline.batch.thread-pool-size` | `8` | — | Max concurrent batch jobs |
| `pipeline.batch.default-page-size` | `100` | — | Default pagination size |
| `pipeline.batch.max-page-size` | `10000` | — | Hard cap on page size |

When running in Docker, `KAFKA_BOOTSTRAP_SERVERS` is automatically set to `kafka:29092` (the internal Docker network address) via `docker-compose.yml`.

---

## Technology Stack

| Component | Technology |
|-----------|-----------|
| **Language** | Scala 2.13.13 |
| **Build** | sbt 1.11.6 + sbt-assembly |
| **Compute** | Apache Spark 3.5.1 |
| **Streaming** | Spark Structured Streaming + Kafka |
| **Storage** | Delta Lake 3.1.0 |
| **HTTP Server** | Apache Pekko HTTP 1.0.1 |
| **JSON** | Circe 0.14.6 + Spray JSON |
| **Config** | Typesafe Config 1.4.3 |
| **Logging** | Logback + scala-logging |
| **Container** | Eclipse Temurin 17 JRE + Spark 3.5.1 |
| **Python Scripts** | uv + kafka-python + Faker + PySpark |
| **Infrastructure** | Docker Compose (Kafka 7.5 + ZK + Kafka UI) |

---

## Troubleshooting

### `Failed to find data source: kafka`

The fat JAR is missing `META-INF/services/` files. This happens when the `assemblyMergeStrategy` in `build.sbt` discards all `META-INF/` entries. The fix (already applied in this repo) is:

```scala
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "services", _ @_*) => MergeStrategy.concat  // ← critical
  case PathList("META-INF", "MANIFEST.MF")     => MergeStrategy.discard
  case PathList("META-INF", xs @ _*)           => MergeStrategy.discard
  case "reference.conf"                        => MergeStrategy.concat
  case x                                       => MergeStrategy.first
}
```

Run `sbt clean assembly` and rebuild the Docker image: `docker compose up -d --build pipeline`.

### `UnknownTopicOrPartitionException`

The Kafka topic `transactions-stream` doesn't exist yet. Start the stream generator first (`uv run python kafka_stream_generator.py`), wait for at least one batch of messages, then call `POST /stream/start`.

### `InconsistentClusterIdException` (Kafka won't start)

Stale Kafka data from a previous cluster. Clear volumes:

```bash
docker compose down -v
docker compose up -d
```

### Streaming queries show `isActive: false`

Check the pipeline logs for the root cause:

```bash
docker compose logs --tail 50 pipeline 2>&1 | grep -E "ERROR|terminated"
```

Common causes: Kafka topic doesn't exist, Kafka broker unreachable, checkpoint corruption. After fixing, call `POST /stream/start` again to restart.

### Pipeline container keeps restarting

Check logs for startup errors:

```bash
docker compose logs pipeline
```

If you see `ClassNotFoundException` or version mismatch errors, ensure the Dockerfile's Spark version matches `build.sbt` (both should be 3.5.1 with Scala 2.13).