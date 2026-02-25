# Unified Batch & Streaming Pipeline — Architecture & Agent Guide

This document describes the end-to-end architecture and implementation plan for building an industry-scale unified batch and streaming data pipeline using Apache Kafka and Apache Spark (Scala). It serves as the primary reference for all agents working in this repository.

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Problem Statement](#2-problem-statement)
3. [High-Level Architecture](#3-high-level-architecture)
4. [Industry Data Schema](#4-industry-data-schema)
5. [How the Architecture Meets Each Requirement](#5-how-the-architecture-meets-each-requirement)
6. [Implementation Plan](#6-implementation-plan)
7. [Analysis Aggregator Functions](#7-analysis-aggregator-functions)
8. [Delta Lake Storage Layer](#8-delta-lake-storage-layer)
9. [API Reference](#9-api-reference)
10. [Dummy Data Generation & Seeding](#10-dummy-data-generation--seeding)
11. [Key Dependencies](#11-key-dependencies-buildsbt)
12. [Anticipated Challenges & Mitigations](#12-anticipated-challenges--mitigations)
13. [Success Criteria](#13-success-criteria)

---

## 1. Executive Summary

The goal is to build an **always-on, long-lived Apache Spark application** written in Scala that seamlessly bridges real-time and historical data processing. By embedding an HTTP server within the Spark Driver and leveraging Spark's **FAIR Scheduler**, a single `SparkSession` will:

- Continuously consume streaming data from Kafka.
- Simultaneously serve on-demand batch processing requests against the same (or different) Kafka topics.
- Output results to an industry-standard storage layer (Delta Lake / JDBC).
- Expose batch results by **batch ID** for downstream consumers to query on demand.
- Run a library of **analysis aggregator functions** (revenue analytics, fraud scoring, inventory heatmaps, customer segmentation) executed by Spark across both paradigms.

The application behaves like a **service**, not a traditional job — it starts once and runs indefinitely until an explicit stop signal is received.

---

## 2. Problem Statement

### The "Silo" Issue

Traditionally, Apache Spark applications are built as binary choices:

1. **Batch Applications:** Start, process a finite chunk of static data (e.g., "process yesterday's logs"), then shut down.
2. **Streaming Applications:** Start and run indefinitely, processing new data events in real-time via micro-batches.

### The Gap

Real-world applications often need *both*. For example, a fraud detection system must:
- Check live transactions against real-time patterns (Streaming).
- Re-train its model periodically using the last week's data (Batch).

Currently, developers maintain two separate codebases and two separate operational lifecycles. This project delivers a **Unified Engine**: a single application that handles both paradigms concurrently.

### Key Challenges

| Challenge | Description |
|---|---|
| **Lifecycle Mismatch** | Batch jobs are designed to die; streaming jobs loop forever. The unified app must survive batch job completion without affecting the stream. |
| **Resource Contention** | A heavy batch job must not starve the real-time stream of CPU/RAM. |
| **Single Context** | Both workloads must share one `SparkSession` — no separate clusters per task. |
| **Result Retrieval** | Batch results must be persisted, indexed by batch ID, and queryable after completion via REST. |
| **Schema Consistency** | Both stream and batch pipelines must operate on identical schemas to avoid data drift. |

---

## 3. High-Level Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                         Apache Kafka Cluster                         │
│   ┌───────────────────────┐          ┌────────────────────────────┐  │
│   │  transactions-stream  │          │    transactions-batch      │  │
│   │  (live POS events)    │          │    (historical replay)     │  │
│   └──────────┬────────────┘          └─────────────┬──────────────┘  │
└──────────────│─────────────────────────────────────│─────────────────┘
               │ (infinite subscribe)                │ (bounded offset range)
               ▼                                     ▼
┌──────────────────────────────────────────────────────────────────────┐
│                     Spark Driver (JVM Process)                       │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │              Embedded HTTP Server (Pekko HTTP)                 │  │
│  │                                                                │  │
│  │  POST /stream/start          POST /batch/run                  │  │
│  │  POST /stream/stop           GET  /batch/status/:batchId      │  │
│  │  GET  /batch/data/:batchId   GET  /batch/data/:batchId/agg    │  │
│  │  GET  /health                POST /stop                       │  │
│  └───────────────────────────────┬────────────────────────────────┘  │
│                                  │                                   │
│  ┌───────────────────────────────▼────────────────────────────────┐  │
│  │           Single SparkSession (FAIR Scheduler)                 │  │
│  │                                                                │  │
│  │  ┌──────────────────────┐  ┌────────────────────────────────┐  │  │
│  │  │   Streaming Query    │  │   Batch Job(s) (thread pool)   │  │  │
│  │  │  (background async)  │  │   (concurrent, on-demand)      │  │  │
│  │  │                      │  │                                │  │  │
│  │  │  ┌────────────────┐  │  │  ┌──────────────────────────┐  │  │  │
│  │  │  │ AnalysisAggr.  │  │  │  │   AnalysisAggregator     │  │  │  │
│  │  │  │ (real-time)    │  │  │  │   (historical analysis)  │  │  │  │
│  │  │  └────────────────┘  │  │  └──────────────────────────┘  │  │  │
│  │  └──────────────────────┘  └────────────────────────────────┘  │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │                    Batch Registry (in-memory)                  │  │
│  │  ConcurrentHashMap[batchId -> BatchJobRecord]                  │  │
│  │  BatchJobRecord: {status, startTime, endTime, deltaPath,      │  │
│  │                   rowCount, errorMsg, analysisType}            │  │
│  └────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────┘
               │                                     │
               ▼                                     ▼
┌──────────────────────────────┐  ┌──────────────────────────────────┐
│       Delta Lake             │  │    Delta Lake (batch results)    │
│   (streaming sink)           │  │    /data/delta/batches/{batchId} │
│   /data/delta/stream/        │  │    /data/delta/batch_agg/        │
│   └── transactions/          │  │                                  │
│   └── stream_aggregations/   │  │    JDBC Sink (PostgreSQL etc)    │
└──────────────────────────────┘  └──────────────────────────────────┘
```

### Component Responsibilities

| Component | Role |
|---|---|
| **Apache Kafka** | Central nervous system. Decouples producers from consumers. Retains data per retention policy (e.g., 7 days), enabling batch replay of historical data. Topics use the industry transaction schema defined in §4. |
| **Embedded HTTP Server (Pekko HTTP)** | Keeps the Driver JVM alive indefinitely. Accepts REST commands to start/stop stream, trigger batch jobs, retrieve batch results by ID, and query aggregation outputs. |
| **SparkSession (FAIR Scheduler)** | Single entry point for all Spark operations. `spark.scheduler.mode=FAIR` ensures streaming and batch jobs share resources concurrently. |
| **Analysis Aggregator** | A Scala object containing reusable Spark DataFrame transformation functions (§7). Both streaming and batch jobs invoke the same aggregator functions to ensure analytical consistency. |
| **Batch Registry** | A thread-safe `ConcurrentHashMap[String, BatchJobRecord]` that tracks every submitted batch job's lifecycle: `PENDING → RUNNING → COMPLETED / FAILED`. Stores the Delta path so results can be read back by batch ID. |
| **Delta Lake** | ACID-compliant Lakehouse sink. Streaming writes to `/data/delta/stream/transactions/`. Each batch job writes raw + aggregated results to `/data/delta/batches/{batchId}/`. Enables time-travel queries and schema enforcement. |

### Data Flow — Streaming Path

1. Kafka `transactions-stream` topic receives JSON events from POS terminals, mobile apps, or payment gateways.
2. Spark Structured Streaming reads the topic via `readStream.format("kafka")`.
3. JSON values are parsed against the `TransactionEvent` schema (§4).
4. The `AnalysisAggregator.realtimeDashboard()` function computes sliding-window metrics (revenue per minute, transaction counts, fraud flags).
5. Raw events write to `delta://stream/transactions/` (append mode, partitioned by `event_date`).
6. Aggregated metrics write to `delta://stream/stream_aggregations/` (complete/update mode).

### Data Flow — Batch Path

1. Client sends `POST /batch/run` with topic, offset range, and desired analysis type.
2. Server generates a UUID `batchId`, inserts a `PENDING` record into the Batch Registry, and returns the `batchId` immediately.
3. On a separate thread (from a fixed thread pool), Spark reads the bounded Kafka range via `read.format("kafka")`.
4. JSON values are parsed against the same `TransactionEvent` schema.
5. The requested `AnalysisAggregator` function runs (e.g., `revenueByCategory`, `customerSegmentation`).
6. Raw data writes to `delta://batches/{batchId}/raw/`.
7. Aggregated results write to `delta://batches/{batchId}/aggregated/`.
8. Registry entry is updated to `COMPLETED` with `rowCount` and `deltaPath`.
9. Client polls `GET /batch/status/{batchId}` or retrieves results via `GET /batch/data/{batchId}`.

### Data Flow — Batch Retrieval Path (from Delta Lake)

1. Client sends `GET /batch/data/{batchId}` (optionally with `?limit=N&offset=M`).
2. Server looks up the `batchId` in the Batch Registry to find the Delta path.
3. If `status == COMPLETED`, Spark reads from `delta://batches/{batchId}/raw/` using `spark.read.format("delta").load(path)`.
4. Results are serialized to JSON and returned in the HTTP response with pagination metadata.
5. For aggregated results, `GET /batch/data/{batchId}/aggregated` reads from the aggregated Delta path instead.

---

## 4. Industry Data Schema

### Domain: Retail / E-Commerce Transaction Processing

This schema represents a realistic point-of-sale (POS) and e-commerce transaction event, suitable for fraud detection, revenue analytics, inventory management, and customer segmentation use cases.

### 4.1 `TransactionEvent` — Primary Event Schema

This is the JSON schema for every event produced to Kafka and stored in Delta Lake.

| Field | Type | Nullable | Description | Example |
|---|---|---|---|---|
| `transaction_id` | `STRING` | No | UUID v4. Globally unique identifier for this transaction. | `"a3f8e2b1-7c44-4d12-9e56-1a2b3c4d5e6f"` |
| `event_timestamp` | `TIMESTAMP` | No | ISO-8601 UTC timestamp when the event occurred at the source. Used as the **event-time** column for watermarking. | `"2025-01-15T14:32:07.123Z"` |
| `processing_timestamp` | `TIMESTAMP` | Yes | Set by the pipeline at ingestion time. `null` in Kafka; populated on write to Delta. | `"2025-01-15T14:32:08.456Z"` |
| `customer_id` | `STRING` | No | Unique customer identifier (hashed PII). | `"CUST-00042871"` |
| `customer_tier` | `STRING` | Yes | Loyalty program tier. One of: `bronze`, `silver`, `gold`, `platinum`. | `"gold"` |
| `product_id` | `STRING` | No | SKU or product identifier. | `"SKU-ELC-00912"` |
| `product_name` | `STRING` | No | Human-readable product name. | `"Sony WH-1000XM5 Headphones"` |
| `category` | `STRING` | No | Top-level product category. One of: `electronics`, `clothing`, `grocery`, `home_garden`, `sports`, `beauty`, `automotive`, `books`, `toys`, `pharmacy`. | `"electronics"` |
| `sub_category` | `STRING` | Yes | Granular sub-category. | `"audio_headphones"` |
| `brand` | `STRING` | Yes | Manufacturer or brand name. | `"Sony"` |
| `quantity` | `INT` | No | Number of units purchased. Must be >= 1. | `2` |
| `unit_price` | `DOUBLE` | No | Price per unit in the transaction currency. | `349.99` |
| `discount_percent` | `DOUBLE` | Yes | Discount applied (0.0 - 100.0). Default `0.0`. | `15.0` |
| `total_amount` | `DOUBLE` | No | `quantity * unit_price * (1 - discount_percent/100)`. Pre-computed by the source. | `594.98` |
| `tax_amount` | `DOUBLE` | Yes | Tax collected on this line item. | `53.55` |
| `currency` | `STRING` | No | ISO 4217 currency code. | `"USD"` |
| `payment_method` | `STRING` | No | One of: `credit_card`, `debit_card`, `upi`, `wallet`, `bank_transfer`, `cod`, `gift_card`. | `"credit_card"` |
| `card_network` | `STRING` | Yes | If card payment: `visa`, `mastercard`, `amex`, `rupay`, `discover`. Null for non-card. | `"visa"` |
| `transaction_status` | `STRING` | No | One of: `completed`, `pending`, `failed`, `refunded`, `chargeback`. | `"completed"` |
| `channel` | `STRING` | No | Originating channel. One of: `pos_in_store`, `web`, `mobile_app`, `marketplace`, `call_center`. | `"mobile_app"` |
| `store_id` | `STRING` | Yes | Physical or virtual store identifier. Null for marketplace. | `"STORE-MUM-042"` |
| `region` | `STRING` | No | Geographic region. One of: `north`, `south`, `east`, `west`, `central`, `northeast`. | `"west"` |
| `city` | `STRING` | No | City of the transaction origin. | `"Mumbai"` |
| `state` | `STRING` | Yes | State/province code. | `"MH"` |
| `postal_code` | `STRING` | Yes | ZIP/postal code. | `"400001"` |
| `device_type` | `STRING` | Yes | For digital channels: `android`, `ios`, `desktop`, `tablet`. Null for POS. | `"android"` |
| `session_id` | `STRING` | Yes | Web/app session identifier for clickstream correlation. | `"sess-8a7b6c5d4e3f"` |
| `ip_address` | `STRING` | Yes | Hashed/masked IP for fraud analysis. | `"192.168.xxx.xxx"` |
| `is_fraudulent` | `BOOLEAN` | Yes | Label from the fraud detection engine. `null` if not yet scored. | `false` |
| `fraud_score` | `DOUBLE` | Yes | ML model fraud probability (0.0 - 1.0). `null` if not scored. | `0.03` |
| `batch_id` | `STRING` | Yes | Populated only during batch processing. The UUID of the batch job that processed this record. `null` in raw stream. | `"batch-20250115-001"` |

### 4.2 Spark StructType Definition

```
val TransactionSchema: StructType = new StructType()
  .add("transaction_id",        StringType,    nullable = false)
  .add("event_timestamp",       TimestampType, nullable = false)
  .add("processing_timestamp",  TimestampType, nullable = true)
  .add("customer_id",           StringType,    nullable = false)
  .add("customer_tier",         StringType,    nullable = true)
  .add("product_id",            StringType,    nullable = false)
  .add("product_name",          StringType,    nullable = false)
  .add("category",              StringType,    nullable = false)
  .add("sub_category",          StringType,    nullable = true)
  .add("brand",                 StringType,    nullable = true)
  .add("quantity",              IntegerType,   nullable = false)
  .add("unit_price",            DoubleType,    nullable = false)
  .add("discount_percent",      DoubleType,    nullable = true)
  .add("total_amount",          DoubleType,    nullable = false)
  .add("tax_amount",            DoubleType,    nullable = true)
  .add("currency",              StringType,    nullable = false)
  .add("payment_method",        StringType,    nullable = false)
  .add("card_network",          StringType,    nullable = true)
  .add("transaction_status",    StringType,    nullable = false)
  .add("channel",               StringType,    nullable = false)
  .add("store_id",              StringType,    nullable = true)
  .add("region",                StringType,    nullable = false)
  .add("city",                  StringType,    nullable = false)
  .add("state",                 StringType,    nullable = true)
  .add("postal_code",           StringType,    nullable = true)
  .add("device_type",           StringType,    nullable = true)
  .add("session_id",            StringType,    nullable = true)
  .add("ip_address",            StringType,    nullable = true)
  .add("is_fraudulent",         BooleanType,   nullable = true)
  .add("fraud_score",           DoubleType,    nullable = true)
  .add("batch_id",              StringType,    nullable = true)
```

### 4.3 Delta Lake Partitioning Strategy

| Table | Partition Columns | Rationale |
|---|---|---|
| `stream/transactions` | `event_date` (derived: `cast(event_timestamp as DATE)`) | Enables efficient time-range queries on historical streaming data. One partition per day. |
| `stream/stream_aggregations` | `window_start` | Aggregation windows are naturally time-ordered. |
| `batches/{batchId}/raw` | `category` | Batch analysis is often category-scoped; partitioning avoids full scans. |
| `batches/{batchId}/aggregated` | None (small tables) | Aggregated results are small enough to not require partitioning. |

### 4.4 Sample JSON Event (as produced to Kafka)

```
{
  "transaction_id": "a3f8e2b1-7c44-4d12-9e56-1a2b3c4d5e6f",
  "event_timestamp": "2025-01-15T14:32:07.123Z",
  "processing_timestamp": null,
  "customer_id": "CUST-00042871",
  "customer_tier": "gold",
  "product_id": "SKU-ELC-00912",
  "product_name": "Sony WH-1000XM5 Headphones",
  "category": "electronics",
  "sub_category": "audio_headphones",
  "brand": "Sony",
  "quantity": 2,
  "unit_price": 349.99,
  "discount_percent": 15.0,
  "total_amount": 594.98,
  "tax_amount": 53.55,
  "currency": "USD",
  "payment_method": "credit_card",
  "card_network": "visa",
  "transaction_status": "completed",
  "channel": "mobile_app",
  "store_id": null,
  "region": "west",
  "city": "Mumbai",
  "state": "MH",
  "postal_code": "400001",
  "device_type": "android",
  "session_id": "sess-8a7b6c5d4e3f",
  "ip_address": "192.168.xxx.xxx",
  "is_fraudulent": false,
  "fraud_score": 0.03,
  "batch_id": null
}
```

---

## 5. How the Architecture Meets Each Requirement

### Requirement 1: Integrate Apache Spark and Apache Kafka
**Solution:** Use the official `spark-sql-kafka-0-10` connector. This allows Spark to read from Kafka natively as either a `DataStreamReader` (streaming) or a bounded `DataFrame` (batch) using the same API surface. Both paths parse against the shared `TransactionEvent` schema (§4).

### Requirement 2 & 3: Single Context + Multiple Concurrent Batches
**Solution:** The `SparkSession` is instantiated **once** at application startup as a shared `lazy val` singleton in `SparkManager`. The HTTP server handles each `/batch/run` request on a separate JVM thread (from a `FixedThreadPool(8)`). Each thread uses the same session to execute a `spark.read.format("kafka")...load()` call independently, without blocking the streaming query. Each batch gets a unique `batchId`, and results are written to an isolated Delta path `batches/{batchId}/`.

### Requirement 4: Live Until Stop Is Invoked
**Solution:** Pekko HTTP's `Http().newServerAt(...).bind(routes)` returns a `Future[ServerBinding]` that keeps the main thread alive. The application lifecycle is:
1. Initialize `SparkSession` (FAIR scheduler + Delta extensions).
2. Bind the HTTP server (main thread parks here via `Await.result`).
3. On `POST /stop`: stop all active streaming queries, drain in-flight batch jobs, call `spark.stop()`, unbind the server, call `System.exit(0)`.

### Requirement 5: FAIR Scheduler for Non-Blocking Concurrency
**Solution:** `spark.scheduler.mode=FAIR` causes the cluster to interleave the streaming micro-batch jobs and batch jobs, allocating CPU cores to both simultaneously. Two **Scheduler Pools** are configured in `fairscheduler.xml`:
- `streaming-pool`: weight=3, minShare=4 (priority for real-time SLA).
- `batch-pool`: weight=1, minShare=1 (best-effort for historical analysis).

### Requirement 6: Batch Result Retrieval by ID
**Solution:** Every batch job produces a unique `batchId`. The raw and aggregated DataFrames are written to `delta://batches/{batchId}/raw/` and `delta://batches/{batchId}/aggregated/`. The `GET /batch/data/{batchId}` endpoint reads from Delta using `spark.read.format("delta").load(path)`, converts to JSON, and returns paginated results. This decouples the batch computation from result consumption — clients can retrieve data minutes or hours after the batch completes.

### Requirement 7: Reusable Analysis Aggregators
**Solution:** A single `AnalysisAggregator` Scala object (§7) contains pure `DataFrame => DataFrame` transformation functions. Both `StreamingJob` and `BatchJob` call these functions. This guarantees that a metric like "revenue by category" is computed identically whether it's over a real-time micro-batch or a historical batch of 10M records.

---

## 6. Implementation Plan

### Phase 1: Infrastructure & Configuration

#### Kafka Setup
- Deploy a highly available Kafka cluster (or use managed: Confluent Cloud, Amazon MSK, Azure Event Hubs).
- Create topics:
  - `transactions-stream` — 12 partitions, 3 replicas, 7-day retention. Used for live streaming.
  - `transactions-batch` — 12 partitions, 3 replicas, 30-day retention. Used for historical batch replay.
- Partition counts should match or exceed the number of Spark executor cores for optimal parallelism.
- For local development, use the `docker-compose.yml` in this repo (single-broker, no replication).

#### Kafka Topic Configuration

```
# Production settings for transactions-stream
num.partitions=12
replication.factor=3
retention.ms=604800000          # 7 days
max.message.bytes=1048576       # 1 MB
cleanup.policy=delete

# Production settings for transactions-batch
num.partitions=12
replication.factor=3
retention.ms=2592000000         # 30 days
max.message.bytes=1048576
cleanup.policy=delete
```

#### Critical Spark Configuration

```
# Scheduler
spark.scheduler.mode=FAIR
spark.scheduler.allocation.file=conf/fairscheduler.xml

# Streaming
spark.sql.streaming.checkpointLocation=s3a://your-bucket/checkpoints/
spark.sql.streaming.minBatchesToRetain=100
spark.sql.streaming.stateStore.providerClass=org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider

# Dynamic Allocation
spark.dynamicAllocation.enabled=true
spark.dynamicAllocation.minExecutors=2
spark.dynamicAllocation.maxExecutors=20
spark.dynamicAllocation.executorIdleTimeout=60s
spark.dynamicAllocation.schedulerBacklogTimeout=1s

# Delta Lake
spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
spark.databricks.delta.schema.autoMerge.enabled=true
spark.databricks.delta.optimizeWrite.enabled=true

# Kafka Consumer
spark.kafka.consumer.max.poll.records=500
spark.kafka.consumer.fetch.min.bytes=1
spark.kafka.consumer.fetch.max.wait.ms=500

# Memory
spark.driver.memory=4g
spark.executor.memory=8g
spark.memory.offHeap.enabled=true
spark.memory.offHeap.size=2g
```

#### FAIR Scheduler Pools (`conf/fairscheduler.xml`)

```
<?xml version="1.0"?>
<allocations>
  <pool name="streaming-pool">
    <schedulingMode>FAIR</schedulingMode>
    <weight>3</weight>
    <minShare>4</minShare>
  </pool>
  <pool name="batch-pool">
    <schedulingMode>FAIR</schedulingMode>
    <weight>1</weight>
    <minShare>1</minShare>
  </pool>
</allocations>
```

#### Application Configuration (`conf/application.conf`)

```
pipeline {
  app-name = "UnifiedTransactionPipeline"

  http {
    host = "0.0.0.0"
    port = 8080
  }

  spark {
    master = "local[*]"              # Override in production: spark://host:7077 or k8s://...
    checkpoint-dir = "/data/checkpoints"
    fair-scheduler-file = "conf/fairscheduler.xml"
  }

  kafka {
    bootstrap-servers = "localhost:9092"
    stream-topic = "transactions-stream"
    batch-topic = "transactions-batch"
    consumer-group-prefix = "unified-pipeline"
  }

  delta {
    base-path = "/data/delta"
    stream-transactions-path = ${pipeline.delta.base-path}"/stream/transactions"
    stream-aggregations-path = ${pipeline.delta.base-path}"/stream/stream_aggregations"
    batch-base-path = ${pipeline.delta.base-path}"/batches"
  }

  batch {
    thread-pool-size = 8             # Max concurrent batch jobs
    default-page-size = 100          # Pagination default for GET /batch/data
    max-page-size = 10000            # Hard cap on page size
  }
}
```

### Phase 2: Application Development (Scala)

#### Project Structure

```
StreamAndBatchProcessing/
├── build.sbt                                    ← SBT build with all dependencies
├── project/
│   ├── build.properties                         ← sbt.version=1.11.6
│   └── plugins.sbt                              ← sbt-assembly plugin
├── src/
│   └── main/
│       ├── scala/
│       │   └── com/pipeline/
│       │       ├── Main.scala                   ← Entry point; wires SparkSession + HTTP server
│       │       ├── SparkManager.scala           ← Singleton SparkSession (lazy val)
│       │       ├── models/
│       │       │   ├── Schemas.scala            ← TransactionEvent StructType + case classes
│       │       │   └── BatchJobRecord.scala     ← Case class for batch registry entries
│       │       ├── registry/
│       │       │   └── BatchRegistry.scala      ← ConcurrentHashMap wrapper for batch tracking
│       │       ├── analytics/
│       │       │   └── AnalysisAggregator.scala ← All Spark aggregation functions (§7)
│       │       ├── routes/
│       │       │   ├── StreamRoutes.scala        ← /stream/start, /stream/stop
│       │       │   ├── BatchRoutes.scala         ← /batch/run, /batch/status, /batch/data
│       │       │   └── ControlRoutes.scala       ← /stop, /health
│       │       ├── jobs/
│       │       │   ├── StreamingJob.scala        ← Kafka streaming read → Delta write
│       │       │   └── BatchJob.scala            ← Bounded Kafka read → analysis → Delta write
│       │       └── config/
│       │           └── AppConfig.scala           ← Typesafe Config wrapper
│       └── resources/
│           └── application.conf                  ← Default config (overridden by conf/)
├── conf/
│   ├── application.conf                          ← Production config
│   └── fairscheduler.xml                         ← FAIR scheduler pools
├── scripts/
│   ├── kafka_stream_generator.py                 ← Dummy Kafka stream producer (§10.1)
│   └── delta_lake_seeder.py                      ← Historical data seeder for Delta (§10.2)
├── docker/
│   └── Dockerfile                                ← Production container image
├── docker-compose.yml                            ← Local Kafka + Zookeeper
├── data_gen.py                                   ← (Legacy) simple data generator
└── README.md
```

#### Key Implementation Details

**`Main.scala` — Application Entry Point**
- Reads `AppConfig` from Typesafe Config.
- Initializes `SparkManager.spark` (triggers lazy session creation).
- Creates `BatchRegistry` instance.
- Wires all route classes, passing them the shared `SparkSession` and `BatchRegistry`.
- Combines routes: `StreamRoutes ~ BatchRoutes ~ ControlRoutes`.
- Binds Pekko HTTP server on configured host:port.
- Registers a JVM shutdown hook that calls graceful drain.
- Parks the main thread via `Await.result(bindingFuture, Duration.Inf)`.

**`SparkManager.scala` — Singleton Session**
- `lazy val spark: SparkSession` — initialized once on first access.
- Configures: app name, master, FAIR scheduler mode, Delta extensions, checkpoint dir.
- Sets `spark.scheduler.allocation.file` to the fairscheduler.xml path.
- Logs the Spark UI URL on initialization for monitoring.

**`models/Schemas.scala` — Shared Schema Definitions**
- Contains the `TransactionSchema` StructType as defined in §4.2.
- Contains a `TransactionEvent` case class mirroring the schema for type-safe operations.
- Contains helper methods: `parseKafkaValue(df: DataFrame): DataFrame` — takes a raw Kafka DataFrame, casts `value` to string, applies `from_json` with `TransactionSchema`, and `select("data.*")`.

**`models/BatchJobRecord.scala` — Batch Tracking**
```
case class BatchJobRecord(
  batchId:       String,
  status:        String,       // PENDING, RUNNING, COMPLETED, FAILED
  analysisType:  String,       // which AnalysisAggregator function to run
  topic:         String,
  startOffset:   String,
  endOffset:     String,
  submitTime:    Instant,
  startTime:     Option[Instant],
  endTime:       Option[Instant],
  rowCount:      Option[Long],
  deltaRawPath:  Option[String],
  deltaAggPath:  Option[String],
  errorMessage:  Option[String]
)
```

**`registry/BatchRegistry.scala` — Thread-Safe Batch Tracking**
- Wraps `java.util.concurrent.ConcurrentHashMap[String, BatchJobRecord]`.
- Methods: `register(record)`, `updateStatus(batchId, status, ...)`, `get(batchId): Option[BatchJobRecord]`, `listAll(): Seq[BatchJobRecord]`, `listByStatus(status): Seq[BatchJobRecord]`.
- All mutation methods are atomic using `ConcurrentHashMap.compute()`.

**`StreamingJob.scala` — Kafka Streaming Read**
- Sets `spark.sparkContext.setLocalProperty("spark.scheduler.pool", "streaming-pool")` before any Spark action.
- Reads from Kafka via `spark.readStream.format("kafka").option("subscribe", streamTopic)`.
- Parses JSON using `Schemas.parseKafkaValue()`.
- Adds derived columns: `event_date`, `processing_timestamp` (current_timestamp).
- Applies watermark: `.withWatermark("event_timestamp", "10 minutes")`.
- Writes raw events to Delta: `.writeStream.format("delta").partitionBy("event_date").outputMode("append").option("checkpointLocation", ...).start()`.
- Concurrently starts a second streaming query for real-time aggregations via `AnalysisAggregator.realtimeDashboard()`, writing to the stream aggregations Delta path.
- Returns `Seq[StreamingQuery]` handles for lifecycle management.

**`BatchJob.scala` — Bounded Kafka Batch Read**
- Accepts: `batchId`, `topic`, `startingOffsets`, `endingOffsets`, `analysisType`.
- Sets `spark.sparkContext.setLocalProperty("spark.scheduler.pool", "batch-pool")` on the executing thread.
- Reads bounded range: `spark.read.format("kafka").option("startingOffsets", start).option("endingOffsets", end).load()`.
- Parses JSON using `Schemas.parseKafkaValue()`.
- Adds `batch_id` column with the current `batchId`.
- Adds `processing_timestamp` column.
- Writes raw data to `delta://batches/{batchId}/raw/` partitioned by `category`.
- Invokes the requested `AnalysisAggregator` function by name (pattern match on `analysisType`).
- Writes aggregated result to `delta://batches/{batchId}/aggregated/`.
- Updates `BatchRegistry` with `COMPLETED`, `rowCount`, and Delta paths.
- On exception: updates `BatchRegistry` with `FAILED` and `errorMessage`.

**`routes/StreamRoutes.scala` — HTTP Endpoints**
- `POST /stream/start` with body `{ "topic": "transactions-stream" }`:
  - Calls `StreamingJob.start(topic)`.
  - Stores `Seq[StreamingQuery]` references in an `AtomicReference`.
  - Returns `200 { "status": "started", "queryIds": [...] }`.
- `POST /stream/stop`:
  - Calls `.stop()` on all stored streaming query references.
  - Returns `200 { "status": "stopped" }`.
- `GET /stream/status`:
  - Returns the status of all active streaming queries (isActive, lastProgress, etc.).

**`routes/BatchRoutes.scala` — HTTP Endpoints**
- `POST /batch/run` with JSON body:
  ```
  {
    "topic": "transactions-batch",
    "startOffset": "earliest",
    "endOffset": "latest",
    "analysisType": "revenue_by_category"
  }
  ```
  - Generates UUID `batchId`.
  - Registers `PENDING` record in `BatchRegistry`.
  - Submits `BatchJob.run(...)` on the batch `ExecutionContext` (FixedThreadPool).
  - Returns `202 { "batchId": "...", "status": "PENDING" }`.

- `GET /batch/status/:batchId`:
  - Looks up `batchId` in `BatchRegistry`.
  - Returns `200` with the full `BatchJobRecord` as JSON.
  - Returns `404` if `batchId` not found.

- `GET /batch/data/:batchId?limit=100&offset=0`:
  - Looks up `batchId` in registry.
  - If `status != COMPLETED`, returns `409 { "error": "Batch not completed", "status": "RUNNING" }`.
  - Reads raw data from Delta: `spark.read.format("delta").load(deltaRawPath)`.
  - Applies pagination: `.limit(limit).offset(offset)` (offset via row_number window or skip logic).
  - Converts to JSON array and returns with headers: `X-Total-Count`, `X-Page-Size`, `X-Page-Offset`.
  - Returns `200` with `{ "batchId": "...", "rowCount": N, "page": {...}, "data": [...] }`.

- `GET /batch/data/:batchId/aggregated?limit=100&offset=0`:
  - Same as above but reads from the aggregated Delta path.
  - Returns the analysis aggregation results for this batch.

- `GET /batch/list?status=COMPLETED&limit=20`:
  - Returns a list of all batch job records, optionally filtered by status.
  - Enables clients to discover past batch runs.

**`routes/ControlRoutes.scala` — Lifecycle Endpoints**
- `GET /health`:
  - Returns `200 { "status": "healthy", "spark": true, "uptime": "..." }`.
  - Checks `spark.sparkContext.isStopped` — if true, returns 503.
- `POST /stop`:
  - Stops all active streaming queries (graceful drain — waits for current micro-batch).
  - Waits for in-flight batch jobs to complete (with a configurable timeout, default 60s).
  - Calls `spark.stop()`.
  - Unbinds HTTP server.
  - Calls `System.exit(0)`.

### Phase 3: Productionization & Deployment

#### build.sbt — Full Specification

```
name := "UnifiedTransactionPipeline"
version := "1.0.0"
scalaVersion := "2.13.13"

val sparkVersion = "3.5.1"
val pekkoVersion = "1.0.2"
val pekkoHttpVersion = "1.0.1"
val deltaVersion = "3.1.0"
val circeVersion = "0.14.6"

libraryDependencies ++= Seq(
  // Spark
  "org.apache.spark"   %% "spark-core"             % sparkVersion % "provided",
  "org.apache.spark"   %% "spark-sql"              % sparkVersion % "provided",
  "org.apache.spark"   %% "spark-sql-kafka-0-10"   % sparkVersion,

  // Delta Lake
  "io.delta"           %% "delta-spark"            % deltaVersion,

  // Pekko HTTP Server
  "org.apache.pekko"   %% "pekko-http"             % pekkoHttpVersion,
  "org.apache.pekko"   %% "pekko-http-spray-json"  % pekkoHttpVersion,
  "org.apache.pekko"   %% "pekko-actor-typed"      % pekkoVersion,
  "org.apache.pekko"   %% "pekko-stream"           % pekkoVersion,

  // JSON
  "io.circe"           %% "circe-core"             % circeVersion,
  "io.circe"           %% "circe-generic"          % circeVersion,
  "io.circe"           %% "circe-parser"           % circeVersion,

  // Config
  "com.typesafe"        % "config"                 % "1.4.3",

  // Logging
  "ch.qos.logback"      % "logback-classic"        % "1.4.14",
  "com.typesafe.scala-logging" %% "scala-logging"  % "3.9.5",

  // Testing
  "org.scalatest"      %% "scalatest"              % "3.2.17"  % Test,
  "org.apache.pekko"   %% "pekko-http-testkit"     % pekkoHttpVersion % Test
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf"              => MergeStrategy.concat
  case x                             => MergeStrategy.first
}
```

#### Containerization (Docker)

```
FROM apache/spark:3.5.1-scala2.13-java17-ubuntu

USER root
COPY target/scala-2.13/UnifiedTransactionPipeline-assembly-1.0.0.jar /opt/spark/jars/app.jar
COPY conf/ /opt/spark/conf/

ENV SPARK_DRIVER_MEMORY=4g
ENV SPARK_EXECUTOR_MEMORY=8g

EXPOSE 8080 4040

ENTRYPOINT ["/opt/spark/bin/spark-submit", \
  "--class", "com.pipeline.Main", \
  "--master", "local[*]", \
  "--driver-memory", "4g", \
  "/opt/spark/jars/app.jar"]
```

#### Kubernetes Deployment (Spark Operator)

- Deploy using the [Spark on Kubernetes Operator](https://github.com/kubeflow/spark-operator).
- The **Driver pod** exposes a `Kubernetes Service` (ClusterIP or LoadBalancer) so external systems can call the REST endpoints.
- **Executor pods** scale dynamically via `spark.dynamicAllocation.enabled=true` and the Kubernetes External Shuffle Service.
- Store checkpoints on S3 (or GCS/ADLS) so that if the Driver pod restarts, the stream resumes from the last committed offset automatically.
- Mount a `PersistentVolumeClaim` for local Delta Lake paths, or use S3/ADLS-backed Delta paths in production.

#### Kubernetes Service Manifest (Example)

```
apiVersion: v1
kind: Service
metadata:
  name: unified-pipeline-api
  labels:
    app: unified-pipeline
spec:
  type: LoadBalancer
  ports:
    - name: http-api
      port: 8080
      targetPort: 8080
    - name: spark-ui
      port: 4040
      targetPort: 4040
  selector:
    app: unified-pipeline
    component: driver
```

#### Fault Tolerance Summary

| Failure Scenario | Recovery Mechanism |
|---|---|
| Driver pod crash | Kubernetes restarts it; `POST /stream/start` re-attaches to existing checkpoint — zero data loss. Delta writes are ACID so no partial data. |
| Batch job exception | Caught within the `Future`; logged and returned as HTTP error via `GET /batch/status/:batchId`. Stream is **unaffected**. Registry entry marked `FAILED` with error message. |
| Executor pod crash | Spark automatically reschedules tasks on remaining/new executors. Speculative execution can be enabled. |
| Kafka broker unavailability | Spark retries with backoff; configurable via `kafka.consumer.retry.*` options. Streaming query pauses and auto-resumes when Kafka recovers. |
| Delta Lake write conflict | Delta's optimistic concurrency control retries the transaction. `databricks.delta.retryWriteConflict.enabled=true`. |
| Batch Registry loss (Driver restart) | On startup, scan `delta://batches/` directory to rebuild registry from existing batch directories and their `_delta_log` metadata. |

---

## 7. Analysis Aggregator Functions

The `AnalysisAggregator` object contains all Spark-executed analytical transformations. Every function has the signature `(df: DataFrame, spark: SparkSession) => DataFrame`, making them composable and testable in isolation. Both streaming and batch pipelines invoke these same functions.

### 7.1 `revenueByCategory`

**Purpose:** Compute total revenue, average order value, and transaction count grouped by product category.

**Used by:** Batch and streaming.

**Logic:**
```
def revenueByCategory(df: DataFrame, spark: SparkSession): DataFrame = {
  df.filter(col("transaction_status") === "completed")
    .groupBy("category")
    .agg(
      sum("total_amount").alias("total_revenue"),
      avg("total_amount").alias("avg_order_value"),
      count("transaction_id").alias("transaction_count"),
      sum("quantity").alias("total_units_sold"),
      avg("discount_percent").alias("avg_discount_pct"),
      countDistinct("customer_id").alias("unique_customers")
    )
    .orderBy(desc("total_revenue"))
}
```

**Output Schema:**

| Column | Type | Description |
|---|---|---|
| `category` | STRING | Product category |
| `total_revenue` | DOUBLE | Sum of all `total_amount` for completed transactions |
| `avg_order_value` | DOUBLE | Mean `total_amount` |
| `transaction_count` | LONG | Number of completed transactions |
| `total_units_sold` | LONG | Sum of `quantity` |
| `avg_discount_pct` | DOUBLE | Average discount applied |
| `unique_customers` | LONG | Distinct customer count |

---

### 7.2 `revenueByRegionAndCity`

**Purpose:** Geographic revenue heatmap — breakdown by region and city.

**Used by:** Batch (for dashboards and territory planning).

**Logic:**
```
def revenueByRegionAndCity(df: DataFrame, spark: SparkSession): DataFrame = {
  df.filter(col("transaction_status") === "completed")
    .groupBy("region", "city")
    .agg(
      sum("total_amount").alias("total_revenue"),
      count("transaction_id").alias("transaction_count"),
      avg("total_amount").alias("avg_order_value"),
      countDistinct("store_id").alias("active_stores"),
      countDistinct("customer_id").alias("unique_customers")
    )
    .orderBy(desc("total_revenue"))
}
```

---

### 7.3 `paymentMethodAnalysis`

**Purpose:** Understand payment preferences and identify trends across payment methods and card networks.

**Used by:** Batch and streaming.

**Logic:**
```
def paymentMethodAnalysis(df: DataFrame, spark: SparkSession): DataFrame = {
  df.filter(col("transaction_status") === "completed")
    .groupBy("payment_method", "card_network")
    .agg(
      count("transaction_id").alias("transaction_count"),
      sum("total_amount").alias("total_revenue"),
      avg("total_amount").alias("avg_transaction_value"),
      countDistinct("customer_id").alias("unique_customers")
    )
    .withColumn("revenue_share_pct",
      round(col("total_revenue") / sum("total_revenue").over(Window.partitionBy()) * 100, 2)
    )
    .orderBy(desc("transaction_count"))
}
```

---

### 7.4 `customerSegmentation`

**Purpose:** Segment customers by spending behavior for marketing and loyalty programs.

**Used by:** Batch (requires full dataset scan).

**Logic:**
```
def customerSegmentation(df: DataFrame, spark: SparkSession): DataFrame = {
  val customerMetrics = df
    .filter(col("transaction_status") === "completed")
    .groupBy("customer_id", "customer_tier")
    .agg(
      sum("total_amount").alias("lifetime_value"),
      count("transaction_id").alias("total_transactions"),
      avg("total_amount").alias("avg_order_value"),
      max("event_timestamp").alias("last_purchase_date"),
      min("event_timestamp").alias("first_purchase_date"),
      countDistinct("category").alias("category_diversity"),
      sum(when(col("channel") === "mobile_app", 1).otherwise(0)).alias("mobile_orders"),
      sum(when(col("channel") === "web", 1).otherwise(0)).alias("web_orders"),
      sum(when(col("channel") === "pos_in_store", 1).otherwise(0)).alias("instore_orders")
    )

  customerMetrics.withColumn("segment",
    when(col("lifetime_value") >= 10000, "vip")
    .when(col("lifetime_value") >= 5000, "high_value")
    .when(col("lifetime_value") >= 1000, "mid_value")
    .when(col("total_transactions") === 1, "one_time")
    .otherwise("low_value")
  ).orderBy(desc("lifetime_value"))
}
```

---

### 7.5 `fraudDetectionAnalysis`

**Purpose:** Analyze fraud patterns — identify hotspot categories, regions, channels, and time-of-day patterns.

**Used by:** Batch (historical fraud review).

**Logic:**
```
def fraudDetectionAnalysis(df: DataFrame, spark: SparkSession): DataFrame = {
  val fraudDf = df.filter(col("is_fraudulent") === true || col("fraud_score") > 0.7)

  fraudDf.groupBy("category", "region", "channel", "payment_method")
    .agg(
      count("transaction_id").alias("fraud_count"),
      sum("total_amount").alias("fraud_total_amount"),
      avg("fraud_score").alias("avg_fraud_score"),
      avg("total_amount").alias("avg_fraud_amount"),
      countDistinct("customer_id").alias("affected_customers"),
      collect_set("city").alias("affected_cities")
    )
    .orderBy(desc("fraud_count"))
}
```

---

### 7.6 `hourlyTrendAnalysis`

**Purpose:** Time-series breakdown of transactions by hour for capacity planning and staffing.

**Used by:** Batch.

**Logic:**
```
def hourlyTrendAnalysis(df: DataFrame, spark: SparkSession): DataFrame = {
  df.filter(col("transaction_status") === "completed")
    .withColumn("event_date", to_date(col("event_timestamp")))
    .withColumn("event_hour", hour(col("event_timestamp")))
    .groupBy("event_date", "event_hour")
    .agg(
      count("transaction_id").alias("transaction_count"),
      sum("total_amount").alias("hourly_revenue"),
      avg("total_amount").alias("avg_order_value"),
      countDistinct("customer_id").alias("unique_customers"),
      sum("quantity").alias("units_sold")
    )
    .orderBy("event_date", "event_hour")
}
```

---

### 7.7 `channelPerformance`

**Purpose:** Compare performance across sales channels (POS, web, mobile, marketplace, call center).

**Used by:** Batch and streaming.

**Logic:**
```
def channelPerformance(df: DataFrame, spark: SparkSession): DataFrame = {
  df.filter(col("transaction_status") === "completed")
    .groupBy("channel")
    .agg(
      count("transaction_id").alias("transaction_count"),
      sum("total_amount").alias("total_revenue"),
      avg("total_amount").alias("avg_order_value"),
      countDistinct("customer_id").alias("unique_customers"),
      avg("discount_percent").alias("avg_discount"),
      sum(when(col("is_fraudulent") === true, 1).otherwise(0)).alias("fraud_count"),
      avg("fraud_score").alias("avg_fraud_score")
    )
    .withColumn("fraud_rate_pct",
      round(col("fraud_count") / col("transaction_count") * 100, 4)
    )
    .orderBy(desc("total_revenue"))
}
```

---

### 7.8 `realtimeDashboard` (Streaming-Specific)

**Purpose:** Compute sliding-window metrics for a real-time operational dashboard.

**Used by:** Streaming only (requires windowed aggregation with watermark).

**Logic:**
```
def realtimeDashboard(df: DataFrame, spark: SparkSession): DataFrame = {
  df.filter(col("transaction_status").isin("completed", "pending"))
    .withWatermark("event_timestamp", "10 minutes")
    .groupBy(
      window(col("event_timestamp"), "5 minutes", "1 minute"),
      col("category")
    )
    .agg(
      count("transaction_id").alias("txn_count"),
      sum("total_amount").alias("window_revenue"),
      avg("total_amount").alias("avg_order_value"),
      sum(when(col("is_fraudulent") === true, 1).otherwise(0)).alias("fraud_alerts"),
      approx_count_distinct("customer_id").alias("approx_unique_customers")
    )
    .select(
      col("window.start").alias("window_start"),
      col("window.end").alias("window_end"),
      col("category"),
      col("txn_count"),
      col("window_revenue"),
      col("avg_order_value"),
      col("fraud_alerts"),
      col("approx_unique_customers")
    )
}
```

---

### 7.9 `inventoryVelocity`

**Purpose:** Track product sell-through rate for inventory replenishment decisions.

**Used by:** Batch.

**Logic:**
```
def inventoryVelocity(df: DataFrame, spark: SparkSession): DataFrame = {
  df.filter(col("transaction_status") === "completed")
    .groupBy("product_id", "product_name", "category", "sub_category", "brand")
    .agg(
      sum("quantity").alias("total_units_sold"),
      count("transaction_id").alias("order_count"),
      sum("total_amount").alias("total_revenue"),
      avg("unit_price").alias("avg_selling_price"),
      avg("discount_percent").alias("avg_discount"),
      countDistinct("region").alias("regions_sold_in"),
      countDistinct("store_id").alias("stores_sold_in"),
      min("event_timestamp").alias("first_sale"),
      max("event_timestamp").alias("last_sale")
    )
    .withColumn("days_in_range",
      datediff(col("last_sale"), col("first_sale")) + 1
    )
    .withColumn("daily_velocity",
      round(col("total_units_sold") / col("days_in_range"), 2)
    )
    .orderBy(desc("daily_velocity"))
}
```

---

### 7.10 Analysis Type Registry

The `BatchJob` selects the aggregator function based on the `analysisType` string provided in the `POST /batch/run` request:

| `analysisType` Value | Function Called | Description |
|---|---|---|
| `revenue_by_category` | `revenueByCategory` | Category-level revenue breakdown |
| `revenue_by_region` | `revenueByRegionAndCity` | Geographic revenue heatmap |
| `payment_analysis` | `paymentMethodAnalysis` | Payment method trends |
| `customer_segmentation` | `customerSegmentation` | Customer lifetime value & segmentation |
| `fraud_analysis` | `fraudDetectionAnalysis` | Fraud hotspot analysis |
| `hourly_trends` | `hourlyTrendAnalysis` | Time-series hourly breakdown |
| `channel_performance` | `channelPerformance` | Sales channel comparison |
| `inventory_velocity` | `inventoryVelocity` | Product sell-through rate |
| `full_report` | Runs **all** of the above | Combined analysis; writes each to a sub-directory under the batch aggregated path |

---

## 8. Delta Lake Storage Layer

### 8.1 Directory Layout

```
/data/delta/
├── stream/
│   ├── transactions/                      ← Raw streaming events (append-only)
│   │   ├── event_date=2025-01-15/
│   │   ├── event_date=2025-01-16/
│   │   └── _delta_log/
│   └── stream_aggregations/              ← Real-time windowed aggregations
│       ├── window_start=.../
│       └── _delta_log/
├── batches/
│   ├── batch-20250115-a3f8e2b1/
│   │   ├── raw/                          ← Full raw records for this batch
│   │   │   ├── category=electronics/
│   │   │   ├── category=clothing/
│   │   │   └── _delta_log/
│   │   └── aggregated/                   ← Analysis results for this batch
│   │       ├── revenue_by_category/
│   │       ├── customer_segmentation/
│   │       └── _delta_log/
│   ├── batch-20250115-7b2c9d4e/
│   │   ├── raw/
│   │   └── aggregated/
│   └── ...
└── seed/                                  ← Seeded historical data (§10.2)
    └── transactions/
        ├── event_date=2025-01-01/
        ├── event_date=2025-01-02/
        └── _delta_log/
```

### 8.2 Reading Batch Data from Delta Lake

**Used by:** `GET /batch/data/:batchId` and `GET /batch/data/:batchId/aggregated`

The batch retrieval flow is:

1. **Lookup:** Query `BatchRegistry.get(batchId)` to get the `BatchJobRecord`.
2. **Validation:** Ensure `status == "COMPLETED"`. If not, return HTTP 409.
3. **Read Raw Data:**
   ```
   val rawDf = spark.read
     .format("delta")
     .load(record.deltaRawPath.get)     // e.g., /data/delta/batches/{batchId}/raw
   ```
4. **Read Aggregated Data:**
   ```
   val aggDf = spark.read
     .format("delta")
     .load(record.deltaAggPath.get)     // e.g., /data/delta/batches/{batchId}/aggregated
   ```
5. **Pagination:** Apply limit/offset:
   ```
   val page = rawDf
     .withColumn("_row_num", row_number().over(Window.orderBy("event_timestamp")))
     .filter(col("_row_num") > offset && col("_row_num") <= offset + limit)
     .drop("_row_num")
   ```
6. **Serialization:** Convert DataFrame to JSON array:
   ```
   val jsonRows = page.toJSON.collect()
   ```
7. **Response:** Return with pagination metadata.

### 8.3 Delta Table Maintenance (Scheduled)

For production, periodic maintenance should be scheduled (e.g., via a cron batch job or external scheduler):

| Operation | Command | Frequency | Purpose |
|---|---|---|---|
| OPTIMIZE | `OPTIMIZE delta.\`/data/delta/stream/transactions\`` | Every 6 hours | Compact small files from streaming micro-batches into larger Parquet files. |
| VACUUM | `VACUUM delta.\`/data/delta/stream/transactions\` RETAIN 168 HOURS` | Daily | Remove old file versions beyond 7-day retention. |
| Z-ORDER | `OPTIMIZE ... ZORDER BY (customer_id, category)` | Weekly | Co-locate data for common query patterns. |
| DESCRIBE HISTORY | `DESCRIBE HISTORY delta.\`path\`` | On-demand | Audit trail of all operations on a table. |

### 8.4 Time Travel Queries

Delta Lake supports reading historical versions of data. This is useful for debugging or auditing batch results:

```
// Read batch data as it was at a specific version
spark.read.format("delta")
  .option("versionAsOf", 3)
  .load("/data/delta/batches/{batchId}/raw")

// Read batch data as it was at a specific timestamp
spark.read.format("delta")
  .option("timestampAsOf", "2025-01-15T14:00:00Z")
  .load("/data/delta/batches/{batchId}/raw")
```

---

## 9. API Reference

### 9.1 Streaming Endpoints

| Method | Endpoint | Request Body | Response | Description |
|---|---|---|---|---|
| `POST` | `/stream/start` | `{ "topic": "transactions-stream" }` | `200 { "status": "started", "queryIds": ["q1", "q2"] }` | Start the Kafka streaming query and real-time aggregation query. |
| `POST` | `/stream/stop` | — | `200 { "status": "stopped", "queriesStopped": 2 }` | Gracefully stop all active streaming queries (drains current micro-batch). |
| `GET` | `/stream/status` | — | `200 { "active": true, "queries": [...] }` | Return status of all streaming queries including `lastProgress` and `recentProgress`. |

### 9.2 Batch Endpoints

| Method | Endpoint | Request Body | Response | Description |
|---|---|---|---|---|
| `POST` | `/batch/run` | See below | `202 { "batchId": "...", "status": "PENDING" }` | Submit an asynchronous batch job. Returns immediately with a `batchId`. |
| `GET` | `/batch/status/:batchId` | — | `200 { "batchId": "...", "status": "COMPLETED", ... }` | Poll the lifecycle status of a batch job. Returns the full `BatchJobRecord`. |
| `GET` | `/batch/data/:batchId` | Query: `?limit=100&offset=0` | `200 { "batchId": "...", "rowCount": N, "page": {...}, "data": [...] }` | Retrieve raw batch data from Delta Lake by batch ID with pagination. |
| `GET` | `/batch/data/:batchId/aggregated` | Query: `?limit=100&offset=0` | `200 { "batchId": "...", "analysisType": "...", "data": [...] }` | Retrieve aggregated analysis results from Delta Lake by batch ID. |
| `GET` | `/batch/list` | Query: `?status=COMPLETED&limit=20` | `200 { "total": N, "batches": [...] }` | List all batch jobs, optionally filtered by status. |

#### `POST /batch/run` Request Body

```
{
  "topic": "transactions-batch",
  "startOffset": "earliest",
  "endOffset": "latest",
  "analysisType": "revenue_by_category"
}
```

**Field Details:**

| Field | Type | Required | Description |
|---|---|---|---|
| `topic` | string | Yes | Kafka topic to read from. |
| `startOffset` | string | Yes | `"earliest"` or JSON offset map `{"0":100,"1":200,...}` |
| `endOffset` | string | Yes | `"latest"` or JSON offset map `{"0":500,"1":600,...}` |
| `analysisType` | string | Yes | One of the registered analysis types from §7.10. |

#### `GET /batch/status/:batchId` Response Body

```
{
  "batchId": "batch-20250115-a3f8e2b1",
  "status": "COMPLETED",
  "analysisType": "revenue_by_category",
  "topic": "transactions-batch",
  "startOffset": "earliest",
  "endOffset": "latest",
  "submitTime": "2025-01-15T14:00:00Z",
  "startTime": "2025-01-15T14:00:01Z",
  "endTime": "2025-01-15T14:02:35Z",
  "rowCount": 1584230,
  "deltaRawPath": "/data/delta/batches/batch-20250115-a3f8e2b1/raw",
  "deltaAggPath": "/data/delta/batches/batch-20250115-a3f8e2b1/aggregated",
  "errorMessage": null
}
```

#### `GET /batch/data/:batchId` Response Body

```
{
  "batchId": "batch-20250115-a3f8e2b1",
  "status": "COMPLETED",
  "rowCount": 1584230,
  "page": {
    "limit": 100,
    "offset": 0,
    "returned": 100,
    "hasMore": true
  },
  "data": [
    {
      "transaction_id": "a3f8e2b1-7c44-4d12-9e56-1a2b3c4d5e6f",
      "event_timestamp": "2025-01-15T14:32:07.123Z",
      "customer_id": "CUST-00042871",
      "category": "electronics",
      "total_amount": 594.98,
      ...
    },
    ...
  ]
}
```

#### `GET /batch/data/:batchId/aggregated` Response Body (example for `revenue_by_category`)

```
{
  "batchId": "batch-20250115-a3f8e2b1",
  "analysisType": "revenue_by_category",
  "data": [
    {
      "category": "electronics",
      "total_revenue": 4523891.45,
      "avg_order_value": 287.34,
      "transaction_count": 15742,
      "total_units_sold": 28456,
      "avg_discount_pct": 8.7,
      "unique_customers": 12893
    },
    {
      "category": "clothing",
      "total_revenue": 2871456.78,
      "avg_order_value": 89.12,
      "transaction_count": 32214,
      "total_units_sold": 67890,
      "avg_discount_pct": 22.3,
      "unique_customers": 24567
    },
    ...
  ]
}
```

### 9.3 Control Endpoints

| Method | Endpoint | Request Body | Response | Description |
|---|---|---|---|---|
| `GET` | `/health` | — | `200 { "status": "healthy", "spark": true, "uptime": "2h 34m" }` | Liveness/readiness probe for Kubernetes. |
| `POST` | `/stop` | — | `200 { "status": "shutting_down" }` | Gracefully shut down the entire application. |

### 9.4 Error Response Format

All error responses follow a consistent format:

```
{
  "error": "BATCH_NOT_FOUND",
  "message": "No batch job found with ID: batch-xyz",
  "timestamp": "2025-01-15T14:32:07.123Z"
}
```

| HTTP Status | Error Code | When |
|---|---|---|
| 400 | `INVALID_REQUEST` | Malformed JSON, missing required fields, unknown `analysisType`. |
| 404 | `BATCH_NOT_FOUND` | `batchId` not in registry. |
| 409 | `BATCH_NOT_READY` | Requesting data for a batch that is still `PENDING` or `RUNNING`. |
| 500 | `INTERNAL_ERROR` | Unexpected Spark/Delta/Kafka exception. |
| 503 | `SERVICE_UNAVAILABLE` | SparkSession is stopped or unhealthy. |

---

## 10. Dummy Data Generation & Seeding

### 10.1 Kafka Stream Generator (`scripts/kafka_stream_generator.py`)

This Python script continuously produces realistic `TransactionEvent` JSON records to the Kafka `transactions-stream` topic. It simulates real-world traffic patterns with configurable throughput.

**Dependencies:** `pip install kafka-python faker`

**Behavior:**
- Generates events at a configurable rate (default: 10 events/second).
- Randomizes all fields within realistic distributions.
- Simulates time-of-day traffic patterns (higher volume during business hours).
- Injects ~2% fraudulent transactions with elevated `fraud_score`.
- Runs indefinitely until Ctrl+C.

**Configuration (environment variables):**

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `transactions-stream` | Target topic |
| `EVENTS_PER_SECOND` | `10` | Throughput rate |
| `FRAUD_RATE` | `0.02` | Fraction of events flagged as fraudulent |
| `NUM_CUSTOMERS` | `5000` | Size of the customer pool |
| `NUM_PRODUCTS` | `500` | Size of the product catalog |
| `NUM_STORES` | `50` | Size of the store pool |

**Data Distribution Configuration:**

```
CATEGORIES = {
    "electronics":  {"weight": 0.20, "price_range": (29.99, 2499.99), "sub_cats": ["smartphones", "laptops", "audio_headphones", "cameras", "tablets", "wearables"]},
    "clothing":     {"weight": 0.18, "price_range": (9.99, 299.99),  "sub_cats": ["mens_shirts", "womens_dresses", "shoes", "activewear", "accessories"]},
    "grocery":      {"weight": 0.22, "price_range": (1.99, 89.99),   "sub_cats": ["dairy", "snacks", "beverages", "frozen", "organic", "bakery"]},
    "home_garden":  {"weight": 0.10, "price_range": (14.99, 999.99), "sub_cats": ["furniture", "kitchen", "bedding", "garden_tools", "lighting"]},
    "sports":       {"weight": 0.08, "price_range": (19.99, 799.99), "sub_cats": ["fitness", "outdoor", "team_sports", "cycling", "swimming"]},
    "beauty":       {"weight": 0.07, "price_range": (4.99, 199.99),  "sub_cats": ["skincare", "makeup", "haircare", "fragrance", "supplements"]},
    "automotive":   {"weight": 0.04, "price_range": (9.99, 499.99),  "sub_cats": ["parts", "accessories", "tools", "car_care", "electronics"]},
    "books":        {"weight": 0.05, "price_range": (5.99, 49.99),   "sub_cats": ["fiction", "non_fiction", "textbooks", "children", "comics"]},
    "toys":         {"weight": 0.03, "price_range": (7.99, 199.99),  "sub_cats": ["action_figures", "board_games", "educational", "outdoor", "dolls"]},
    "pharmacy":     {"weight": 0.03, "price_range": (2.99, 149.99),  "sub_cats": ["otc_medicine", "vitamins", "first_aid", "personal_care", "baby_care"]},
}

REGIONS = {
    "north":     {"cities": ["Delhi", "Chandigarh", "Lucknow", "Jaipur"], "weight": 0.22},
    "south":     {"cities": ["Bangalore", "Chennai", "Hyderabad", "Kochi"], "weight": 0.25},
    "east":      {"cities": ["Kolkata", "Patna", "Bhubaneswar", "Guwahati"], "weight": 0.12},
    "west":      {"cities": ["Mumbai", "Pune", "Ahmedabad", "Goa"], "weight": 0.28},
    "central":   {"cities": ["Bhopal", "Nagpur", "Indore", "Raipur"], "weight": 0.08},
    "northeast": {"cities": ["Guwahati", "Imphal", "Shillong", "Agartala"], "weight": 0.05},
}

PAYMENT_METHODS = ["credit_card", "debit_card", "upi", "wallet", "bank_transfer", "cod", "gift_card"]
PAYMENT_WEIGHTS = [0.30, 0.20, 0.25, 0.10, 0.05, 0.08, 0.02]

CHANNELS = ["pos_in_store", "web", "mobile_app", "marketplace", "call_center"]
CHANNEL_WEIGHTS = [0.25, 0.20, 0.35, 0.15, 0.05]

CUSTOMER_TIERS = ["bronze", "silver", "gold", "platinum"]
TIER_WEIGHTS = [0.50, 0.30, 0.15, 0.05]
```

**Algorithm:**
1. On startup, pre-generate pools:
   - `customers[]`: 5000 customer IDs with assigned tiers, home regions, preferred channels.
   - `products[]`: 500 product entries with assigned categories, sub-categories, brands, base prices.
   - `stores[]`: 50 store IDs with assigned regions and cities.
2. Main loop (runs at `EVENTS_PER_SECOND` rate):
   - Pick a random customer from the pool.
   - Pick a random product (weighted by category distribution).
   - Pick a store (or null for digital channels).
   - Compute `quantity` (weighted: 70% qty=1, 20% qty=2, 10% qty=3-5).
   - Compute `discount_percent` (0% for 60% of txns, 5-15% for 30%, 20-50% for 10%).
   - Compute `total_amount = quantity * unit_price * (1 - discount/100)`.
   - Compute `tax_amount = total_amount * 0.09` (simulated 9% tax).
   - Set `transaction_status`: 92% completed, 4% pending, 2% failed, 1.5% refunded, 0.5% chargeback.
   - For fraud: if `random() < FRAUD_RATE`, set `is_fraudulent=true`, `fraud_score=random(0.7, 1.0)`. Otherwise `fraud_score=random(0.0, 0.15)`.
   - Serialize to JSON, send to Kafka with `transaction_id` as the message key (ensures ordering per transaction).

**Usage:**
```
cd scripts/
pip install kafka-python faker
python kafka_stream_generator.py

# Or with custom settings:
EVENTS_PER_SECOND=50 KAFKA_TOPIC=transactions-stream python kafka_stream_generator.py
```

---

### 10.2 Delta Lake Historical Seeder (`scripts/delta_lake_seeder.py`)

This PySpark script generates a large volume of historical transaction data and writes it directly to Delta Lake. This seeds the `seed/transactions` table for batch analysis without requiring Kafka.

**Dependencies:** `pip install pyspark delta-spark faker`

**Behavior:**
- Generates a configurable number of historical records (default: 1,000,000).
- Spans a configurable date range (default: last 90 days).
- Uses the exact same `TransactionEvent` schema as the Kafka generator.
- Writes to Delta format at `/data/delta/seed/transactions/` partitioned by `event_date`.
- Also optionally seeds the Kafka `transactions-batch` topic for batch-from-Kafka testing.

**Configuration (environment variables):**

| Variable | Default | Description |
|---|---|---|
| `TOTAL_RECORDS` | `1000000` | Number of records to generate |
| `DATE_RANGE_DAYS` | `90` | How many days of history to generate |
| `DELTA_OUTPUT_PATH` | `/data/delta/seed/transactions` | Delta Lake output path |
| `SEED_KAFKA` | `false` | If `true`, also write records to Kafka topic |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker (if `SEED_KAFKA=true`) |
| `KAFKA_BATCH_TOPIC` | `transactions-batch` | Kafka topic (if `SEED_KAFKA=true`) |
| `SPARK_MASTER` | `local[*]` | Spark master URL |

**Algorithm:**
1. Initialize a local PySpark session with Delta Lake extensions.
2. Generate the same customer/product/store pools as the Kafka generator (§10.1) for consistency.
3. For each record:
   - Assign a random `event_timestamp` within the date range (weighted toward business hours 8 AM - 10 PM).
   - Follow the same category/region/payment distributions as §10.1.
   - Apply time-based seasonality: weekends have 40% more grocery/clothing, weekdays have 30% more electronics.
4. Create a PySpark DataFrame from the generated records.
5. Write to Delta:
   ```
   df.withColumn("event_date", F.to_date("event_timestamp"))
     .write
     .format("delta")
     .partitionBy("event_date")
     .mode("overwrite")
     .save(delta_output_path)
   ```
6. Print summary statistics:
   - Total records written
   - Records per category
   - Date range covered
   - Average order value
   - Fraud rate

**Usage:**
```
cd scripts/

# Generate 1M records to Delta Lake
python delta_lake_seeder.py

# Generate 5M records spanning 180 days, also seed Kafka
TOTAL_RECORDS=5000000 DATE_RANGE_DAYS=180 SEED_KAFKA=true python delta_lake_seeder.py
```

---

### 10.3 Docker Compose for Local Development

The `docker-compose.yml` should be extended to support the full local development stack:

```
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_SYNC_LIMIT: 2
      ZOOKEEPER_INIT_LIMIT: 5
    ports:
      - "2181:2181"
    healthcheck:
      test: ["CMD", "echo", "ruok", "|", "nc", "127.0.0.1", "2181"]
      interval: 10s
      timeout: 5s
      retries: 5

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
      KAFKA_NUM_PARTITIONS: 12
      KAFKA_LOG_RETENTION_HOURS: 168
    depends_on:
      zookeeper:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "kafka-broker-api-versions", "--bootstrap-server", "localhost:9092"]
      interval: 10s
      timeout: 5s
      retries: 5
    volumes:
      - kafka-data:/var/lib/kafka/data

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    ports:
      - "8090:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092
      KAFKA_CLUSTERS_0_ZOOKEEPER: zookeeper:2181
    depends_on:
      kafka:
        condition: service_healthy

volumes:
  kafka-data:
```

---

### 10.4 Quick Start Guide (Local Development)

```
# 1. Start infrastructure
docker-compose up -d
docker ps   # Verify Kafka, Zookeeper, and Kafka-UI are healthy

# 2. Open Kafka UI at http://localhost:8090 to monitor topics

# 3. Seed Delta Lake with historical data
cd scripts/
pip install pyspark delta-spark faker
python delta_lake_seeder.py

# 4. Start the Kafka stream generator (in a separate terminal)
pip install kafka-python faker
python kafka_stream_generator.py

# 5. Build and run the unified pipeline
cd ..
export JAVA_HOME=$(/usr/libexec/java_home -v 17 2>/dev/null || echo $JAVA_HOME)
sbt assembly
spark-submit --class com.pipeline.Main target/scala-2.13/UnifiedTransactionPipeline-assembly-1.0.0.jar

# 6. Interact with the API
# Start streaming
curl -X POST http://localhost:8080/stream/start \
  -H "Content-Type: application/json" \
  -d '{"topic": "transactions-stream"}'

# Submit a batch job
curl -X POST http://localhost:8080/batch/run \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "transactions-batch",
    "startOffset": "earliest",
    "endOffset": "latest",
    "analysisType": "revenue_by_category"
  }'
# Returns: {"batchId": "batch-20250115-a3f8e2b1", "status": "PENDING"}

# Check batch status
curl http://localhost:8080/batch/status/batch-20250115-a3f8e2b1

# Get raw batch data (paginated)
curl "http://localhost:8080/batch/data/batch-20250115-a3f8e2b1?limit=50&offset=0"

# Get aggregated results
curl http://localhost:8080/batch/data/batch-20250115-a3f8e2b1/aggregated

# List all batches
curl "http://localhost:8080/batch/list?status=COMPLETED"

# Health check
curl http://localhost:8080/health

# Stop streaming
curl -X POST http://localhost:8080/stream/stop

# Graceful shutdown
curl -X POST http://localhost:8080/stop
```

---

## 11. Key Dependencies (`build.sbt`)

| Library | Version | Purpose |
|---|---|---|
| `org.apache.spark:spark-core` | 3.5.1 | Core Spark runtime (distributed computing engine) |
| `org.apache.spark:spark-sql` | 3.5.1 | DataFrame / SQL API for structured data processing |
| `org.apache.spark:spark-sql-kafka-0-10` | 3.5.1 | Kafka source/sink connector for both streaming and batch reads |
| `io.delta:delta-spark` | 3.1.0 | Delta Lake ACID transactions, time travel, schema enforcement |
| `org.apache.pekko:pekko-http` | 1.0.1 | Embedded async HTTP server for REST API |
| `org.apache.pekko:pekko-http-spray-json` | 1.0.1 | JSON marshalling/unmarshalling for Pekko HTTP routes |
| `org.apache.pekko:pekko-actor-typed` | 1.0.2 | Actor system required by Pekko HTTP |
| `org.apache.pekko:pekko-stream` | 1.0.2 | Reactive streams for HTTP request/response handling |
| `io.circe:circe-core` | 0.14.6 | Functional JSON library for Scala |
| `io.circe:circe-generic` | 0.14.6 | Automatic derivation of JSON codecs from case classes |
| `io.circe:circe-parser` | 0.14.6 | JSON string parsing |
| `com.typesafe:config` | 1.4.3 | Typesafe application configuration (HOCON format) |
| `ch.qos.logback:logback-classic` | 1.4.14 | Structured logging backend |
| `com.typesafe.scala-logging:scala-logging` | 3.9.5 | Scala-idiomatic logging facade |
| `org.scalatest:scalatest` | 3.2.17 | Unit and integration testing |
| `org.apache.pekko:pekko-http-testkit` | 1.0.1 | HTTP route testing utilities |
| `com.eed3si9n:sbt-assembly` (plugin) | 2.1.5 | Fat JAR packaging for deployment |

### Python Dependencies (for scripts)

| Library | Version | Purpose |
|---|---|---|
| `kafka-python` | 2.0.2+ | Kafka producer for stream generator |
| `faker` | 22.0+ | Realistic fake data generation (names, addresses, etc.) |
| `pyspark` | 3.5.1 | PySpark for Delta Lake seeder script |
| `delta-spark` | 3.1.0 | Delta Lake support in PySpark seeder |

---

## 12. Anticipated Challenges & Mitigations

| Challenge | Mitigation |
|---|---|
| **Resource contention under heavy batch load** | Use Scheduler Pools with higher `weight` (3) and `minShare` (4) for the streaming pool. Monitor with Spark UI (`/stages`, `/jobs`). Set `batch-pool` minShare to 1 so batch jobs never fully starve but always yield to streaming. |
| **Kafka offset management for batch** | Let the client specify exact offsets via the API. For automated runs, query the batch Delta table for `MAX(event_timestamp)` to compute the next starting offset and avoid reprocessing. |
| **Thread safety of SparkSession** | `SparkSession` is thread-safe for `read`/`readStream` operations. However, `setLocalProperty` is thread-local, so each batch thread must set its own pool assignment *before* any Spark action. The `BatchJob.run()` method sets this as its first operation. |
| **Late-arriving data in streaming** | Use Spark's `.withWatermark("event_timestamp", "10 minutes")` to bound state and avoid unbounded memory growth. Late events beyond the watermark are dropped. |
| **Graceful drain on shutdown** | Call `query.stop()` before `spark.stop()` to let the streaming query commit its final micro-batch and advance offsets cleanly. Await in-flight batch futures with a 60-second timeout. |
| **Delta Lake small file problem** | Streaming micro-batches create many small Parquet files. Schedule `OPTIMIZE` operations every 6 hours. Enable `spark.databricks.delta.optimizeWrite.enabled=true` for auto-compaction. |
| **Batch result storage growth** | Implement a retention policy: a background thread or cron job deletes batch directories older than N days (configurable, default 7). Alternatively, `VACUUM` the batch Delta tables. |
| **Batch Registry loss on restart** | On application startup, scan `delta://batches/` for existing batch directories. Rebuild the in-memory registry from Delta `_delta_log` metadata (commit timestamps, row counts). Mark recovered batches as `COMPLETED` (or `UNKNOWN` if log is incomplete). |
| **JSON serialization of large DataFrames** | For `GET /batch/data/:batchId`, enforce pagination (max page size = 10,000). Use Spark's `toJSON` which streams row-by-row rather than materializing the entire DataFrame. Consider NDJSON (newline-delimited JSON) for large responses. |
| **Concurrent batch jobs exhausting memory** | Cap the batch thread pool at 8 concurrent jobs. Each batch job sets `spark.scheduler.pool=batch-pool` with `minShare=1`, so executor resources are shared. Monitor via the Spark UI and alert if batch queue depth exceeds threshold. |
| **Schema evolution** | Delta Lake supports schema evolution via `mergeSchema=true`. If new fields are added to `TransactionEvent`, existing Delta tables auto-evolve. Old columns not present in new data are set to `null`. |

---

## 13. Success Criteria

| Criterion | How It Is Demonstrated |
|---|---|
| **Uniformity** | Single `main()` in `Main.scala` handles both streaming and batch via HTTP API. All data flows through the same `TransactionEvent` schema. |
| **Stability** | A deliberately failed batch job (e.g., bad offsets) returns an HTTP 500 via `GET /batch/status/:batchId` with error details; the streaming query continues uninterrupted. |
| **Kafka Versatility** | The same `spark-sql-kafka-0-10` connector is used for both `readStream` and bounded `read` within the same session. |
| **Control** | `POST /stop` cleanly drains the stream, writes final micro-batch, stops the session, and terminates the JVM — no zombie processes. |
| **Concurrency** | Three simultaneous `POST /batch/run` calls all execute and complete while the streaming query continues processing at its normal throughput. |
| **Batch Retrieval** | After a batch job completes, `GET /batch/data/:batchId` returns raw records and `GET /batch/data/:batchId/aggregated` returns analysis results, both read from Delta Lake. |
| **Analysis Consistency** | The `revenueByCategory` aggregator produces identical results whether invoked from a streaming micro-batch or a historical batch of 10M records. |
| **Delta Lake ACID** | Concurrent streaming writes and batch reads to the same Delta table do not corrupt data. Time-travel queries on batch results return consistent historical snapshots. |
| **Data Quality** | The Kafka stream generator produces realistic, schema-compliant data with proper distributions. The Delta seeder pre-populates 90+ days of history for immediate batch testing. |
| **Observability** | Spark UI at `:4040` shows both streaming and batch jobs in separate scheduler pools. `GET /health` returns service status. `GET /batch/list` provides full audit trail of all batch runs. |