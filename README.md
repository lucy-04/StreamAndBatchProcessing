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
| Java | 17+ | `brew install temurin@17` |
| sbt | 1.11+ | `brew install sbt` |
| Docker | 24+ | `brew install --cask docker` |
| uv | Latest | `brew install uv` |

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
│   ├── application.conf                   ← Production config
│   └── fairscheduler.xml                  ← FAIR scheduler pools
├── scripts/
│   ├── pyproject.toml                     ← UV Python project config
│   ├── kafka_stream_generator.py          ← Realistic Kafka event producer
│   └── delta_lake_seeder.py               ← Historical data seeder (PySpark)
├── docker/Dockerfile                      ← Production container image
├── docker-compose.yml                     ← Kafka + Zookeeper + Kafka UI
└── AGENTS.md                              ← Full architecture specification
```

---

## Quick Start (Local Development)

### 1. Start Infrastructure

```bash
docker-compose up -d
docker ps   # ✅ Verify Kafka, Zookeeper, and Kafka-UI are healthy
```

Open **Kafka UI** at [http://localhost:8090](http://localhost:8090) to monitor topics.

### 2. Seed Delta Lake with Historical Data

```bash
cd scripts/
uv sync
uv run python delta_lake_seeder.py
```

This writes ~1M records to `data/delta/stream/transactions/` (project-relative) so batch processing works immediately.

**Custom settings:**
```bash
TOTAL_RECORDS=5000000 DATE_RANGE_DAYS=180 uv run python delta_lake_seeder.py
```

### 3. Start the Kafka Stream Generator

In a **separate terminal**:
```bash
cd scripts/
uv run python kafka_stream_generator.py
```

Produces ~10 events/sec by default. Customize with:
```bash
EVENTS_PER_SECOND=50 uv run python kafka_stream_generator.py
```

### 4. Build and Run the Pipeline

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17 2>/dev/null || echo $JAVA_HOME)
sbt assembly
spark-submit \
  --class com.pipeline.Main \
  --master "local[*]" \
  --driver-memory 4g \
  target/scala-2.13/UnifiedTransactionPipeline-assembly-1.0.0.jar
```

### 5. Interact with the API

**Start streaming** (Kafka → Delta Lake):
```bash
curl -X POST http://localhost:8080/stream/start \
  -H "Content-Type: application/json" \
  -d '{"topic": "transactions-stream"}'
```

**Submit a batch job** (reads from Delta Lake):
```bash
curl -X POST http://localhost:8080/batch/run \
  -H "Content-Type: application/json" \
  -d '{
    "startDate": "2025-01-01",
    "endDate": "2025-01-15",
    "analysisType": "revenue_by_category"
  }'
```

**Submit with filters:**
```bash
curl -X POST http://localhost:8080/batch/run \
  -H "Content-Type: application/json" \
  -d '{
    "startDate": "2025-01-01",
    "endDate": "2025-01-15",
    "analysisType": "customer_segmentation",
    "filters": {
      "category": "electronics",
      "region": "west"
    }
  }'
```

**Check batch status:**
```bash
curl http://localhost:8080/batch/status/<batchId>
```

**Get raw batch data** (paginated):
```bash
curl "http://localhost:8080/batch/data/<batchId>?limit=50&offset=0"
```

**Get aggregated results:**
```bash
curl http://localhost:8080/batch/data/<batchId>/aggregated
```

**List all batches:**
```bash
curl "http://localhost:8080/batch/list?status=COMPLETED"
```

**Health check:**
```bash
curl http://localhost:8080/health
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
| `GET` | `/batch/data/:batchId` | Retrieve raw batch data (paginated) |
| `GET` | `/batch/data/:batchId/aggregated` | Retrieve aggregated analysis results |
| `GET` | `/batch/list` | List all batch jobs (filter by `?status=`) |

### Control Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/health` | Liveness probe (Spark status + uptime) |
| `POST` | `/stop` | Graceful shutdown |

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

Configuration uses [Typesafe Config](https://github.com/lightbend/config) (HOCON format).

| Config Path | Default | Description |
|-------------|---------|-------------|
| `pipeline.http.port` | `8080` | HTTP server port |
| `pipeline.spark.master` | `local[*]` | Spark master URL |
| `pipeline.kafka.bootstrap-servers` | `localhost:9092` | Kafka broker |
| `pipeline.kafka.stream-topic` | `transactions-stream` | Kafka topic |
| `pipeline.delta.base-path` | `data/delta` | Delta Lake root |
| `pipeline.batch.thread-pool-size` | `8` | Max concurrent batch jobs |
| `pipeline.batch.default-page-size` | `100` | Default pagination size |

Override via `conf/application.conf` or environment variables.

---

## Docker

### Build Production Image

```bash
sbt assembly
docker build -f docker/Dockerfile -t unified-pipeline:1.0.0 .
docker run -p 8080:8080 -p 4040:4040 unified-pipeline:1.0.0
```

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
| **Python Scripts** | uv + kafka-python + Faker + PySpark |
| **Infrastructure** | Docker Compose (Kafka + ZK + Kafka UI) |
