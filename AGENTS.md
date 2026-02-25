# Unified Batch & Streaming Pipeline — Architecture & Agent Guide

This document describes the end-to-end architecture and implementation plan for building an industry-scale unified batch and streaming data pipeline using Apache Kafka and Apache Spark (Scala). It serves as the primary reference for all agents working in this repository.

---

## 1. Executive Summary

The goal is to build an **always-on, long-lived Apache Spark application** written in Scala that seamlessly bridges real-time and historical data processing. By embedding an HTTP server within the Spark Driver and leveraging Spark's **FAIR Scheduler**, a single `SparkSession` will:

- Continuously consume streaming data from Kafka.
- Simultaneously serve on-demand batch processing requests against the same (or different) Kafka topics.
- Output results to an industry-standard storage layer (Delta Lake / JDBC).

The application behaves like a **service**, not a traditional job — it starts once and runs indefinitely until a explicit stop signal is received.

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

---

## 3. High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Apache Kafka Cluster                      │
│   ┌──────────────────┐          ┌──────────────────────────┐    │
│   │  streaming-topic  │          │      batch-topic(s)       │    │
│   └────────┬─────────┘          └────────────┬─────────────┘    │
└────────────│────────────────────────────────│─────────────────  ┘
             │ (infinite subscribe)            │ (bounded offset range)
             ▼                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Spark Driver (JVM Process)                     │
│                                                                   │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │              Embedded HTTP Server (Pekko HTTP)           │   │
│   │   POST /stream/start   POST /batch/run   POST /stop      │   │
│   └──────────────────────────┬──────────────────────────────┘   │
│                               │                                   │
│   ┌───────────────────────────▼──────────────────────────────┐  │
│   │          Single SparkSession (FAIR Scheduler)             │  │
│   │                                                           │  │
│   │   ┌─────────────────────┐  ┌──────────────────────────┐  │  │
│   │   │   Streaming Query   │  │  Batch Job(s) (threads)  │  │  │
│   │   │  (background async) │  │  (concurrent, on-demand) │  │  │
│   │   └─────────────────────┘  └──────────────────────────┘  │  │
│   └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
             │                                 │
             ▼                                 ▼
┌────────────────────────┐      ┌──────────────────────────────┐
│   Delta Lake / Iceberg  │      │   JDBC Sink (PostgreSQL etc) │
│   (streaming sink)      │      │   (batch sink)               │
└────────────────────────┘      └──────────────────────────────┘
```

### Component Responsibilities

| Component | Role |
|---|---|
| **Apache Kafka** | Central nervous system. Decouples producers from consumers. Retains data per retention policy (e.g., 7 days), enabling batch replay of historical data. |
| **Embedded HTTP Server (Pekko HTTP)** | Keeps the Driver JVM alive indefinitely. Accepts REST commands to start/stop stream and trigger batch jobs. |
| **SparkSession (FAIR Scheduler)** | Single entry point for all Spark operations. `spark.scheduler.mode=FAIR` ensures streaming and batch jobs share resources concurrently. |
| **Delta Lake / Iceberg** | ACID-compliant Lakehouse sink. Allows streaming and batch jobs to write to the same tables without data corruption. |

---

## 4. How the Architecture Meets Each Requirement

### Requirement 1: Integrate Apache Spark and Apache Kafka
**Solution:** Use the official `spark-sql-kafka-0-10` connector. This allows Spark to read from Kafka natively as either a `DataStreamReader` (streaming) or a bounded `DataFrame` (batch) using the same API surface.

### Requirement 2 & 3: Single Context + Multiple Concurrent Batches
**Solution:** The `SparkSession` is instantiated **once** at application startup as a shared object. The HTTP server handles each `/batch/run` request on a separate JVM thread. Each thread uses the same session to execute a `spark.read.format("kafka")...load()` call independently, without blocking the streaming query.

### Requirement 4: Live Until Stop Is Invoked
**Solution:** Pekko HTTP's `bindAndHandle` blocks the main thread by returning a `Future` that never completes until explicitly unbound. The application lifecycle is:
1. Initialize `SparkSession`.
2. Bind the HTTP server (main thread parks here).
3. On `POST /stop`: stop all active streaming queries, call `spark.stop()`, unbind the server.

### Requirement 5: FAIR Scheduler for Non-Blocking Concurrency
**Solution:** `spark.scheduler.mode=FAIR` causes the cluster to interleave the streaming micro-batch jobs and batch jobs, allocating CPU cores to both simultaneously. Spark **Scheduler Pools** can be used to assign higher weight/minimum shares to the streaming pool, guaranteeing streaming SLAs under heavy batch load.

---

## 5. Implementation Plan

### Phase 1: Infrastructure & Configuration

#### Kafka Setup
- Deploy a highly available Kafka cluster (or use managed: Confluent Cloud, Amazon MSK, Azure Event Hubs).
- Create topics with partition counts matching the number of Spark executor cores for optimal parallelism.
- Set retention to at least 7 days to allow meaningful batch replays.

#### Critical Spark Configuration

```
spark.scheduler.mode=FAIR
spark.sql.streaming.checkpointLocation=s3a://your-bucket/checkpoints/
spark.dynamicAllocation.enabled=true
spark.dynamicAllocation.minExecutors=2
spark.dynamicAllocation.maxExecutors=20
spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
```

For Scheduler Pools, create a `fairscheduler.xml`:
```
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

### Phase 2: Application Development (Scala)

#### Project Structure

```
StreamAndBatchProcessing/
├── build.sbt
├── project/
│   └── plugins.sbt
├── src/
│   └── main/
│       └── scala/
│           └── com/pipeline/
│               ├── Main.scala               ← Entry point; starts HTTP server
│               ├── SparkManager.scala        ← Singleton SparkSession
│               ├── routes/
│               │   ├── StreamRoutes.scala    ← /stream/start, /stream/stop
│               │   ├── BatchRoutes.scala     ← /batch/run
│               │   └── ControlRoutes.scala   ← /stop
│               ├── jobs/
│               │   ├── StreamingJob.scala    ← Streaming query logic
│               │   └── BatchJob.scala        ← Bounded batch read/write logic
│               └── config/
│                   └── AppConfig.scala       ← Typesafe Config wrapper
├── conf/
│   ├── application.conf
│   └── fairscheduler.xml
└── docker/
    └── Dockerfile
```

#### Key Implementation Details

**`SparkManager.scala` — Singleton Session**
- Initialize `SparkSession` with FAIR scheduler and Delta Lake extensions.
- Expose as a `lazy val` so initialization is deferred until first use and guaranteed to happen only once.

**`StreamingJob.scala` — Kafka Streaming Read**
- Read from Kafka via `spark.readStream.format("kafka")`.
- Deserialize value bytes (e.g., JSON or Avro).
- Apply transformations (filtering, aggregations, windowing).
- Write to Delta Lake sink using `.writeStream.format("delta").outputMode("append").option("checkpointLocation", ...).start()`.
- Assign the query to the `streaming-pool` scheduler pool via `spark.sparkContext.setLocalProperty("spark.scheduler.pool", "streaming-pool")`.
- Return the `StreamingQuery` handle for lifecycle management.

**`BatchJob.scala` — Kafka Bounded Batch Read**
- Accept parameters: `topic`, `startingOffsets` (JSON string or `earliest`), `endingOffsets` (JSON string or `latest`).
- Set `spark.sparkContext.setLocalProperty("spark.scheduler.pool", "batch-pool")` on the executing thread.
- Read via `spark.read.format("kafka").option("startingOffsets", start).option("endingOffsets", end).load()`.
- Apply business transformations.
- Write to JDBC or Delta Lake via `.write.format("jdbc").mode("append").save()` or `.write.format("delta").mode("append").save(path)`.

**`StreamRoutes.scala` — HTTP Endpoints**
- `POST /stream/start` → calls `StreamingJob.start()`, stores the `StreamingQuery` reference.
- `POST /stream/stop` → calls `.stop()` on the stored query reference.

**`BatchRoutes.scala` — HTTP Endpoints**
- `POST /batch/run` with JSON body `{ "topic": "...", "startOffset": "...", "endOffset": "..." }`.
- Submits `BatchJob.run(params)` on a **new Future** (separate thread pool) so the HTTP response returns immediately (async).
- Returns a `jobId` for status tracking.

**`ControlRoutes.scala` — Shutdown Endpoint**
- `POST /stop` → stops all active streaming queries, calls `spark.stop()`, triggers HTTP server unbind.

### Phase 3: Productionization & Deployment

#### Containerization (Docker)

```
# Build fat JAR
sbt assembly

# Package into Docker image containing Spark binaries + fat JAR
docker build -f docker/Dockerfile -t unified-pipeline:latest .
```

#### Kubernetes Deployment (Spark Operator)

- Deploy using the [Spark on Kubernetes Operator](https://github.com/kubeflow/spark-operator).
- The **Driver pod** exposes a `Kubernetes Service` (ClusterIP or LoadBalancer) so external systems can call the REST endpoints.
- **Executor pods** scale dynamically via `spark.dynamicAllocation.enabled=true` and the Kubernetes External Shuffle Service.
- Store checkpoints on S3 (or GCS/ADLS) so that if the Driver pod restarts, the stream resumes from the last committed offset automatically.

#### Fault Tolerance Summary

| Failure Scenario | Recovery Mechanism |
|---|---|
| Driver pod crash | Kubernetes restarts it; `POST /stream/start` re-attaches to existing checkpoint — zero data loss. |
| Batch job exception | Caught within the `Future`; logged and returned as HTTP error. Stream is **unaffected**. |
| Executor pod crash | Spark automatically reschedules tasks on remaining/new executors. |
| Kafka broker unavailability | Spark retries with backoff; configurable via `kafka.consumer.retry.*` options. |

---

## 6. API Reference

| Method | Endpoint | Body | Description |
|---|---|---|---|
| `POST` | `/stream/start` | `{ "topic": "events" }` | Start the Kafka streaming query. |
| `POST` | `/stream/stop` | — | Stop the active streaming query. |
| `POST` | `/batch/run` | `{ "topic": "events", "startOffset": "{\"p0\":100}", "endOffset": "{\"p0\":500}" }` | Trigger an async batch job over a bounded offset range. |
| `GET` | `/batch/status/:jobId` | — | Poll the status of a submitted batch job. |
| `GET` | `/health` | — | Liveness probe for Kubernetes. |
| `POST` | `/stop` | — | Gracefully shut down the entire application. |

---

## 7. Key Dependencies (`build.sbt`)

| Library | Version | Purpose |
|---|---|---|
| `org.apache.spark:spark-core` | 3.5.x | Core Spark runtime |
| `org.apache.spark:spark-sql` | 3.5.x | DataFrame / SQL API |
| `org.apache.spark:spark-sql-kafka-0-10` | 3.5.x | Kafka source/sink connector |
| `io.delta:delta-spark` | 3.x | Delta Lake ACID sink |
| `org.apache.pekko:pekko-http` | 1.x | Embedded async HTTP server |
| `org.apache.pekko:pekko-actor-typed` | 1.x | Actor system for HTTP server |
| `com.typesafe:config` | 1.4.x | Typesafe application configuration |
| `io.circe:circe-generic` | 0.14.x | JSON (de)serialization for HTTP bodies |

---

## 8. Anticipated Challenges & Mitigations

| Challenge | Mitigation |
|---|---|
| **Resource contention under heavy batch load** | Use Scheduler Pools with higher `weight` and `minShare` for the streaming pool. Monitor with Spark UI (`/stages`, `/jobs`). |
| **Kafka offset management for batch** | Query the target sink (Delta table or JDBC) for the `MAX(processed_timestamp)` before each batch job, and use that as `startingOffsets` to avoid reprocessing. |
| **Thread safety of SparkSession** | `SparkSession` is thread-safe for `read`/`readStream`; however, `setLocalProperty` is thread-local, so each batch thread must set its own pool assignment. |
| **Late-arriving data in streaming** | Use Spark's `withWatermark` on event-time columns to bound state and avoid unbounded memory growth. |
| **Graceful drain on shutdown** | Call `query.stop()` before `spark.stop()` to let the streaming query commit its final micro-batch and advance offsets cleanly. |

---

## 9. Success Criteria

| Criterion | How It Is Demonstrated |
|---|---|
| **Uniformity** | Single `main()` in `Main.scala` handles both streaming and batch via HTTP API. |
| **Stability** | A deliberately failed batch job (e.g., bad offsets) returns an HTTP 500; the streaming query continues uninterrupted. |
| **Kafka Versatility** | The same `spark-sql-kafka-0-10` connector is used for both `readStream` and bounded `read` within the same session. |
| **Control** | `POST /stop` cleanly drains the stream, writes final micro-batch, stops the session, and terminates the JVM — no zombie processes. |
| **Concurrency** | Three simultaneous `POST /batch/run` calls all execute and complete while the streaming query continues processing at its normal throughput. |