# Web-Based Distributed System Architecture & Integration Guide

This document defines the production and development architecture for the integrated **Web-Based Distributed System** of `distributed-job-platform` (FastAPI scheduler, Redis coordination/queues, PostgreSQL authoritative persistence, background workers, and real-time dashboard).

---

## 1. System Topology & Architectural Overview

The platform implements a multi-tier, decoupled, asynchronous distributed architecture:

```
[ Client / Browser ] 
        |
        | HTTP/REST (JSON)
        v
+-----------------------------------------------------------+
| FastAPI Scheduler & API Gateway                           |
|  - Request Validation & Schema Serialization              |
|  - Idempotency & Deduplication Filter                     |
|  - Leader Election & Coordination Ingress                 |
|  - Prometheus Metrics & Health Instrumentation            |
+-------------+-------------------------------+-------------+
              |                               |
              | SQL Transactions              | LPUSH / BRPOP / PubSub
              v                               v
+-------------------------------+   +-----------------------------+
| PostgreSQL / SQLite Database  |   | Redis In-Memory Broker      |
| (Authoritative Durable State) |   | (Ephemeral Queues & Leases) |
|  - `jobs` Table (Fencing)     |   |  - `job:queue:default`      |
|  - `workers` Heartbeats       |   |  - `job:queue:dead_letter`  |
|  - `schema_migrations`        |   |  - `leader:lease:key`       |
+-------------------------------+   +-----------------------------+
              ^                               |
              |                               |
              | SQL Status & Progress Updates | BRPOP Jobs / Heartbeats
              +---------------+---------------+
                              |
               +--------------+--------------+
               |                             |
               v                             v
    +--------------------+        +--------------------+
    | Worker Process 1   |        | Worker Process 2   |
    | (OS Subprocess/Pod)|        | (OS Subprocess/Pod)|
    +--------------------+        +--------------------+
```

---

## 2. Service Boundaries & Responsibilities

| Subsystem | Boundary Type | Technologies | Primary Invariants & Responsibilities |
| :--- | :--- | :--- | :--- |
| **API Scheduler** | Public / Gateway | FastAPI, Uvicorn, Pydantic | Receives client requests; validates schemas; assigns UUIDs; atomically persists pending jobs in database; enqueues references into Redis; exposes Prometheus metrics. |
| **Authoritative Store** | Internal State | PostgreSQL (prod) / SQLite (dev) | Single source of truth for job states, execution history, and fencing epochs. Prevents duplicate updates via optimistic locking and conditional `UPDATE ... WHERE claim_token = :token`. |
| **Message Broker** | Internal Transport | Redis 5.0+ | Fast queue delivery (`LPUSH` / `BRPOP`), distributed leader lease locks (`SET NX PX`), and real-time event broadcasting (`PUBLISH`). Queues store job references, not large payloads. |
| **Worker Cluster** | Decoupled Compute | Python Multi-Process Workers | Polls queues; registers heartbeats; executes isolated tasks; captures stdout/stderr; updates DB status with fenced claim tokens; handles retries with exponential backoff. |
| **Observability & UI** | Front-End / Metrics | Vanilla JS Dashboard & Prometheus | Polls `/health`, `/metrics`, and `/jobs` to display real-time cluster health, worker liveness, queue depth, and election status. |

---

## 3. HTTP API Reference & Contract

### 3.1 Job Lifecycle Endpoints

#### `POST /jobs` — Submit New Distributed Job
- **Method**: `POST`
- **Request Headers**: `Content-Type: application/json`, `X-Idempotency-Key: <uuid>` (Optional)
- **Request Body**:
  ```json
  {
    "task_name": "process_dataset",
    "payload": {"dataset_id": "ds-401", "rows": 10000},
    "priority": 1,
    "max_retries": 3,
    "timeout_seconds": 60.0
  }
  ```
- **Responses**:
  - `201 Created`: Job persisted in DB and enqueued in Redis.
    ```json
    {
      "job_id": "job-8f9b2c14",
      "status": "PENDING",
      "task_name": "process_dataset",
      "created_at_iso": "2026-10-13T14:30:00Z"
    }
    ```
  - `400 Bad Request`: Validation failure (unknown task name, malformed JSON, negative priority).
  - `503 Service Unavailable`: Dependent storage or broker outage (PostgreSQL or Redis unreachable).

#### `GET /jobs/{job_id}` — Inspect Job Execution Status
- **Method**: `GET`
- **Responses**:
  - `200 OK`:
    ```json
    {
      "job_id": "job-8f9b2c14",
      "status": "COMPLETED",
      "task_name": "process_dataset",
      "worker_id": "worker-node-1",
      "progress_pct": 100.0,
      "result": {"processed": 10000, "status": "ok"},
      "error_message": null,
      "created_at_iso": "2026-10-13T14:30:00Z",
      "completed_at_iso": "2026-10-13T14:30:04Z"
    }
    ```
  - `404 Not Found`: Job ID does not exist in authoritative storage.

#### `POST /jobs/{job_id}/cancel` — Fenced Cancellation Request
- **Method**: `POST`
- **Responses**:
  - `200 OK`: Job marked as `CANCELLED`. If currently executing, running worker aborts upon next fencing check.
  - `409 Conflict`: Job is already in a terminal state (`COMPLETED` or `FAILED`).

---

### 3.2 Coordination & Cluster Introspection Endpoints

#### `GET /health` — Multi-Dependency Health Probe
- **Method**: `GET`
- **Responses**:
  - `200 OK`: All dependencies healthy.
    ```json
    {
      "status": "healthy",
      "database": "connected",
      "redis": "connected",
      "active_workers": 2,
      "timestamp_iso": "2026-10-13T14:30:00Z"
    }
    ```
  - `503 Service Unavailable`: One or more dependencies are degraded.
    ```json
    {
      "status": "degraded",
      "database": "connected",
      "redis": "connection_refused",
      "active_workers": 0
    }
    ```

#### `GET /workers` — Real Worker Heartbeat Registry
- **Method**: `GET`
- **Responses**:
  - `200 OK`: Lists all active workers with last heartbeat timestamps. Workers whose heartbeats exceed the 10-second TTL are automatically flagged as `OFFLINE`.

#### `GET /leader` — Active Leader Lease Inspection
- **Method**: `GET`
- **Responses**:
  - `200 OK`: Returns current leader instance ID, lease epoch, and lease expiration timestamp.

---

## 4. Authentication Assumptions & Security Boundaries

1. **Current Security Boundary**:
   - The platform is engineered as an educational distributed computing testbed and internal private cluster.
   - Endpoints assume execution within an isolated private network / VPC, where communication between the FastAPI gateway, Redis, and PostgreSQL is unauthenticated or protected by service passwords (`REDIS_PASSWORD`, `POSTGRES_PASSWORD`).
2. **Production Hardening Guidelines**:
   - **Ingress Layer**: Introduce an API Gateway (e.g., Kong, Envoy, Traefik) terminating TLS and enforcing JWT Bearer token authentication.
   - **Inter-Service Communication**: Enable mutual TLS (mTLS) for Redis 6+ and PostgreSQL connections.
   - **Worker Fencing**: Job submission inputs must be sanitized to prevent code injection via untrusted task names.

---

## 5. Failure Models, Graceful Shutdown & Recovery Guarantees

### 5.1 Broker Outage (Redis Down)
- **Symptom**: Redis becomes unreachable due to network partition or process crash.
- **Scheduler Invariant**: FastAPI cannot enqueue new jobs into Redis. Requests fail fast with HTTP 503 (`Service Unavailable`), preserving database consistency without creating phantom jobs.
- **Worker Invariant**: Workers enter an exponential backoff retry loop (0.5s to 5.0s) polling Redis. In-flight jobs continue executing to completion and update PostgreSQL directly.
- **Recovery**: When Redis restarts, workers reconnect immediately. Stranded pending jobs in PostgreSQL are reconciled and re-enqueued by the scheduler leader reconciliation loop.

### 5.2 Database Outage (PostgreSQL Down)
- **Symptom**: Authoritative database connection drops.
- **Scheduler Invariant**: Read and write transactions fail immediately with standard rollback. No state changes occur.
- **Worker Invariant**: Workers detecting database failure during task completion do not acknowledge the job in Redis. The job remains in the queue or is reclaimed after worker heartbeat timeout, ensuring at-least-once delivery.

### 5.3 Worker Crash & Claim Fencing
- **Symptom**: Worker process crashes while executing a claimed job.
- **Reclamation Protocol**:
  1. The worker stops sending heartbeats to PostgreSQL/Redis.
  2. The scheduler leader runs a periodic reconciliation sweep (every 5 seconds).
  3. Any running job whose owning worker has not sent a heartbeat within the lease TTL (15s) is marked `RECLAIMED`.
  4. The job is incremented in retry count and re-enqueued into Redis.
  5. If the dead worker awakens after a long garbage collection pause and attempts to complete the job, its conditional SQL update fails because its claim token has been invalidated by the new epoch.

### 5.4 Graceful Shutdown
- Upon receiving `SIGTERM` or `SIGINT`:
  1. Worker stops accepting new jobs from Redis.
  2. In-flight jobs are allowed a configurable grace period (default: 30 seconds) to complete cleanly.
  3. If the job finishes within the window, the result is saved and acknowledged.
  4. If the grace period expires, the worker forcibly terminates and releases its claim so other cluster nodes can re-execute the task.
