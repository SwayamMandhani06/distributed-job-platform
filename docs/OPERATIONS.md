# Distributed Job Platform — Operational Runbook & Cluster Operations Guide

> **Phase 2 Technical Runbook: Real Multi-Process Cluster, Durable PostgreSQL Persistence, Redis Delivery, Prometheus Observability, and Fault Recovery.**

---

## 1. Multi-Process Cluster Architecture

The Phase 2 platform deployment runs a fully decoupled, multi-container distributed topology using Docker Compose:

```mermaid
graph TB
    subgraph Client Layer
        Browser["Live Web Dashboard<br/>:8000"]
        HTTPClient["HTTP REST Clients / SDKs"]
    end

    subgraph Service Tier
        Scheduler["FastAPI Scheduler<br/>job-platform-scheduler :8000<br/>(Leader Lease Holder)"]
        Worker1["Autonomous Worker 1<br/>job-platform-worker-1"]
        Worker2["Autonomous Worker 2<br/>job-platform-worker-2"]
    end

    subgraph Data & Transit Tier
        Postgres[("PostgreSQL 16 Engine<br/>job-platform-postgres :5432<br/>Authoritative Persistence")]
        RedisBroker[("Redis 7 Broker<br/>job-platform-redis :6379<br/>AOF & RDB Persistence")]
        Prometheus["Prometheus 2.51<br/>job-platform-prometheus :9090<br/>Metrics Scraper"]
    end

    HTTPClient -->|POST /submit_job| Scheduler
    Browser -->|GET / & /api/dashboard_data| Scheduler
    Scheduler -->|ACID Persistence & Fencing| Postgres
    Scheduler -->|LPUSH job_queue| RedisBroker
    Worker1 -->|RPOPLPUSH| RedisBroker
    Worker2 -->|RPOPLPUSH| RedisBroker
    Worker1 -->|Atomic Claim & Fenced Complete| Postgres
    Worker2 -->|Atomic Claim & Fenced Complete| Postgres
    Prometheus -->|Scrape /metrics :5s| Scheduler
```

---

## 2. Cluster Operational Commands

### A. Clean Start (Production & Development Cluster)
Launches all six containers in detached mode, verifying service health checks and dependency ordering:
```bash
docker compose up -d
```

### B. Verify Cluster Status & Health
Check container state, healthcheck evaluations, and mapped ports:
```bash
docker compose ps
```
Expected output:
```text
NAME                      IMAGE                                STATUS                    PORTS
job-platform-postgres     postgres:16-alpine                   Up (healthy)              0.0.0.0:5432->5432/tcp
job-platform-redis        redis:7-alpine                       Up (healthy)              0.0.0.0:6379->6379/tcp
job-platform-scheduler    distributed-job-platform-scheduler   Up (healthy)              0.0.0.0:8000->8000/tcp
job-platform-worker-1     distributed-job-platform-worker-1    Up (running)              8000/tcp
job-platform-worker-2     distributed-job-platform-worker-2    Up (running)              8000/tcp
job-platform-prometheus   prom/prometheus:v2.51.0              Up (running)              0.0.0.0:9090->9090/tcp
```

### C. Live Log Streaming
Each service emits structured, timestamped logs with dedicated logging contexts:
```bash
# Stream all cluster logs
docker compose logs -f

# Stream individual service logs
docker compose logs -f scheduler
docker compose logs -f worker-1
docker compose logs -f worker-2
docker compose logs -f postgres
docker compose logs -f redis
docker compose logs -f prometheus
```

### D. Rebuild Cluster Containers
Rebuild container images with updated application code without destroying persisted volume data:
```bash
docker compose up -d --build
```

### E. Graceful Cluster Stop
Gracefully stops all worker and scheduler instances, allowing active jobs to finish and flushing Redis AOF buffers to disk:
```bash
docker compose down
```

### F. Destructive Cluster Reset (DATA LOSS WARNING)
> [!CAUTION]
> **DESTRUCTIVE OPERATION**: The following command stops all containers and **permanently deletes** all persistent Docker volumes (`postgres_data`, `redis_data`, and `prometheus_data`). All historical job records, registered workers, leader epochs, and metrics history will be permanently destroyed.
```bash
docker compose down -v
```

---

## 3. Data Migration Runbook: SQLite to Authoritative PostgreSQL

To migrate existing SQLite historical records (`job_platform.db`) to the production PostgreSQL cluster without downtime:

1. Ensure the PostgreSQL container is running:
   ```bash
   docker compose up -d postgres
   ```
2. Execute the idempotent migration script:
   ```bash
   python -m tools.migrate_sqlite_to_postgres --sqlite-path job_platform.db --pg-dsn postgresql://postgres:postgres@localhost:5432/job_platform
   ```
3. The script migrates:
   - `schema_migrations`: Version tracking history.
   - `leader_fencing`: Authoritative coordinator fencing epoch records.
   - `jobs`: Complete job history, timestamps, Lamport logical clocks, payloads, and execution outputs.
   - `workers`: Worker registration records, startup timestamps, PIDs, and completed task counters.
4. Summary metrics will be printed upon completion:
   ```text
   ==================================================
   MIGRATION SUMMARY:
     schema_migrations   : 4
     leader_fencing      : 1
     jobs                : 6
     workers             : 1
   ==================================================
   ```

---

## 4. Dual-Storage Development Architecture

To keep developer onboarding frictionless, the platform supports two interchangeable persistence modes:

| Mode | Environment Variable | Database Engine | Use Case |
| :--- | :--- | :--- | :--- |
| **Cluster Mode (Authoritative)** | `DATABASE_URL=postgresql://user:pass@host:5432/db` | PostgreSQL 16 (MVCC, Row-Level Locks) | Multi-container Docker Compose, integration testing, production deployments. |
| **Local Mode (Zero-Config)** | `DATABASE_URL=""` (unset) | SQLite (WAL mode, busy timeout=30s) | Single-developer offline prototyping, unit testing (`pytest`), lightweight dev. |

---

## 5. Observability & Monitoring Endpoints

| Service / Interface | URL | Description |
| :--- | :--- | :--- |
| **Live Observability Dashboard** | `http://localhost:8000/` | Real-time web UI showing worker heartbeats, PIDs, queue depth, and job history. |
| **Scheduler REST API** | `http://localhost:8000/docs` | Interactive Swagger API documentation for job submission and inspection. |
| **Prometheus Exposition Endpoint** | `http://localhost:8000/metrics` | Native Prometheus text format metrics scraped every 5s by Prometheus. |
| **Prometheus Dashboard** | `http://localhost:9090/` | Prometheus query console and target health monitor. |
| **Health Probe** | `http://localhost:8000/health` | HTTP probe reporting scheduler status, queue depth, and worker count. |

---

## 6. Testing & Quality Verification

```bash
# 1. Run full unit test suite (144 tests on isolated SQLite)
python -m pytest tests/

# 2. Run real cluster integration tests (against live PostgreSQL & Redis)
DATABASE_URL="postgresql://postgres:postgres@localhost:5432/job_platform" python -m unittest tests/test_cluster_integration.py
```
