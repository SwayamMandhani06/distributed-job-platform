# Persistence Architecture Audit & PostgreSQL Production Migration Plan

> **Technical Specification & Runbook: Transitioning from SQLite WAL to Authoritative PostgreSQL in Multi-Container & Cloud Deployments.**

---

## 1. Executive Summary & SQLite Concurrency Audit

The current platform implementation utilizes SQLite with Write-Ahead Logging (`PRAGMA journal_mode=WAL`) and configured busy timeouts (`PRAGMA busy_timeout=30000`). While SQLite provides zero-configuration local persistence suitable for single-node prototypes and academic demonstrations, scaling to multi-container container clusters (Docker Compose / Kubernetes) introduces fundamental architectural limitations.

### A. Core SQLite Concurrency Limitations

1. **Single-Writer Serialization & Lock Contention (`SQLITE_BUSY`)**:
   - SQLite enforces database-level write serialization. Even in WAL mode, only **one process can hold the write transaction lock (`BEGIN IMMEDIATE` / `EXCLUSIVE`) at any instant**.
   - Under burst workloads (e.g., 50+ concurrent workers attempting `UPDATE jobs SET status = 'running'` alongside coordinator recovery loops), transaction lock wait times exceed `busy_timeout`, raising `sqlite3.OperationalError: database is locked`.

2. **Network Filesystem (NFS / SMB / CIFS) Corruption Risks**:
   - Multi-container container orchestration (Kubernetes pods across multiple nodes) requiring shared persistent volumes cannot safely mount a shared SQLite file.
   - POSIX file locking over NFS is notoriously unreliable; broken lock leases or split-brain file handles cause silent B-tree corruption.

3. **Absence of Row-Level Locking (`FOR UPDATE SKIP LOCKED`)**:
   - In modern high-throughput job platforms, workers claim tasks using `SELECT ... FOR UPDATE SKIP LOCKED`.
   - In SQLite, workers must rely on table-level transactional updates (`UPDATE jobs ... WHERE status = 'pending' LIMIT 1`), causing severe serialization bottlenecks under high concurrency.

4. **WAL Checkpoint Latency & Memory Footprint**:
   - As transaction volume increases, the WAL file grows until passive or active checkpoints run. Checkpoints contend with active readers, intermittently spiking read latency from microseconds to hundreds of milliseconds.

---

## 2. Target Production Architecture: PostgreSQL 16+

PostgreSQL provides:
* **Multi-Version Concurrency Control (MVCC)** with fine-grained row-level locking.
* **`FOR UPDATE SKIP LOCKED`**: Enables workers to pull pending workloads without lock contention or duplicate claims.
* **Native JSONB Storage**: Efficient indexing and JSON path querying for job payloads and handler execution outputs.
* **High Availability**: Streaming replication with Patroni or AWS Aurora / Google Cloud SQL.

```mermaid
graph TB
    subgraph Multi-Node Cluster
        Scheduler1["FastAPI Scheduler 1<br/>(Leader Lease)"]
        Scheduler2["FastAPI Scheduler 2<br/>(Standby)"]
        Worker1["Worker Consumer 1"]
        Worker2["Worker Consumer 2"]
        Worker3["Worker Consumer 3"]
    end

    subgraph Data & Queue Tier
        PgBouncer["PgBouncer Connection Pooler<br/>(Transaction Pooling :6432)"]
        PostgresPrimary[("PostgreSQL 16 Primary<br/>MVCC + Row-Level Locks")]
        PostgresReplica[("PostgreSQL 16 Read Replica<br/>Streaming Replication")]
        RedisQueue[("Redis Cluster / Sentinel<br/>Reliable At-Least-Once Queue")]
    end

    Scheduler1 --> PgBouncer
    Scheduler2 --> PgBouncer
    Worker1 --> PgBouncer
    Worker2 --> PgBouncer
    Worker3 --> PgBouncer

    PgBouncer --> PostgresPrimary
    PostgresPrimary -. Synchronous / Async Stream .-> PostgresReplica
    Scheduler1 <--> RedisQueue
    Worker1 <--> RedisQueue
```

---

## 3. PostgreSQL Authoritative Schema Definition

The PostgreSQL schema preserves identical field naming, semantics, claim-token fencing, and epoch fencing conventions while utilizing native PostgreSQL data types.

```sql
-- ===========================================================================
-- PostgreSQL Migration DDL: Distributed Job Platform
-- ===========================================================================

-- 1. Schema Version Tracking
CREATE TABLE IF NOT EXISTS schema_migrations (
    version     INTEGER PRIMARY KEY,
    name        VARCHAR(128) NOT NULL,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 2. Core Jobs Table
CREATE TABLE IF NOT EXISTS jobs (
    job_id              UUID PRIMARY KEY,
    job_type            VARCHAR(64) NOT NULL,
    payload             JSONB NOT NULL DEFAULT '{}'::jsonb,
    status              VARCHAR(32) NOT NULL DEFAULT 'pending',
    worker_id           VARCHAR(64),
    submitted_at        TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at          TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,
    result              JSONB,
    error_message       TEXT,
    retry_count         INTEGER NOT NULL DEFAULT 0,
    max_retries         INTEGER NOT NULL DEFAULT 3,
    claimed_at          TIMESTAMPTZ,
    lease_expires_at    TIMESTAMPTZ,
    claim_token         UUID,
    lamport_ts          BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT check_status CHECK (
        status IN ('pending', 'queued', 'running', 'completed', 'failed', 'dead_letter')
    )
);

-- Composite Performance Indexes
CREATE INDEX IF NOT EXISTS idx_jobs_status_lease 
    ON jobs (status, lease_expires_at) 
    WHERE status = 'running';

CREATE INDEX IF NOT EXISTS idx_jobs_pending_claim 
    ON jobs (submitted_at ASC) 
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_jobs_submitted_at_desc 
    ON jobs (submitted_at DESC);

-- 3. Worker Registry Table
CREATE TABLE IF NOT EXISTS workers (
    worker_id       VARCHAR(64) PRIMARY KEY,
    status          VARCHAR(32) NOT NULL DEFAULT 'online',
    last_heartbeat  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    registered_at   TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    jobs_completed  BIGINT NOT NULL DEFAULT 0,
    pid             INTEGER,
    startup_time    TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    current_job_id  UUID REFERENCES jobs(job_id) ON DELETE SET NULL,
    CONSTRAINT check_worker_status CHECK (status IN ('online', 'offline', 'stale'))
);

CREATE INDEX IF NOT EXISTS idx_workers_liveness 
    ON workers (status, last_heartbeat);

-- 4. Leader Fencing Epoch Table
CREATE TABLE IF NOT EXISTS leader_fencing (
    scope           VARCHAR(64) PRIMARY KEY,
    leader_id       VARCHAR(64) NOT NULL,
    epoch           BIGINT NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Record Initial Version
INSERT INTO schema_migrations (version, name) 
VALUES (1, 'postgresql_authoritative_initial')
ON CONFLICT (version) DO NOTHING;
```

---

## 4. Zero-Downtime Data Migration Plan

### Phase A: Preparation (T - 2 Days)
1. Provision managed PostgreSQL 16 instance (AWS RDS, Cloud SQL, or Docker Compose).
2. Apply above DDL schema and indexes.
3. Verify connection latency from Scheduler and Worker nodes ($< 2$ ms).

### Phase B: Dual-Read & Data Sync (T - 1 Hour)
Execute the migration script below to copy durable historical records from SQLite into PostgreSQL:

```python
"""
Data Migration Utility: SQLite -> PostgreSQL 16
"""
import json
import sqlite3
import psycopg2
from psycopg2.extras import execute_batch

def migrate_sqlite_to_postgres(sqlite_path: str, pg_dsn: str):
    sqlite_conn = sqlite3.connect(sqlite_path)
    sqlite_conn.row_factory = sqlite3.Row
    pg_conn = psycopg2.connect(pg_dsn)

    with sqlite_conn, pg_conn:
        pg_cursor = pg_conn.cursor()

        # 1. Migrate Leader Fencing
        fences = sqlite_conn.execute("SELECT * FROM leader_fencing").fetchall()
        for f in fences:
            pg_cursor.execute("""
                INSERT INTO leader_fencing (scope, leader_id, epoch, updated_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (scope) DO UPDATE 
                SET epoch = EXCLUDED.epoch, leader_id = EXCLUDED.leader_id, updated_at = EXCLUDED.updated_at
            """, (f["scope"], f["leader_id"], f["epoch"], f["updated_at"]))

        # 2. Migrate Jobs
        jobs = sqlite_conn.execute("SELECT * FROM jobs").fetchall()
        job_rows = []
        for j in jobs:
            payload_json = json.loads(j["payload"]) if j["payload"] else {}
            result_json = json.loads(j["result"]) if j["result"] else None
            job_rows.append((
                j["job_id"], j["job_type"], json.dumps(payload_json), j["status"],
                j["worker_id"], j["submitted_at"], j["started_at"], j["completed_at"],
                json.dumps(result_json) if result_json else None, j["error_message"],
                j["retry_count"], j["max_retries"], j["claimed_at"], j["lease_expires_at"],
                j["claim_token"], j["lamport_ts"]
            ))

        execute_batch(pg_cursor, """
            INSERT INTO jobs (
                job_id, job_type, payload, status, worker_id, submitted_at,
                started_at, completed_at, result, error_message, retry_count,
                max_retries, claimed_at, lease_expires_at, claim_token, lamport_ts
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (job_id) DO NOTHING
        """, job_rows, page_size=500)

        # 3. Migrate Workers
        workers = sqlite_conn.execute("SELECT * FROM workers").fetchall()
        for w in workers:
            pg_cursor.execute("""
                INSERT INTO workers (worker_id, status, last_heartbeat, registered_at, jobs_completed, pid, startup_time)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (worker_id) DO NOTHING
            """, (w["worker_id"], w["status"], w["last_heartbeat"], w["registered_at"], w["jobs_completed"], w["pid"], w["startup_time"]))

        pg_conn.commit()
        print(f"Migrated {len(jobs)} jobs and {len(workers)} workers successfully.")
```

### Phase C: Cutover (T - 0 Minutes)
1. Stop background load generator.
2. Allow active Redis queue to drain.
3. Update environment configuration: `DATABASE_URL=postgresql://user:pass@postgres:5432/job_platform`.
4. Restart Scheduler and Workers pointing to PostgreSQL.

---

## 5. Rollback & Disaster Recovery Runbook

1. **Trigger Condition**:
   - Connection exhaustion on PostgreSQL or unexpected ORM/driver latency $> 100$ ms.
2. **Immediate Rollback Procedure**:
   - Re-set `DATABASE_URL=""` or `JOB_PLATFORM_DB_NAME="job_platform.db"`.
   - SQLite database was preserved unmodified in read-only mode during cutover.
   - Restart Scheduler and Worker instances; they immediately resume from the local SQLite datastore.
3. **Data Reconciliation**:
   - Any jobs completed on PostgreSQL during the test window can be synchronized back to SQLite by selecting `WHERE completed_at > :cutover_time`.
