"""
SQLite-based job and worker persistence layer for the distributed job platform.

WHY persistent job state matters in a distributed system
--------------------------------------------------------
Without persistence, all job knowledge lives only in memory. If the
scheduler process crashes or restarts:
  * In-flight job information is lost — clients have no way to check
    what happened to their submitted work.
  * There is no audit trail — you can't answer "which jobs ran yesterday?"
    or "which worker handled job X?".
  * The dashboard (Stage 6) would have nothing to display.

By writing every state transition to SQLite, the ``jobs`` table becomes
the **single source of truth** for job state across the entire platform.

WHY Heartbeat Monitoring matters in a Distributed System (Unit 3 Pattern)
-------------------------------------------------------------------------
In an asynchronous distributed system, nodes do NOT share memory or a single
physical clock. There is no instant way to check if a remote compute node
is operational.

The **Heartbeat Pattern** solves this fundamental problem:
  1. Every worker process periodically sends a "liveness signal" (heartbeat)
     updating its `last_heartbeat` timestamp in the shared storage layer.
  2. The Scheduler runs an autonomous background monitor that checks for
     stale heartbeats.
  3. If a worker fails to send a heartbeat within `timeout_seconds` (e.g., 15s),
     the system presumes the worker node has crashed, network-partitioned, or frozen,
     and marks its status as `'offline'`.

This is an **Unreliable Failure Detector** (Chandra-Toueg model) — a fundamental
building block required for fault tolerance, failover, and leader election.

WHY raw SQL instead of an ORM?
------------------------------
  * **Transparency** — every query is visible and explainable in a viva.
  * **Lightweight** — zero extra dependencies; ``sqlite3`` ships with Python.
"""

import json
import os
import re
import sqlite3
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from uuid import uuid4

from common.config import (
    DB_PATH,
    SQLITE_TIMEOUT_SECONDS,
    POSTGRES_CONNECT_TIMEOUT_SECONDS,
    get_db_path,
)
from common.logging_config import setup_logger

logger = setup_logger("database")


class StaleLeaderError(Exception):
    """Raised when an authoritative coordinator operation is attempted by a stale leader epoch."""
    pass


class DummyCursor:
    """No-op cursor returned for transaction start commands under PostgreSQL."""
    rowcount = 0

    def fetchone(self) -> None:
        return None

    def fetchall(self) -> List[Any]:
        return []


class PostgresConnectionWrapper:
    """Wrapper around psycopg2 connection providing sqlite3-compatible execution semantics."""

    def __init__(self, raw_conn: Any):
        self._conn = raw_conn

    def execute(self, sql: str, params: Optional[Union[Tuple[Any, ...], List[Any]]] = None) -> Any:
        clean_sql = sql.strip()
        if clean_sql.upper().startswith("BEGIN"):
            # PostgreSQL connections in psycopg2 are already within an active transaction block
            return DummyCursor()

        if clean_sql.upper().startswith("PRAGMA"):
            # Pragmas are SQLite-specific; provide compatible cursor simulation
            class PragmaCursor:
                rowcount = 1

                def fetchone(self) -> Any:
                    return (30000,)

                def fetchall(self) -> List[Any]:
                    return [(30000,)]

            return PragmaCursor()

        # 1. Translate scalar MAX(a, b) to standard GREATEST(a, b)
        pg_sql = re.sub(
            r"\bMAX\s*\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)",
            r"GREATEST(\1, \2)",
            sql,
            flags=re.IGNORECASE,
        )

        # 2. Translate SQLite null-safe token comparison to PostgreSQL compatible condition
        pg_sql = pg_sql.replace(
            "(claim_token IS ? OR claim_token = ?)",
            "((%s::text IS NULL AND claim_token IS NULL) OR claim_token = %s)",
        )

        # 3. Translate ? parameter placeholders to %s
        pg_sql = pg_sql.replace("?", "%s")

        from psycopg2.extras import RealDictCursor
        cur = self._conn.cursor(cursor_factory=RealDictCursor)
        if params is not None:
            cur.execute(pg_sql, tuple(params))
        else:
            cur.execute(pg_sql)
        return cur

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "PostgresConnectionWrapper":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if exc_type is not None:
            self.rollback()
        else:
            self.commit()
        self.close()


def _get_postgres_connection(
    dsn: str,
    timeout: float = 10.0,
    max_retries: int = 5,
    retry_delay: float = 1.0,
) -> Any:
    """Establish connection to PostgreSQL with retry backoff for container startup readiness."""
    import psycopg2
    deadline = time.time() + timeout
    last_err: Optional[Exception] = None
    for attempt in range(max_retries):
        try:
            return psycopg2.connect(dsn, connect_timeout=int(timeout))
        except psycopg2.OperationalError as exc:
            last_err = exc
            if time.time() >= deadline:
                break
            time.sleep(retry_delay)
    logger.error("Failed to connect to PostgreSQL at %s: %s", dsn, last_err)
    if last_err:
        raise last_err
    raise RuntimeError(f"Could not connect to PostgreSQL at {dsn}")


def is_postgres_configured() -> bool:
    """Return True if DATABASE_URL is set in environment, indicating authoritative PostgreSQL persistence."""
    return bool(os.getenv("DATABASE_URL"))


def _get_connection(db_path: Optional[Path] = None, timeout: float = SQLITE_TIMEOUT_SECONDS) -> Any:
    """Open a connection to PostgreSQL (if DATABASE_URL is set) or SQLite with WAL mode."""
    if db_path is None and os.getenv("DATABASE_URL"):
        raw_conn = _get_postgres_connection(os.environ["DATABASE_URL"], timeout=POSTGRES_CONNECT_TIMEOUT_SECONDS)
        return PostgresConnectionWrapper(raw_conn)

    target_path = db_path or get_db_path()
    conn = sqlite3.connect(str(target_path), timeout=timeout)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    busy_timeout_ms = int(timeout * 1000)
    conn.execute(f"PRAGMA busy_timeout = {busy_timeout_ms}")
    return conn


def _migrate_db(conn: Any) -> None:
    """Idempotently apply schema migrations, version tracking, and performance indexes."""
    # 1. Schema migrations table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version     INTEGER PRIMARY KEY,
            name        TEXT NOT NULL,
            applied_at  TEXT NOT NULL
        )
    """)

    is_pg = isinstance(conn, PostgresConnectionWrapper)

    if not is_pg:
        # SQLite schema inspection & migrations
        cursor_jobs = conn.execute("PRAGMA table_info(jobs)")
        existing_jobs_cols = {row["name"] for row in cursor_jobs.fetchall()}

        jobs_columns_to_add = [
            ("retry_count", "INTEGER NOT NULL DEFAULT 0"),
            ("max_retries", "INTEGER NOT NULL DEFAULT 3"),
            ("claimed_at", "TEXT"),
            ("lease_expires_at", "TEXT"),
            ("claim_token", "TEXT"),
            ("lamport_ts", "INTEGER NOT NULL DEFAULT 0"),
        ]
        for col_name, col_def in jobs_columns_to_add:
            if col_name not in existing_jobs_cols:
                conn.execute(f"ALTER TABLE jobs ADD COLUMN {col_name} {col_def}")
                logger.info("Migrated jobs table: added column %s", col_name)

        cursor_workers = conn.execute("PRAGMA table_info(workers)")
        existing_workers_cols = {row["name"] for row in cursor_workers.fetchall()}

        workers_columns_to_add = [
            ("pid", "INTEGER"),
            ("startup_time", "TEXT"),
            ("current_job_id", "TEXT"),
        ]
        for col_name, col_def in workers_columns_to_add:
            if col_name not in existing_workers_cols:
                conn.execute(f"ALTER TABLE workers ADD COLUMN {col_name} {col_def}")
                logger.info("Migrated workers table: added column %s", col_name)
    else:
        # PostgreSQL schema inspection & migrations
        cursor_jobs = conn.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'jobs'")
        existing_jobs_cols = {row["column_name"] for row in cursor_jobs.fetchall()}

        jobs_columns_to_add = [
            ("retry_count", "INTEGER NOT NULL DEFAULT 0"),
            ("max_retries", "INTEGER NOT NULL DEFAULT 3"),
            ("claimed_at", "TEXT"),
            ("lease_expires_at", "TEXT"),
            ("claim_token", "TEXT"),
            ("lamport_ts", "BIGINT NOT NULL DEFAULT 0"),
        ]
        for col_name, col_def in jobs_columns_to_add:
            if col_name not in existing_jobs_cols:
                conn.execute(f"ALTER TABLE jobs ADD COLUMN IF NOT EXISTS {col_name} {col_def}")
                logger.info("Migrated PostgreSQL jobs table: added column %s", col_name)

        cursor_workers = conn.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'workers'")
        existing_workers_cols = {row["column_name"] for row in cursor_workers.fetchall()}

        workers_columns_to_add = [
            ("pid", "INTEGER"),
            ("startup_time", "TEXT"),
            ("current_job_id", "TEXT"),
        ]
        for col_name, col_def in workers_columns_to_add:
            if col_name not in existing_workers_cols:
                conn.execute(f"ALTER TABLE workers ADD COLUMN IF NOT EXISTS {col_name} {col_def}")
                logger.info("Migrated PostgreSQL workers table: added column %s", col_name)

    # 4. Composite performance indexes
    conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status_lease ON jobs (status, lease_expires_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_submitted_at ON jobs (submitted_at DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_workers_status_hb ON workers (status, last_heartbeat)")

    # 5. Record version migrations
    now_iso = datetime.now(timezone.utc).isoformat()
    migrations = [
        (1, "initial_schema"),
        (2, "claim_fencing_and_leader_leases"),
        (3, "worker_process_identity_and_liveness"),
        (4, "composite_query_indexes"),
        (5, "postgresql_authoritative_migration"),
    ]
    for v_num, v_name in migrations:
        conn.execute(
            "INSERT INTO schema_migrations (version, name, applied_at) VALUES (?, ?, ?) ON CONFLICT (version) DO NOTHING",
            (v_num, v_name, now_iso),
        )


def init_db(db_path: Optional[Path] = None) -> None:
    """Create ``jobs``, ``workers``, and ``leader_fencing`` tables if they do not exist and run migrations."""
    conn = _get_connection(db_path=db_path)
    try:
        # 1. Jobs table (base schema)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                job_id          TEXT PRIMARY KEY,
                job_type        TEXT    NOT NULL,
                payload         TEXT    NOT NULL DEFAULT '{}',
                status          TEXT    NOT NULL DEFAULT 'pending',
                worker_id       TEXT,
                submitted_at    TEXT    NOT NULL,
                started_at      TEXT,
                completed_at    TEXT,
                result          TEXT,
                error_message   TEXT,
                retry_count     INTEGER NOT NULL DEFAULT 0,
                max_retries     INTEGER NOT NULL DEFAULT 3,
                claimed_at      TEXT,
                lease_expires_at TEXT,
                claim_token     TEXT,
                lamport_ts      BIGINT  NOT NULL DEFAULT 0
            )
        """)

        # 2. Workers table (Stage 4 Worker Registry)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS workers (
                worker_id       TEXT PRIMARY KEY,
                status          TEXT    NOT NULL DEFAULT 'online',
                last_heartbeat  TEXT    NOT NULL,
                registered_at   TEXT    NOT NULL,
                jobs_completed  BIGINT  NOT NULL DEFAULT 0,
                pid             INTEGER,
                startup_time    TEXT,
                current_job_id  TEXT
            )
        """)

        # 3. Leader Fencing table (Unit III Leader Leases & Monotonic Fencing Epochs)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS leader_fencing (
                scope           TEXT PRIMARY KEY,
                leader_id       TEXT NOT NULL,
                epoch           BIGINT  NOT NULL,
                updated_at      TEXT NOT NULL
            )
        """)

        # Run migration for existing databases
        _migrate_db(conn)

        conn.commit()
        db_desc = "PostgreSQL cluster" if isinstance(conn, PostgresConnectionWrapper) else str(db_path or get_db_path())
        logger.info("Database initialised and migrated (jobs, workers & leader_fencing) at %s", db_desc)
    finally:
        conn.close()


def validate_and_record_leader_epoch(
    conn: Any,
    leader_id: str,
    epoch: int,
    scope: str = "scheduler_recovery",
    commit: bool = True,
) -> None:
    """Validate that caller's epoch is >= durable epoch, and record it atomically.

    Prevents stale leaders from executing writes after a newer leader has taken over.
    If the caller's epoch is strictly less than the durable epoch, StaleLeaderError is raised.
    When commit=False, caller must commit the transaction (e.g., inside BEGIN IMMEDIATE).
    """
    cursor = conn.execute("SELECT epoch, leader_id FROM leader_fencing WHERE scope = ?", (scope,))
    row = cursor.fetchone()
    if row is not None:
        current_epoch = int(row["epoch"])
        if epoch < current_epoch:
            raise StaleLeaderError(
                f"Fencing violation: caller epoch {epoch} (leader '{leader_id}') is superseded "
                f"by durable epoch {current_epoch} (held by '{row['leader_id']}')."
            )

    now_iso = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT INTO leader_fencing (scope, leader_id, epoch, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(scope) DO UPDATE SET
            leader_id = excluded.leader_id,
            epoch = excluded.epoch,
            updated_at = excluded.updated_at
        WHERE excluded.epoch >= leader_fencing.epoch
        """,
        (scope, leader_id, epoch, now_iso),
    )
    if commit:
        conn.commit()


def get_current_fencing_epoch(scope: str = "scheduler_recovery") -> Optional[Dict[str, Any]]:
    """Return the durable leader fencing record for the specified scope."""
    conn = _get_connection()
    try:
        cursor = conn.execute("SELECT * FROM leader_fencing WHERE scope = ?", (scope,))
        row = cursor.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()



# ---------------------------------------------------------------------------
# Job CRUD operations
# ---------------------------------------------------------------------------
def insert_job(
    job_id: str,
    job_type: str,
    payload: Dict[str, Any],
    submitted_at: str,
    lamport_ts: int = 0,
    max_retries: int = 3,
) -> None:
    """Insert a new job row with status ``'pending'``."""
    conn = _get_connection()
    try:
        conn.execute(
            """
            INSERT INTO jobs (job_id, job_type, payload, status, submitted_at, lamport_ts, max_retries, retry_count)
            VALUES (?, ?, ?, 'pending', ?, ?, ?, 0)
            """,
            (job_id, job_type, json.dumps(payload), submitted_at, lamport_ts, max_retries),
        )
        conn.commit()
        logger.info("Inserted job %s with status 'pending' (lamport_ts=%d)", job_id, lamport_ts)
    except Exception as exc:
        conn.rollback()
        logger.error("Failed to insert job %s: %s", job_id, exc)
        raise
    finally:
        conn.close()


def update_job_status(
    job_id: str,
    status: str,
    *,
    worker_id: Optional[str] = None,
    result: Optional[str] = None,
    error_message: Optional[str] = None,
    lamport_ts: Optional[int] = None,
    claim_token: Optional[str] = None,
) -> bool:
    """Update a job's status and associated metadata with claim fencing.

    When claim_token is provided (worker-owned completion/failure), the update is strictly
    fenced to match the current claim_token and require status == 'running'.
    Returns True if the update was applied, False if rejected (e.g. lease expired / reclaimed).
    """
    now = datetime.now(timezone.utc).isoformat()
    started_at = now if status == "running" else None
    completed_at = now if status in ("completed", "failed", "dead_letter") else None

    if isinstance(result, (dict, list)):
        result = json.dumps(result)

    conn = _get_connection()
    try:
        # Fenced worker-owned transition
        if claim_token is not None:
            cursor = conn.execute(
                """
                UPDATE jobs
                SET status        = ?,
                    worker_id     = COALESCE(?, worker_id),
                    started_at    = COALESCE(?, started_at),
                    completed_at  = COALESCE(?, completed_at),
                    result        = COALESCE(?, result),
                    error_message = COALESCE(?, error_message),
                    claim_token   = NULL,
                    lease_expires_at = NULL,
                    lamport_ts    = CASE WHEN ? IS NOT NULL THEN MAX(lamport_ts, ?) + 1 ELSE lamport_ts END
                WHERE job_id = ?
                  AND status = 'running'
                  AND claim_token = ?
                """,
                (status, worker_id, started_at, completed_at, result, error_message, lamport_ts, lamport_ts, job_id, claim_token),
            )
            if cursor.rowcount > 0:
                conn.commit()
                logger.info("Updated job %s -> status='%s' (fenced by token %s)", job_id, status, claim_token)
                return True
            else:
                conn.rollback()
                logger.warning(
                    "FENCING REJECTED: Worker update for job %s to '%s' failed (claim_token mismatch or job no longer running)",
                    job_id,
                    status,
                )
                return False

        # Administrative / scheduler-owned transition (claim_token is None)
        if status in ("completed", "failed", "dead_letter"):
            cursor = conn.execute(
                """
                UPDATE jobs
                SET status        = ?,
                    worker_id     = COALESCE(?, worker_id),
                    started_at    = COALESCE(?, started_at),
                    completed_at  = COALESCE(?, completed_at),
                    result        = COALESCE(?, result),
                    error_message = COALESCE(?, error_message),
                    claim_token   = NULL,
                    lease_expires_at = NULL,
                    lamport_ts    = CASE WHEN ? IS NOT NULL THEN MAX(lamport_ts, ?) + 1 ELSE lamport_ts END
                WHERE job_id = ?
                """,
                (status, worker_id, started_at, completed_at, result, error_message, lamport_ts, lamport_ts, job_id),
            )
        else:
            cursor = conn.execute(
                """
                UPDATE jobs
                SET status        = ?,
                    worker_id     = COALESCE(?, worker_id),
                    started_at    = COALESCE(?, started_at),
                    completed_at  = COALESCE(?, completed_at),
                    result        = COALESCE(?, result),
                    error_message = COALESCE(?, error_message),
                    lamport_ts    = CASE WHEN ? IS NOT NULL THEN MAX(lamport_ts, ?) + 1 ELSE lamport_ts END
                WHERE job_id = ?
                """,
                (status, worker_id, started_at, completed_at, result, error_message, lamport_ts, lamport_ts, job_id),
            )
        conn.commit()
        logger.info("Updated job %s -> status='%s'", job_id, status)
        return bool(cursor.rowcount > 0)
    except Exception as exc:
        conn.rollback()
        logger.error("Failed to update job %s to status '%s': %s", job_id, status, exc)
        raise
    finally:
        conn.close()


def claim_job_for_worker(
    job_id: str,
    worker_id: str,
    lease_seconds: float = 30.0,
    lamport_ts: int = 0,
) -> Optional[Dict[str, Any]]:
    """Atomically claim a job for execution by a worker.

    Sets status to 'running', records claim identity and timestamps, increments attempt count,
    and returns the claimed job record with a unique claim_token.
    Guards against stealing active, unexpired running jobs (preventing duplicate queue entry race conditions).
    """
    now_dt = datetime.now(timezone.utc)
    now_str = now_dt.isoformat()
    lease_expires_str = (now_dt + timedelta(seconds=lease_seconds)).isoformat()
    claim_token = str(uuid4())

    conn = _get_connection()
    try:
        cursor = conn.execute(
            """
            UPDATE jobs
            SET status = 'running',
                worker_id = ?,
                claimed_at = ?,
                lease_expires_at = ?,
                claim_token = ?,
                started_at = COALESCE(started_at, ?),
                retry_count = retry_count + 1,
                lamport_ts = MAX(lamport_ts, ?) + 1
            WHERE job_id = ?
              AND (
                  status IN ('pending', 'queued')
                  OR (status = 'running' AND lease_expires_at IS NOT NULL AND lease_expires_at < ?)
              )
            """,
            (worker_id, now_str, lease_expires_str, claim_token, now_str, lamport_ts, job_id, now_str),
        )
        conn.commit()
        if cursor.rowcount > 0:
            cursor_sel = conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,))
            row = cursor_sel.fetchone()
            if row:
                logger.info(
                    "Worker %s claimed job %s (attempt %d, token=%s)",
                    worker_id,
                    job_id,
                    row["retry_count"],
                    claim_token,
                )
                return dict(row)
        return None
    except Exception as exc:
        conn.rollback()
        logger.error("Failed to claim job %s for worker %s: %s", job_id, worker_id, exc)
        raise
    finally:
        conn.close()


def find_and_recover_stale_jobs(
    lease_timeout_seconds: float = 30.0,
    lamport_ts: int = 0,
    leader_id: Optional[str] = None,
    leader_epoch: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Identify jobs with expired leases or assigned to offline workers and safely recover them.

    Guards against duplicate/competing recovery sweeps using atomic conditional SQL updates
    and leader epoch fencing inside an atomic BEGIN IMMEDIATE transaction.
    If leader_epoch is provided, verifies that it is >= durable leader epoch before updating.
    If retry_count >= max_retries, transitions to 'dead_letter'.
    Otherwise, resets status to 'pending' for re-enqueueing.
    """
    now_dt = datetime.now(timezone.utc)
    now_str = now_dt.isoformat()
    conn = _get_connection()
    recovered: List[Dict[str, Any]] = []
    try:
        conn.execute("BEGIN IMMEDIATE")
        if leader_epoch is not None:
            validate_and_record_leader_epoch(
                conn,
                leader_id=leader_id or "unknown",
                epoch=leader_epoch,
                scope="scheduler_recovery",
                commit=False,
            )

        query = """
            SELECT j.*, w.status as worker_status
            FROM jobs j
            LEFT JOIN workers w ON j.worker_id = w.worker_id
            WHERE j.status = 'running'
              AND (
                  (j.lease_expires_at IS NOT NULL AND j.lease_expires_at < ?)
                  OR (w.status = 'offline')
              )
        """
        cursor = conn.execute(query, (now_str,))
        stale_jobs = [dict(row) for row in cursor.fetchall()]

        for job in stale_jobs:
            job_id = job["job_id"]
            current_retries = job.get("retry_count", 1)
            max_retries = job.get("max_retries", 3)
            current_token = job.get("claim_token")

            if current_retries >= max_retries:
                err_msg = (
                    f"Exhausted maximum retry attempts ({current_retries}/{max_retries}). "
                    "Worker failed or lease expired."
                )
                upd_cursor = conn.execute(
                    """
                    UPDATE jobs
                    SET status = 'dead_letter',
                        error_message = ?,
                        completed_at = ?,
                        claim_token = NULL,
                        lease_expires_at = NULL,
                        lamport_ts = MAX(lamport_ts, ?) + 1
                    WHERE job_id = ?
                      AND status = 'running'
                      AND (claim_token IS ? OR claim_token = ?)
                      AND (? IS NULL OR (SELECT epoch FROM leader_fencing WHERE scope = 'scheduler_recovery') <= ?)
                    """,
                    (err_msg, now_str, lamport_ts, job_id, current_token, current_token, leader_epoch, leader_epoch),
                )
                if upd_cursor.rowcount > 0:
                    logger.warning("Job %s transitioned to DEAD_LETTER: %s", job_id, err_msg)
                    recovered.append({
                        "job_id": job_id,
                        "action": "dead_letter",
                        "reason": err_msg,
                        "retry_count": current_retries,
                    })
                elif leader_epoch is not None:
                    fence = conn.execute("SELECT epoch FROM leader_fencing WHERE scope = 'scheduler_recovery'").fetchone()
                    if fence and int(fence["epoch"]) > leader_epoch:
                        raise StaleLeaderError(
                            f"Fencing violation during recovery: epoch {leader_epoch} superseded by {fence['epoch']}"
                        )
            else:
                upd_cursor = conn.execute(
                    """
                    UPDATE jobs
                    SET status = 'pending',
                        worker_id = NULL,
                        claim_token = NULL,
                        lease_expires_at = NULL,
                        lamport_ts = MAX(lamport_ts, ?) + 1
                    WHERE job_id = ?
                      AND status = 'running'
                      AND (claim_token IS ? OR claim_token = ?)
                      AND (? IS NULL OR (SELECT epoch FROM leader_fencing WHERE scope = 'scheduler_recovery') <= ?)
                    """,
                    (lamport_ts, job_id, current_token, current_token, leader_epoch, leader_epoch),
                )
                if upd_cursor.rowcount > 0:
                    logger.info(
                        "Job %s recovered from stale worker and reset to 'pending' (attempt %d/%d)",
                        job_id,
                        current_retries,
                        max_retries,
                    )
                    recovered.append({
                        "job_id": job_id,
                        "action": "requeued",
                        "retry_count": current_retries,
                        "max_retries": max_retries,
                    })
                elif leader_epoch is not None:
                    fence = conn.execute("SELECT epoch FROM leader_fencing WHERE scope = 'scheduler_recovery'").fetchone()
                    if fence and int(fence["epoch"]) > leader_epoch:
                        raise StaleLeaderError(
                            f"Fencing violation during recovery: epoch {leader_epoch} superseded by {fence['epoch']}"
                        )

        conn.commit()
        return recovered
    except Exception as exc:
        conn.rollback()
        if not isinstance(exc, StaleLeaderError):
            logger.error("Error during stale job recovery sweep: %s", exc)
        raise
    finally:
        conn.close()


def get_dead_letter_jobs() -> List[Dict[str, Any]]:
    """Return all jobs in 'dead_letter' status."""
    conn = _get_connection()
    try:
        cursor = conn.execute("SELECT * FROM jobs WHERE status = 'dead_letter' ORDER BY completed_at DESC")
        return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()


def reconcile_processing_queue(
    processing_job_ids: List[str],
    lamport_ts: int = 0,
    leader_id: Optional[str] = None,
    leader_epoch: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Reconcile Redis processing queue entries against SQLite durable state (Phase 2.1).

    Idempotently determines whether in-flight queue items:
      - Belong to already completed / failed / dead_letter jobs -> 'evict'
      - Belong to pending jobs (stranded after worker or scheduler crash) -> 'requeue'
      - Do not exist in the database -> 'evict'
      - Are actively running with valid leases -> ignored ('keep')
    If leader_epoch is provided, verifies that it is >= durable leader epoch.
    """
    if not processing_job_ids:
        return []

    actions: List[Dict[str, Any]] = []
    conn = _get_connection()
    try:
        if leader_epoch is not None:
            validate_and_record_leader_epoch(
                conn,
                leader_id=leader_id or "unknown",
                epoch=leader_epoch,
                scope="scheduler_recovery",
            )

        # Deduplicate to prevent redundant operations on duplicate queue entries
        unique_job_ids = list(dict.fromkeys(processing_job_ids))
        for jid in unique_job_ids:
            cursor = conn.execute("SELECT * FROM jobs WHERE job_id = ?", (jid,))
            row = cursor.fetchone()
            if not row:
                actions.append({
                    "job_id": jid,
                    "action": "evict",
                    "reason": "job_not_found_in_database",
                })
                continue

            job = dict(row)
            status = job.get("status")

            if status in ("completed", "failed", "dead_letter"):
                actions.append({
                    "job_id": jid,
                    "action": "evict",
                    "reason": f"job_already_{status}",
                })
            elif status == "pending":
                actions.append({
                    "job_id": jid,
                    "action": "requeue",
                    "reason": "stranded_pending_job",
                })
        return actions
    finally:
        conn.close()


def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    """Return a single job's full row as a dictionary."""
    conn = _get_connection()
    try:
        cursor = conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,))
        row = cursor.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_all_jobs() -> List[Dict[str, Any]]:
    """Return every job in the database, most recent first."""
    conn = _get_connection()
    try:
        cursor = conn.execute("SELECT * FROM jobs ORDER BY submitted_at DESC")
        return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Worker Registry & Heartbeat Operations (Stage 4)
# ---------------------------------------------------------------------------
def register_worker(
    worker_id: str,
    pid: Optional[int] = None,
    startup_time: Optional[str] = None,
) -> None:
    """Register a new worker node or reactivate an existing one (UPSERT).

    Stores worker_id, status, last_heartbeat, registered_at, pid, and startup_time.
    """
    init_db()
    now = datetime.now(timezone.utc).isoformat()
    start_ts = startup_time or now
    conn = _get_connection()
    try:
        conn.execute(
            """
            INSERT INTO workers (worker_id, status, last_heartbeat, registered_at, jobs_completed, pid, startup_time, current_job_id)
            VALUES (?, 'online', ?, ?, 0, ?, ?, NULL)
            ON CONFLICT(worker_id) DO UPDATE SET
                status = 'online',
                last_heartbeat = excluded.last_heartbeat,
                pid = COALESCE(excluded.pid, workers.pid),
                startup_time = COALESCE(excluded.startup_time, workers.startup_time)
            """,
            (worker_id, now, now, pid, start_ts),
        )
        conn.commit()
        logger.info("Worker %s registered/online (pid=%s)", worker_id, pid)
    finally:
        conn.close()


def update_heartbeat(worker_id: str, current_job_id: Optional[str] = None) -> None:
    """Update a worker's ``last_heartbeat`` timestamp to now and set status to ``'online'``."""
    now = datetime.now(timezone.utc).isoformat()
    conn = _get_connection()
    try:
        if current_job_id is not None:
            conn.execute(
                """
                UPDATE workers
                SET last_heartbeat = ?,
                    status = 'online',
                    current_job_id = ?
                WHERE worker_id = ?
                """,
                (now, current_job_id, worker_id),
            )
        else:
            conn.execute(
                """
                UPDATE workers
                SET last_heartbeat = ?,
                    status = 'online'
                WHERE worker_id = ?
                """,
                (now, worker_id),
            )
        conn.commit()
    finally:
        conn.close()


def clean_test_artifacts() -> Dict[str, int]:
    """Purge synthetic test jobs and test workers that may have been created during unit testing."""
    conn = _get_connection()
    try:
        cursor_jobs = conn.execute(
            """
            DELETE FROM jobs
            WHERE job_id LIKE 'test-%'
               OR job_id LIKE 'stranded-%'
               OR job_id LIKE 'reconcile-%'
               OR job_id LIKE 'fence-%'
               OR job_id LIKE 'crash-window-%'
               OR job_id LIKE 'retry-limit-%'
               OR job_id LIKE 'running-missing-%'
               OR job_id LIKE 'active-lease-%'
               OR job_id LIKE 'job-fail-test-%'
            """
        )
        cursor_workers = conn.execute(
            """
            DELETE FROM workers
            WHERE worker_id LIKE 'worker-test-%'
               OR worker_id LIKE 'worker-crashed-%'
               OR worker_id LIKE 'worker-dead-%'
               OR worker_id LIKE 'worker-flaky-%'
               OR worker_id LIKE 'worker-success%'
               OR worker_id LIKE 'worker-indep%'
               OR worker_id LIKE 'worker-1'
               OR worker_id LIKE 'worker-2'
               OR worker_id LIKE 'w3'
            """
        )
        conn.commit()
        logger.info(
            "Purged test artifacts from SQLite: %d jobs, %d workers",
            cursor_jobs.rowcount,
            cursor_workers.rowcount,
        )
        return {
            "deleted_jobs": cursor_jobs.rowcount,
            "deleted_workers": cursor_workers.rowcount,
        }
    finally:
        conn.close()


def increment_jobs_completed(worker_id: str) -> None:
    """Increment the ``jobs_completed`` counter for a worker node."""
    conn = _get_connection()
    try:
        conn.execute(
            """
            UPDATE workers
            SET jobs_completed = jobs_completed + 1
            WHERE worker_id = ?
            """,
            (worker_id,),
        )
        conn.commit()
        logger.info("Incremented jobs_completed for worker %s", worker_id)
    finally:
        conn.close()


def get_all_workers() -> List[Dict[str, Any]]:
    """Return all registered workers ordered by registration time."""
    conn = _get_connection()
    try:
        cursor = conn.execute("SELECT * FROM workers ORDER BY registered_at ASC")
        return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()


def mark_stale_workers_offline(
    timeout_seconds: float = 15.0,
    leader_id: Optional[str] = None,
    leader_epoch: Optional[int] = None,
) -> List[str]:
    """Find workers whose last heartbeat is older than timeout_seconds and set status='offline'.

    Guards against races against both worker heartbeats (CAS on last_heartbeat) and newer
    leaders (epoch fencing check in SQL) inside an atomic BEGIN IMMEDIATE transaction.
    If leader_epoch is provided, verifies that it is >= durable leader epoch before updating.

    Returns
    -------
    list[str]
        List of worker_ids that were newly marked offline.
    """
    now_dt = datetime.now(timezone.utc)
    conn = _get_connection()
    marked_offline: List[str] = []
    try:
        conn.execute("BEGIN IMMEDIATE")
        if leader_epoch is not None:
            validate_and_record_leader_epoch(
                conn,
                leader_id=leader_id or "unknown",
                epoch=leader_epoch,
                scope="scheduler_recovery",
                commit=False,
            )

        cursor = conn.execute("SELECT worker_id, last_heartbeat FROM workers WHERE status = 'online'")
        rows = cursor.fetchall()
        stale_candidates: List[Tuple[str, str]] = []

        for row in rows:
            w_id = row["worker_id"]
            hb_str = row["last_heartbeat"]
            try:
                hb_dt = datetime.fromisoformat(hb_str)
                elapsed = (now_dt - hb_dt).total_seconds()
                if elapsed > timeout_seconds:
                    stale_candidates.append((w_id, hb_str))
            except ValueError:
                # Malformed timestamp string; candidate for offline
                stale_candidates.append((w_id, hb_str))

        for w_id, hb_str in stale_candidates:
            upd = conn.execute(
                """
                UPDATE workers
                SET status = 'offline'
                WHERE worker_id = ?
                  AND status = 'online'
                  AND last_heartbeat = ?
                  AND (? IS NULL OR (SELECT epoch FROM leader_fencing WHERE scope = 'scheduler_recovery') <= ?)
                """,
                (w_id, hb_str, leader_epoch, leader_epoch),
            )
            if upd.rowcount > 0:
                marked_offline.append(w_id)
            elif leader_epoch is not None:
                fence = conn.execute("SELECT epoch FROM leader_fencing WHERE scope = 'scheduler_recovery'").fetchone()
                if fence and int(fence["epoch"]) > leader_epoch:
                    raise StaleLeaderError(
                        f"Fencing violation during worker staleness demotion: epoch {leader_epoch} superseded by {fence['epoch']}"
                    )

        if marked_offline:
            logger.warning("Marked %d workers OFFLINE: %s", len(marked_offline), marked_offline)

        conn.commit()
        return marked_offline
    except Exception as exc:
        conn.rollback()
        if not isinstance(exc, StaleLeaderError):
            logger.error("Error in mark_stale_workers_offline: %s", exc)
        raise
    finally:
        conn.close()
