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
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from common.config import DB_PATH
from common.logging_config import setup_logger

logger = setup_logger("database")


def _get_connection() -> sqlite3.Connection:
    """Open a connection to the SQLite database with Row factory and WAL mode."""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db() -> None:
    """Create ``jobs`` and ``workers`` tables if they do not exist."""
    conn = _get_connection()
    try:
        # 1. Jobs table
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
                error_message   TEXT
            )
        """)

        # 2. Workers table (Stage 4 Worker Registry)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS workers (
                worker_id       TEXT PRIMARY KEY,
                status          TEXT    NOT NULL DEFAULT 'online',
                last_heartbeat  TEXT    NOT NULL,
                registered_at   TEXT    NOT NULL,
                jobs_completed  INTEGER NOT NULL DEFAULT 0
            )
        """)

        conn.commit()
        logger.info("Database initialised (jobs & workers tables) at %s", DB_PATH)
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
) -> None:
    """Insert a new job row with status ``'pending'``."""
    conn = _get_connection()
    try:
        conn.execute(
            """
            INSERT INTO jobs (job_id, job_type, payload, status, submitted_at)
            VALUES (?, ?, ?, 'pending', ?)
            """,
            (job_id, job_type, json.dumps(payload), submitted_at),
        )
        conn.commit()
        logger.info("Inserted job %s with status 'pending'", job_id)
    finally:
        conn.close()


def update_job_status(
    job_id: str,
    status: str,
    *,
    worker_id: Optional[str] = None,
    result: Optional[str] = None,
    error_message: Optional[str] = None,
) -> None:
    """Update a job's status and associated metadata."""
    now = datetime.now(timezone.utc).isoformat()
    started_at = now if status == "running" else None
    completed_at = now if status in ("completed", "failed") else None

    conn = _get_connection()
    try:
        conn.execute(
            """
            UPDATE jobs
            SET status        = ?,
                worker_id     = COALESCE(?, worker_id),
                started_at    = COALESCE(?, started_at),
                completed_at  = COALESCE(?, completed_at),
                result        = COALESCE(?, result),
                error_message = COALESCE(?, error_message)
            WHERE job_id = ?
            """,
            (status, worker_id, started_at, completed_at, result, error_message, job_id),
        )
        conn.commit()
        logger.info("Updated job %s -> status='%s'", job_id, status)
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
def register_worker(worker_id: str) -> None:
    """Register a new worker node or reactivate an existing one (UPSERT).

    Uses SQLite ``INSERT ... ON CONFLICT`` to atomically insert or update
    the worker row, setting ``status='online'`` and updating ``last_heartbeat``.
    """
    init_db()
    now = datetime.now(timezone.utc).isoformat()
    conn = _get_connection()
    try:
        conn.execute(
            """
            INSERT INTO workers (worker_id, status, last_heartbeat, registered_at, jobs_completed)
            VALUES (?, 'online', ?, ?, 0)
            ON CONFLICT(worker_id) DO UPDATE SET
                status = 'online',
                last_heartbeat = excluded.last_heartbeat
            """,
            (worker_id, now, now),
        )
        conn.commit()
        logger.info("Worker %s registered/online", worker_id)
    finally:
        conn.close()


def update_heartbeat(worker_id: str) -> None:
    """Update a worker's ``last_heartbeat`` timestamp to now and set status to ``'online'``."""
    now = datetime.now(timezone.utc).isoformat()
    conn = _get_connection()
    try:
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


def mark_stale_workers_offline(timeout_seconds: float = 15.0) -> List[str]:
    """Find workers whose last heartbeat is older than timeout_seconds and set status='offline'.

    Returns
    -------
    list[str]
        List of worker_ids that were newly marked offline.
    """
    now_dt = datetime.now(timezone.utc)
    conn = _get_connection()
    newly_offline = []
    try:
        cursor = conn.execute("SELECT worker_id, last_heartbeat FROM workers WHERE status = 'online'")
        rows = cursor.fetchall()

        for row in rows:
            w_id = row["worker_id"]
            hb_str = row["last_heartbeat"]
            try:
                hb_dt = datetime.fromisoformat(hb_str)
                elapsed = (now_dt - hb_dt).total_seconds()
                if elapsed > timeout_seconds:
                    newly_offline.append(w_id)
            except ValueError:
                # Malformed timestamp string; mark offline safely
                newly_offline.append(w_id)

        if newly_offline:
            placeholders = ",".join("?" for _ in newly_offline)
            conn.execute(
                f"UPDATE workers SET status = 'offline' WHERE worker_id IN ({placeholders})",
                newly_offline,
            )
            conn.commit()
            logger.warning("Marked %d workers OFFLINE: %s", len(newly_offline), newly_offline)

        return newly_offline
    finally:
        conn.close()
