"""
Authoritative Data Migration Utility: SQLite -> PostgreSQL 16+
Part of Distributed Job Platform — Phase 2 Multi-Process Cluster.

This tool safely and idempotently migrates durable state (jobs, workers, leader leases,
and schema migration records) from local SQLite persistence into authoritative PostgreSQL.

Usage:
    python -m tools.migrate_sqlite_to_postgres --sqlite-path job_platform.db --pg-dsn postgresql://postgres:postgres@localhost:5432/job_platform
    or with environment variable:
    DATABASE_URL=postgresql://postgres:postgres@localhost:5432/job_platform python -m tools.migrate_sqlite_to_postgres
"""

import argparse
import os
import sqlite3
import sys
from pathlib import Path
from typing import Dict, Any

import psycopg2
from psycopg2.extras import RealDictCursor, execute_batch

from common.logging_config import setup_logger

logger = setup_logger("migration")


def migrate_sqlite_to_postgres(sqlite_path: Path, pg_dsn: str) -> Dict[str, int]:
    """Migrate all persistent entities from SQLite into PostgreSQL with idempotent conflict handling."""
    if not sqlite_path.exists():
        raise FileNotFoundError(f"Source SQLite database not found at {sqlite_path}")

    logger.info("Starting migration from SQLite [%s] -> PostgreSQL [%s]", sqlite_path, pg_dsn)

    sqlite_conn = sqlite3.connect(str(sqlite_path))
    sqlite_conn.row_factory = sqlite3.Row

    pg_conn = psycopg2.connect(pg_dsn)

    stats = {
        "schema_migrations": 0,
        "leader_fencing": 0,
        "jobs": 0,
        "workers": 0,
    }

    try:
        with pg_conn:
            pg_cur = pg_conn.cursor()

            # Ensure schema tables exist in PostgreSQL
            os.environ["DATABASE_URL"] = pg_dsn
            from common.database import init_db
            init_db()

            # 1. Migrate Schema Migrations
            cursor = sqlite_conn.execute("SELECT * FROM schema_migrations ORDER BY version ASC")
            migrations = [dict(r) for r in cursor.fetchall()]
            for m in migrations:
                pg_cur.execute(
                    """
                    INSERT INTO schema_migrations (version, name, applied_at)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (version) DO NOTHING
                    """,
                    (m["version"], m["name"], m["applied_at"]),
                )
            stats["schema_migrations"] = len(migrations)
            logger.info("Migrated %d schema migration records", len(migrations))

            # 2. Migrate Leader Fencing
            cursor = sqlite_conn.execute("SELECT * FROM leader_fencing")
            fences = [dict(r) for r in cursor.fetchall()]
            for f in fences:
                pg_cur.execute(
                    """
                    INSERT INTO leader_fencing (scope, leader_id, epoch, updated_at)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (scope) DO UPDATE SET
                        leader_id = EXCLUDED.leader_id,
                        epoch = EXCLUDED.epoch,
                        updated_at = EXCLUDED.updated_at
                    WHERE EXCLUDED.epoch >= leader_fencing.epoch
                    """,
                    (f["scope"], f["leader_id"], f["epoch"], f["updated_at"]),
                )
            stats["leader_fencing"] = len(fences)
            logger.info("Migrated %d leader fencing records", len(fences))

            # 3. Migrate Jobs
            cursor = sqlite_conn.execute("SELECT * FROM jobs")
            jobs = [dict(r) for r in cursor.fetchall()]
            job_rows = []
            for j in jobs:
                job_rows.append((
                    j["job_id"],
                    j["job_type"],
                    j.get("payload") or "{}",
                    j.get("status") or "pending",
                    j.get("worker_id"),
                    j["submitted_at"],
                    j.get("started_at"),
                    j.get("completed_at"),
                    j.get("result"),
                    j.get("error_message"),
                    j.get("retry_count", 0),
                    j.get("max_retries", 3),
                    j.get("claimed_at"),
                    j.get("lease_expires_at"),
                    j.get("claim_token"),
                    j.get("lamport_ts", 0),
                ))

            if job_rows:
                execute_batch(
                    pg_cur,
                    """
                    INSERT INTO jobs (
                        job_id, job_type, payload, status, worker_id,
                        submitted_at, started_at, completed_at, result,
                        error_message, retry_count, max_retries, claimed_at,
                        lease_expires_at, claim_token, lamport_ts
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (job_id) DO UPDATE SET
                        status = EXCLUDED.status,
                        worker_id = COALESCE(EXCLUDED.worker_id, jobs.worker_id),
                        started_at = COALESCE(EXCLUDED.started_at, jobs.started_at),
                        completed_at = COALESCE(EXCLUDED.completed_at, jobs.completed_at),
                        result = COALESCE(EXCLUDED.result, jobs.result),
                        error_message = COALESCE(EXCLUDED.error_message, jobs.error_message),
                        retry_count = EXCLUDED.retry_count,
                        max_retries = EXCLUDED.max_retries,
                        claimed_at = EXCLUDED.claimed_at,
                        lease_expires_at = EXCLUDED.lease_expires_at,
                        claim_token = EXCLUDED.claim_token,
                        lamport_ts = GREATEST(EXCLUDED.lamport_ts, jobs.lamport_ts)
                    """,
                    job_rows,
                    page_size=200,
                )
            stats["jobs"] = len(jobs)
            logger.info("Migrated %d job records into PostgreSQL", len(jobs))

            # 4. Migrate Workers
            cursor = sqlite_conn.execute("SELECT * FROM workers")
            workers = [dict(r) for r in cursor.fetchall()]
            for w in workers:
                pg_cur.execute(
                    """
                    INSERT INTO workers (
                        worker_id, status, last_heartbeat, registered_at,
                        jobs_completed, pid, startup_time, current_job_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (worker_id) DO UPDATE SET
                        status = EXCLUDED.status,
                        last_heartbeat = EXCLUDED.last_heartbeat,
                        jobs_completed = GREATEST(EXCLUDED.jobs_completed, workers.jobs_completed),
                        pid = COALESCE(EXCLUDED.pid, workers.pid),
                        startup_time = COALESCE(EXCLUDED.startup_time, workers.startup_time),
                        current_job_id = EXCLUDED.current_job_id
                    """,
                    (
                        w["worker_id"],
                        w.get("status", "online"),
                        w["last_heartbeat"],
                        w["registered_at"],
                        w.get("jobs_completed", 0),
                        w.get("pid"),
                        w.get("startup_time"),
                        w.get("current_job_id"),
                    ),
                )
            stats["workers"] = len(workers)
            logger.info("Migrated %d worker records into PostgreSQL", len(workers))

        logger.info("Migration completed successfully: %s", stats)
        return stats
    finally:
        sqlite_conn.close()
        pg_conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate SQLite data to authoritative PostgreSQL.")
    parser.add_argument(
        "--sqlite-path",
        type=Path,
        default=Path("job_platform.db"),
        help="Path to source SQLite database file (default: job_platform.db)",
    )
    parser.add_argument(
        "--pg-dsn",
        type=str,
        default=os.getenv("DATABASE_URL"),
        help="PostgreSQL DSN connection URL (or set DATABASE_URL environment variable)",
    )

    args = parser.parse_args()
    if not args.pg_dsn:
        logger.error("No PostgreSQL DSN provided. Specify --pg-dsn or set DATABASE_URL.")
        sys.exit(1)

    try:
        stats = migrate_sqlite_to_postgres(args.sqlite_path, args.pg_dsn)
        print("\n" + "=" * 50)
        print("MIGRATION SUMMARY:")
        for k, v in stats.items():
            print(f"  {k:20s}: {v}")
        print("=" * 50)
    except Exception as exc:
        logger.exception("Migration failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
