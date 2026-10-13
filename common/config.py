"""
Centralized configuration settings for the Distributed Job Processing Platform.

This module consolidates all operational parameters, environment variable overrides,
network ports, timeouts, and file paths across the platform.

Centralizing configuration ensures:
  1. Consistency across Scheduler, Worker, and Test Client services.
  2. Easy environment tuning via environment variables (12-Factor App methodology).
  3. Clear documentation of all magic numbers for academic evaluation and viva defense.
"""

import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

# Load environment variables from .env file if present
load_dotenv()

# ---------------------------------------------------------------------------
# Base Paths & Database Settings
# ---------------------------------------------------------------------------
BASE_DIR: Path = Path(__file__).resolve().parent.parent

def get_db_path() -> Path:
    """Resolve database path dynamically from environment variable (enables test isolation)."""
    db_name = os.getenv("JOB_PLATFORM_DB_NAME", "job_platform.db")
    return BASE_DIR / db_name

DB_FILENAME: str = os.getenv("JOB_PLATFORM_DB_NAME", "job_platform.db")
DB_PATH: Path = get_db_path()
DATABASE_URL: Optional[str] = os.getenv("DATABASE_URL")
POSTGRES_CONNECT_TIMEOUT_SECONDS: float = float(os.getenv("POSTGRES_CONNECT_TIMEOUT_SECONDS", "10.0"))
SQLITE_TIMEOUT_SECONDS: float = float(os.getenv("SQLITE_TIMEOUT_SECONDS", "30.0"))

# ---------------------------------------------------------------------------
# Scheduler Service Settings
# ---------------------------------------------------------------------------
SCHEDULER_HOST: str = os.getenv("SCHEDULER_HOST", "localhost")
SCHEDULER_PORT: int = int(os.getenv("SCHEDULER_PORT", "8000"))
SCHEDULER_URL: str = f"http://{SCHEDULER_HOST}:{SCHEDULER_PORT}"

# Background liveness monitor settings
STALENESS_CHECK_INTERVAL_SECONDS: float = float(os.getenv("STALENESS_CHECK_INTERVAL_SECONDS", "5.0"))
WORKER_STALENESS_TIMEOUT_SECONDS: float = float(os.getenv("WORKER_STALENESS_TIMEOUT_SECONDS", "15.0"))

# ---------------------------------------------------------------------------
# Worker Service Settings
# ---------------------------------------------------------------------------
WORKER_HTTP_PORT: int = int(os.getenv("WORKER_HTTP_PORT", "8001"))
WORKER_HTTP_URL: str = f"http://localhost:{WORKER_HTTP_PORT}"
HEARTBEAT_INTERVAL_SECONDS: float = float(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "5.0"))
SIMULATED_JOB_DURATION_SECONDS: float = float(os.getenv("SIMULATED_JOB_DURATION_SECONDS", "2.0"))

# ---------------------------------------------------------------------------
# Message Queue & Reliable Delivery Settings (Redis)
# ---------------------------------------------------------------------------
REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB: int = int(os.getenv("REDIS_DB", "0"))
QUEUE_NAME: str = os.getenv("QUEUE_NAME", "job_queue")
PROCESSING_QUEUE_NAME: str = os.getenv("PROCESSING_QUEUE_NAME", f"{QUEUE_NAME}:processing")
REDIS_BRPOP_TIMEOUT_SECONDS: int = int(os.getenv("REDIS_BRPOP_TIMEOUT_SECONDS", "5"))

# At-least-once lease & recovery settings (Phase 2 & Phase 2.1)
# RETRY CONVENTION: MAX_JOB_RETRIES defines the maximum total execution attempts
# (1 initial execution attempt + up to MAX_JOB_RETRIES - 1 recovery retries).
# Once attempt count reaches MAX_JOB_RETRIES without durable success, the job
# transitions to the terminal 'dead_letter' state.
JOB_LEASE_TIMEOUT_SECONDS: float = float(os.getenv("JOB_LEASE_TIMEOUT_SECONDS", "30.0"))
MAX_JOB_RETRIES: int = int(os.getenv("MAX_JOB_RETRIES", "3"))
STALE_JOB_CHECK_INTERVAL_SECONDS: float = float(os.getenv("STALE_JOB_CHECK_INTERVAL_SECONDS", "5.0"))
RETRY_DELAY_SECONDS: float = float(os.getenv("RETRY_DELAY_SECONDS", "5.0"))

# ---------------------------------------------------------------------------
# Dashboard UI Settings
# ---------------------------------------------------------------------------
DASHBOARD_POLL_INTERVAL_MS: int = int(os.getenv("DASHBOARD_POLL_INTERVAL_MS", "2000"))
DASHBOARD_RECENT_JOBS_LIMIT: int = int(os.getenv("DASHBOARD_RECENT_JOBS_LIMIT", "15"))

# ---------------------------------------------------------------------------
# Scheduler Leadership, Leases & Fencing Settings (Unit III — Synchronization)
# ---------------------------------------------------------------------------
SCHEDULER_ID: str = os.getenv("SCHEDULER_ID", f"scheduler-{os.getpid()}")
LEADER_LEASE_KEY: str = os.getenv("LEADER_LEASE_KEY", "scheduler:leader:lease")
LEADER_EPOCH_KEY: str = os.getenv("LEADER_EPOCH_KEY", "scheduler:leader:epoch")
LEADER_LEASE_TTL_SECONDS: float = float(os.getenv("LEADER_LEASE_TTL_SECONDS", "6.0"))
LEADER_RENEWAL_INTERVAL_SECONDS: float = float(os.getenv("LEADER_RENEWAL_INTERVAL_SECONDS", "2.0"))

