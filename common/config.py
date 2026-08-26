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

# ---------------------------------------------------------------------------
# Base Paths
# ---------------------------------------------------------------------------
BASE_DIR: Path = Path(__file__).resolve().parent.parent
DB_FILENAME: str = os.getenv("JOB_PLATFORM_DB_NAME", "job_platform.db")
DB_PATH: Path = BASE_DIR / DB_FILENAME

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
# Message Queue Settings (Redis)
# ---------------------------------------------------------------------------
REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB: int = int(os.getenv("REDIS_DB", "0"))
QUEUE_NAME: str = os.getenv("QUEUE_NAME", "job_queue")
REDIS_BRPOP_TIMEOUT_SECONDS: int = int(os.getenv("REDIS_BRPOP_TIMEOUT_SECONDS", "5"))

# ---------------------------------------------------------------------------
# Dashboard UI Settings
# ---------------------------------------------------------------------------
DASHBOARD_POLL_INTERVAL_MS: int = int(os.getenv("DASHBOARD_POLL_INTERVAL_MS", "2000"))
DASHBOARD_RECENT_JOBS_LIMIT: int = int(os.getenv("DASHBOARD_RECENT_JOBS_LIMIT", "15"))
