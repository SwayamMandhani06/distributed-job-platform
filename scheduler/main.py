"""
Scheduler Service — the central coordinator of the distributed job platform.

Architecture role
-----------------
The scheduler is the **entry point** for all job submissions. External clients
send job requests here via HTTP.

Stage 5 Additions (Live Web Dashboard & System Observability)
-------------------------------------------------------------
  1. **Observability Concept**: In a distributed system composed of independent,
     decoupled processes operating without shared memory, you cannot attach a debugger
     or inspect a single process's heap to understand system state.
     Observability (aggregating metrics, logs, worker liveness, and job history into
     a unified view) is itself a fundamental distributed systems design concern.
  2. **Root Route (`GET /`)**: Serves a live, self-updating HTML dashboard.
  3. **Aggregated API Route (`GET /api/dashboard_data`)**: Single-query endpoint returning
     metrics cards data, worker status list, and recent job history.
"""

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator, List
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from common.config import (
    STALENESS_CHECK_INTERVAL_SECONDS,
    WORKER_STALENESS_TIMEOUT_SECONDS,
    DASHBOARD_RECENT_JOBS_LIMIT,
)
from common.logging_config import setup_logger
from common.database import (
    init_db,
    insert_job,
    get_job,
    get_all_jobs,
    get_all_workers,
    mark_stale_workers_offline,
)
from common.queue_client import JobQueue
from scheduler.models import JobRequest, JobResponse

# ---------------------------------------------------------------------------
# Application bootstrap & Templates
# ---------------------------------------------------------------------------
logger = setup_logger("scheduler")
TEMPLATES_DIR = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


async def staleness_checker_loop(
    check_interval_seconds: float = STALENESS_CHECK_INTERVAL_SECONDS,
    timeout_seconds: float = WORKER_STALENESS_TIMEOUT_SECONDS,
) -> None:
    """Async background task that periodically checks for stale worker heartbeats."""
    logger.info("Started background worker staleness monitor (interval=%.1fs, timeout=%.1fs)", check_interval_seconds, timeout_seconds)
    while True:
        try:
            stale_ids = mark_stale_workers_offline(timeout_seconds=timeout_seconds)
            if stale_ids:
                logger.warning(
                    "STALENESS CHECKER: Worker(s) %s marked OFFLINE (no heartbeat in >%.0fs)",
                    stale_ids,
                    timeout_seconds,
                )
        except asyncio.CancelledError:
            logger.info("Staleness monitor loop cancelled.")
            break
        except Exception as exc:
            logger.error("Error in staleness checker loop: %s", exc)

        await asyncio.sleep(check_interval_seconds)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Manage the scheduler's startup and shutdown lifecycle."""
    # ---- Startup ----------------------------------------------------------
    init_db()
    app.state.queue = JobQueue()
    checker_task = asyncio.create_task(staleness_checker_loop(check_interval_seconds=5.0, timeout_seconds=15.0))
    logger.info("Scheduler service started — Redis queue, heartbeat monitor & Live Dashboard online")

    yield

    # ---- Shutdown ---------------------------------------------------------
    checker_task.cancel()
    try:
        await checker_task
    except asyncio.CancelledError:
        pass
    logger.info("Scheduler service shutting down")


app = FastAPI(
    title="Scheduler Service",
    description=(
        "Central coordinator that accepts job submissions, persists job & worker "
        "metadata in SQLite, enqueues tasks into Redis, monitors worker liveness, and serves a live web dashboard."
    ),
    version="0.5.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Dashboard Routes (Stage 5)
# ---------------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse, summary="Live Web Dashboard")
def render_dashboard(request: Request) -> HTMLResponse:
    """Serve the single-page live-updating HTML dashboard."""
    html_file = TEMPLATES_DIR / "dashboard.html"
    return HTMLResponse(content=html_file.read_text(encoding="utf-8"))


@app.get("/api/dashboard_data", summary="Aggregated Dashboard Metrics & History")
def get_dashboard_data(request: Request) -> dict:
    """Single aggregated JSON payload for the live web dashboard.

    Avoids multiple client-side HTTP calls by returning job counts, worker list,
    queue length, and recent job history in a single atomic response.
    """
    all_jobs = get_all_jobs()
    workers = get_all_workers()

    # Compute job counts by status
    job_counts = {"pending": 0, "running": 0, "completed": 0, "failed": 0}
    for job in all_jobs:
        status = job.get("status", "pending")
        if status in job_counts:
            job_counts[status] += 1
        elif status == "queued":
            job_counts["pending"] += 1

    total_workers = len(workers)
    online_workers = sum(1 for w in workers if w.get("status") == "online")

    queue_len = 0
    try:
        queue: JobQueue = request.app.state.queue
        queue_len = queue.queue_length()
    except Exception:
        queue_len = -1

    return {
        "job_counts": job_counts,
        "total_workers": total_workers,
        "online_workers": online_workers,
        "pending_queue_length": queue_len,
        "workers": workers,
        "recent_jobs": all_jobs[:DASHBOARD_RECENT_JOBS_LIMIT],
    }


# ---------------------------------------------------------------------------
# API Endpoints
# ---------------------------------------------------------------------------
@app.post(
    "/submit_job",
    response_model=JobResponse,
    status_code=202,
    summary="Submit a new job for asynchronous distributed execution",
)
def submit_job(job: JobRequest, request: Request) -> JobResponse:
    """Accept a job, persist it as 'pending', enqueue it in Redis, and return immediately."""
    job_id = str(uuid4())
    submitted_at = job.submitted_at.isoformat()

    logger.info(
        "Received job submission | id=%s  type=%s  payload_keys=%s",
        job_id,
        job.job_type,
        list(job.payload.keys()),
    )

    # 1. Persist job in SQLite
    insert_job(
        job_id=job_id,
        job_type=job.job_type,
        payload=job.payload,
        submitted_at=submitted_at,
    )

    # 2. Push job_id onto Redis queue
    try:
        queue: JobQueue = request.app.state.queue
        queue.push_job(job_id)
    except Exception as exc:
        logger.error("Failed to enqueue job %s in Redis: %s", job_id, exc)
        raise HTTPException(
            status_code=500,
            detail=f"Database saved job, but queue dispatch failed: {exc}",
        )

    logger.info("Job %s enqueued into Redis successfully", job_id)

    # 3. Return 202 Accepted with status 'queued'
    return JobResponse(
        job_id=job_id,
        status="queued",
        message="Job successfully accepted and enqueued into Redis message queue.",
    )


@app.get(
    "/job/{job_id}",
    summary="Get a job's current state",
    responses={404: {"description": "Job not found"}},
)
def get_job_status(job_id: str) -> dict:
    """Look up a single job by ID from SQLite."""
    job = get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")
    return job


@app.get(
    "/jobs",
    summary="List all jobs",
    response_model=List[dict],
)
def list_jobs() -> list:
    """Return every job in the database, most recent first."""
    return get_all_jobs()


@app.get(
    "/workers",
    summary="List all registered workers and their health status",
    response_model=List[dict],
)
def list_workers() -> list:
    """Return all registered workers, their liveness status, and job completion metrics."""
    return get_all_workers()


@app.get("/health", summary="Health check & system metrics")
def health_check(request: Request) -> dict:
    """Lightweight liveness, queue depth, and worker health probe."""
    queue_len = 0
    try:
        queue: JobQueue = request.app.state.queue
        queue_len = queue.queue_length()
    except Exception:
        queue_len = -1

    workers = get_all_workers()
    total_workers = len(workers)
    online_workers = sum(1 for w in workers if w.get("status") == "online")

    return {
        "service": "scheduler",
        "status": "healthy",
        "pending_queue_length": queue_len,
        "total_workers": total_workers,
        "online_workers": online_workers,
    }
