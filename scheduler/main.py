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
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncGenerator, List, Optional
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from common.clocks import LamportClock
from common.config import (
    STALENESS_CHECK_INTERVAL_SECONDS,
    WORKER_STALENESS_TIMEOUT_SECONDS,
    DASHBOARD_RECENT_JOBS_LIMIT,
    JOB_LEASE_TIMEOUT_SECONDS,
    STALE_JOB_CHECK_INTERVAL_SECONDS,
    SCHEDULER_ID,
    LEADER_LEASE_TTL_SECONDS,
    LEADER_RENEWAL_INTERVAL_SECONDS,
)
from common.logging_config import setup_logger
from common.database import (
    init_db,
    insert_job,
    update_job_status,
    get_job,
    get_all_jobs,
    get_all_workers,
    mark_stale_workers_offline,
    find_and_recover_stale_jobs,
    get_dead_letter_jobs,
    reconcile_processing_queue,
    get_current_fencing_epoch,
    StaleLeaderError,
)
from common.leader_lease import LeaderLeaseManager
from common.queue_client import JobQueue
from scheduler.models import JobRequest, JobResponse

# ---------------------------------------------------------------------------
# Application bootstrap & Templates
# ---------------------------------------------------------------------------
logger = setup_logger("scheduler")
TEMPLATES_DIR = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# Coordinator Lamport Logical Clock (Unit III — Synchronization)
lamport_clock = LamportClock()


async def leader_lease_loop(
    app: FastAPI,
    interval_seconds: float = LEADER_RENEWAL_INTERVAL_SECONDS,
) -> None:
    """Async background task to manage leader lease acquisition and periodic renewal (Unit III)."""
    logger.info("Started background leader lease manager loop (interval=%.1fs)", interval_seconds)
    while True:
        try:
            lease_mgr: Optional[LeaderLeaseManager] = getattr(app.state, "lease_manager", None)
            if lease_mgr:
                if lease_mgr.is_leader:
                    renewed = await asyncio.to_thread(lease_mgr.renew_lease)
                    if not renewed:
                        logger.warning("Scheduler %s lost leadership lease during renewal.", lease_mgr.leader_id)
                else:
                    acquired, epoch = await asyncio.to_thread(lease_mgr.try_acquire_lease)
                    if acquired:
                        logger.info("Scheduler %s acquired leadership lease (Epoch %d)", lease_mgr.leader_id, epoch)
        except asyncio.CancelledError:
            logger.info("Leader lease loop cancelled.")
            break
        except Exception as exc:
            logger.error("Error in leader lease loop: %s", exc)

        await asyncio.sleep(interval_seconds)


async def staleness_checker_loop(
    app: FastAPI,
    check_interval_seconds: float = STALENESS_CHECK_INTERVAL_SECONDS,
    timeout_seconds: float = WORKER_STALENESS_TIMEOUT_SECONDS,
) -> None:
    """Async background task that periodically checks for stale worker heartbeats.

    Only the authoritative leader scheduler executes worker staleness marking.
    """
    logger.info("Started background worker staleness monitor (interval=%.1fs, timeout=%.1fs)", check_interval_seconds, timeout_seconds)
    while True:
        try:
            lease_mgr: Optional[LeaderLeaseManager] = getattr(app.state, "lease_manager", None)
            # Only authoritative leader executes worker staleness monitoring
            if lease_mgr is not None and not lease_mgr.is_leader:
                await asyncio.sleep(check_interval_seconds)
                continue

            leader_id = lease_mgr.leader_id if lease_mgr else SCHEDULER_ID
            leader_epoch = lease_mgr.current_epoch if lease_mgr else 0

            try:
                stale_ids = mark_stale_workers_offline(
                    timeout_seconds=timeout_seconds,
                    leader_id=leader_id,
                    leader_epoch=leader_epoch,
                )
                if stale_ids:
                    logger.warning(
                        "STALENESS CHECKER: Worker(s) %s marked OFFLINE (no heartbeat in >%.0fs)",
                        stale_ids,
                        timeout_seconds,
                    )
            except StaleLeaderError as sle:
                logger.warning("StaleLeaderError in worker staleness checker: %s. Stepping down.", sle)
                if lease_mgr:
                    lease_mgr.step_down()
        except asyncio.CancelledError:
            logger.info("Staleness monitor loop cancelled.")
            break
        except Exception as exc:
            logger.error("Error in staleness checker loop: %s", exc)

        await asyncio.sleep(check_interval_seconds)


async def stale_job_recovery_loop(
    app: FastAPI,
    check_interval_seconds: float = STALE_JOB_CHECK_INTERVAL_SECONDS,
    lease_timeout_seconds: float = JOB_LEASE_TIMEOUT_SECONDS,
) -> None:
    """Async background task that periodically inspects, recovers, and reconciles stale jobs (Phase 2, 2.1 & 3).

    Only the active leader performs authoritative job recovery sweeps guarded by fencing tokens.
    """
    logger.info(
        "Started background stale job recovery monitor (interval=%.1fs, lease=%.1fs)",
        check_interval_seconds,
        lease_timeout_seconds,
    )
    while True:
        try:
            lease_mgr: Optional[LeaderLeaseManager] = getattr(app.state, "lease_manager", None)
            # Only authoritative leader executes stale job recovery and queue reconciliation
            if lease_mgr is not None and not lease_mgr.is_leader:
                await asyncio.sleep(check_interval_seconds)
                continue

            current_clock = lamport_clock.tick()
            queue: Optional[JobQueue] = getattr(app.state, "queue", None)
            leader_id = lease_mgr.leader_id if lease_mgr else SCHEDULER_ID
            leader_epoch = lease_mgr.current_epoch if lease_mgr else 0

            # 1. Recover stale running jobs whose leases expired or whose workers are offline
            try:
                recovered = find_and_recover_stale_jobs(
                    lease_timeout_seconds=lease_timeout_seconds,
                    lamport_ts=current_clock,
                    leader_id=leader_id,
                    leader_epoch=leader_epoch,
                )
            except StaleLeaderError as sle:
                logger.warning("Fencing violation during stale job recovery: %s. Stepping down.", sle)
                if lease_mgr:
                    lease_mgr.step_down()
                await asyncio.sleep(check_interval_seconds)
                continue

            if recovered and queue:
                # Pre-dispatch guard: check leadership lease was not lost during SQLite recovery
                if lease_mgr is not None and (not lease_mgr.is_leader or lease_mgr.current_epoch != leader_epoch):
                    logger.warning("Leadership lost or epoch changed before Redis recovery dispatch. Aborting.")
                    await asyncio.sleep(check_interval_seconds)
                    continue

                for item in recovered:
                    if lease_mgr is not None and (not lease_mgr.is_leader or lease_mgr.current_epoch != leader_epoch):
                        logger.warning("Leadership lost during Redis recovery dispatch. Halting remaining dispatches.")
                        break

                    job_id = item["job_id"]
                    action = item["action"]
                    if action == "requeued":
                        queue.requeue_job(job_id)
                        logger.warning(
                            "RECOVERY: Requeued abandoned job %s (attempt %d/%d, epoch %d)",
                            job_id,
                            item.get("retry_count", 1),
                            item.get("max_retries", 3),
                            leader_epoch,
                        )
                    elif action == "dead_letter":
                        queue.remove_from_processing(job_id)
                        logger.error(
                            "RECOVERY: Job %s permanently DEAD_LETTERED: %s",
                            job_id,
                            item.get("reason", "Max retries exceeded"),
                        )

            # 2. Reconcile Redis processing queue against durable SQLite records (Phase 2.1 & 3)
            if queue:
                if lease_mgr is not None and (not lease_mgr.is_leader or lease_mgr.current_epoch != leader_epoch):
                    await asyncio.sleep(check_interval_seconds)
                    continue

                processing_jobs = queue.get_processing_jobs()
                if processing_jobs:
                    try:
                        reconciled = reconcile_processing_queue(
                            processing_jobs,
                            lamport_ts=current_clock,
                            leader_id=leader_id,
                            leader_epoch=leader_epoch,
                        )
                    except StaleLeaderError as sle:
                        logger.warning("Fencing violation during queue reconciliation: %s. Stepping down.", sle)
                        if lease_mgr:
                            lease_mgr.step_down()
                        await asyncio.sleep(check_interval_seconds)
                        continue

                    for rec in reconciled:
                        if lease_mgr is not None and (not lease_mgr.is_leader or lease_mgr.current_epoch != leader_epoch):
                            logger.warning("Leadership lost during Redis queue reconciliation dispatch. Halting.")
                            break

                        jid = rec["job_id"]
                        act = rec["action"]
                        if act == "evict":
                            queue.remove_from_processing(jid)
                            logger.info(
                                "RECONCILIATION: Evicted stale job %s from processing queue (%s)",
                                jid,
                                rec.get("reason"),
                            )
                        elif act == "requeue":
                            queue.requeue_job(jid)
                            logger.warning(
                                "RECONCILIATION: Restored stranded pending job %s to pending queue (%s)",
                                jid,
                                rec.get("reason"),
                            )
        except asyncio.CancelledError:
            logger.info("Stale job recovery loop cancelled.")
            break
        except Exception as exc:
            logger.error("Error in stale job recovery loop: %s", exc)

        await asyncio.sleep(check_interval_seconds)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Manage the scheduler's startup and shutdown lifecycle."""
    # ---- Startup ----------------------------------------------------------
    init_db()
    app.state.queue = JobQueue()
    lease_mgr = LeaderLeaseManager(leader_id=SCHEDULER_ID)
    app.state.lease_manager = lease_mgr

    # Attempt initial lease acquisition non-blockingly
    try:
        await asyncio.to_thread(lease_mgr.try_acquire_lease)
    except Exception as exc:
        logger.warning("Initial leader lease acquisition failed: %s", exc)

    lease_task = asyncio.create_task(
        leader_lease_loop(app=app, interval_seconds=LEADER_RENEWAL_INTERVAL_SECONDS)
    )
    checker_task = asyncio.create_task(
        staleness_checker_loop(app=app, check_interval_seconds=STALENESS_CHECK_INTERVAL_SECONDS, timeout_seconds=WORKER_STALENESS_TIMEOUT_SECONDS)
    )
    recovery_task = asyncio.create_task(
        stale_job_recovery_loop(app=app, check_interval_seconds=STALE_JOB_CHECK_INTERVAL_SECONDS, lease_timeout_seconds=JOB_LEASE_TIMEOUT_SECONDS)
    )
    logger.info("Scheduler service started [ID=%s] — Redis queue, reliable recovery, leader leases online", SCHEDULER_ID)

    yield

    # ---- Shutdown ---------------------------------------------------------
    lease_task.cancel()
    checker_task.cancel()
    recovery_task.cancel()
    for t in (lease_task, checker_task, recovery_task):
        try:
            await t
        except asyncio.CancelledError:
            pass

    try:
        await asyncio.to_thread(lease_mgr.release_lease)
    except Exception as exc:
        logger.error("Error releasing leader lease during shutdown: %s", exc)

    logger.info("Scheduler service [ID=%s] shutting down", SCHEDULER_ID)


app = FastAPI(
    title="Scheduler Service",
    description=(
        "Central coordinator that accepts job submissions, persists job & worker "
        "metadata in SQLite, enqueues tasks into Redis, monitors worker liveness, and serves a live web dashboard."
    ),
    version="0.6.0",
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
    queue length, leader lease diagnostics, and recent job history in a single atomic response.
    """
    all_jobs = get_all_jobs()
    workers = get_all_workers()

    # Compute job counts by status (including Phase 2 dead_letter)
    job_counts = {"pending": 0, "running": 0, "completed": 0, "failed": 0, "dead_letter": 0}
    for job in all_jobs:
        status = job.get("status", "pending")
        if status in job_counts:
            job_counts[status] += 1
        elif status == "queued":
            job_counts["pending"] += 1

    total_workers = len(workers)
    online_workers = sum(1 for w in workers if w.get("status") == "online")

    queue_len = 0
    processing_len = 0
    try:
        queue: JobQueue = request.app.state.queue
        queue_len = queue.queue_length()
        processing_len = queue.processing_length()
    except Exception:
        queue_len = -1
        processing_len = -1

    lease_manager: Optional[LeaderLeaseManager] = getattr(request.app.state, "lease_manager", None)
    leader_info = lease_manager.get_leader_status() if lease_manager else {
        "is_leader": False,
        "current_leader": None,
        "epoch": 0,
        "lease_ttl_remaining_seconds": 0,
    }

    return {
        "job_counts": job_counts,
        "total_workers": total_workers,
        "online_workers": online_workers,
        "pending_queue_length": queue_len,
        "processing_queue_length": processing_len,
        "lamport_clock": lamport_clock.get_time(),
        "workers": workers,
        "recent_jobs": all_jobs[:DASHBOARD_RECENT_JOBS_LIMIT],
        "leader": leader_info,
    }


# ---------------------------------------------------------------------------
# Leader Lease & Election Endpoints (Unit III — Synchronization)
# ---------------------------------------------------------------------------
@app.get("/api/leader", summary="Leader Lease and Fencing Epoch status (Unit III)")
def get_leader_status(request: Request) -> dict:
    """Return distributed leader lease and fencing epoch status."""
    lease_manager: Optional[LeaderLeaseManager] = getattr(request.app.state, "lease_manager", None)
    if lease_manager:
        status = lease_manager.get_leader_status()
        durable = get_current_fencing_epoch()
        status["durable_fencing_epoch"] = durable["epoch"] if durable else None
        return status
    return {
        "is_leader": False,
        "current_leader": None,
        "epoch": 0,
        "status": "uninitialized",
        "local_instance_id": SCHEDULER_ID,
    }


@app.get("/api/elections", summary="Election algorithms and consensus diagnostics (Unit III)")
def get_election_info(request: Request) -> dict:
    """Return metadata and diagnostic status of election algorithms and mutual exclusion."""
    lease_manager: Optional[LeaderLeaseManager] = getattr(request.app.state, "lease_manager", None)
    leader_info = lease_manager.get_leader_status() if lease_manager else {}
    durable_epoch = get_current_fencing_epoch()
    return {
        "active_consensus": "Leader Lease with Monotonic Fencing Epochs (Redis Lua)",
        "current_leader": leader_info.get("current_leader"),
        "is_leader": leader_info.get("is_leader", False),
        "fencing_epoch": leader_info.get("epoch", 0),
        "durable_epoch": durable_epoch["epoch"] if durable_epoch else 0,
        "lease_ttl_remaining_seconds": leader_info.get("lease_ttl_remaining_seconds", 0),
        "algorithms": {
            "bully": {
                "name": "Garcia-Molina Bully Election",
                "syllabus_unit": "Unit III — Synchronization",
                "message_complexity": "O(N^2)",
                "assumption": "Synchronous/failure-detectable with unique priorities",
                "demo_script": "python -m tools.bully_election_demo",
            },
            "ring": {
                "name": "Chang & Roberts Logical Ring Election",
                "syllabus_unit": "Unit III — Synchronization",
                "message_complexity": "O(N) best case, O(N^2) worst case",
                "assumption": "Logical ring with dead successor bypass",
                "demo_script": "python -m tools.ring_election_demo",
            },
            "mutual_exclusion": {
                "name": "Lamport Distributed Mutual Exclusion",
                "syllabus_unit": "Unit III — Synchronization",
                "message_complexity": "3(N - 1) messages per CS entry",
                "ordering": "(Lamport timestamp, process_id) total order",
                "demo_script": "python -m tools.mutex_lamport_demo",
            },
        },
    }



# ---------------------------------------------------------------------------
# Clock Synchronization & Health Endpoints (Unit III)
# ---------------------------------------------------------------------------
@app.get("/api/time", summary="Physical Time Server endpoint for Cristian's Algorithm")
def get_server_time() -> dict:
    """Return coordinator physical timestamp for Cristian's clock synchronization (Unit III)."""
    now_ts = time.time()
    return {
        "server_time": now_ts,
        "iso": datetime.now(timezone.utc).isoformat(),
        "lamport_clock": lamport_clock.get_time(),
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
    # Advance coordinator logical clock on message ingestion
    logical_ts = lamport_clock.send_event()

    logger.info(
        "Received job submission | id=%s  type=%s  L#%d  payload_keys=%s",
        job_id,
        job.job_type,
        logical_ts,
        list(job.payload.keys()),
    )

    # 1. Persist job in SQLite with Lamport timestamp
    insert_job(
        job_id=job_id,
        job_type=job.job_type,
        payload=job.payload,
        submitted_at=submitted_at,
        lamport_ts=logical_ts,
    )

    # 2. Push job_id onto Redis queue
    try:
        queue: JobQueue = getattr(request.app.state, "queue", None)
        if queue is None:
            raise RuntimeError("Queue service is uninitialized on scheduler")
        queue.push_job(job_id)
    except Exception as exc:
        logger.error("Failed to enqueue job %s in Redis: %s", job_id, exc)
        try:
            update_job_status(
                job_id=job_id,
                status="failed",
                error_message=f"Queue dispatch failed: {exc}",
            )
        except Exception as db_exc:
            logger.error("Failed to update status for dropped job %s: %s", job_id, db_exc)

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
