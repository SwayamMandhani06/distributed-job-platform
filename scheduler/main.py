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
from fastapi.responses import HTMLResponse, PlainTextResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from common.clocks import (
    LamportClock,
    VectorClock,
    record_lamport_event,
    get_lamport_traces,
)
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
from common.logging_config import setup_logger, set_trace_context, clear_trace_context, correlation_id_ctx
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

# Coordinator Logical Clocks (Unit III — Synchronization)
lamport_clock = LamportClock()
coordinator_vector_clock = VectorClock("scheduler", ["scheduler"])


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

            # Record background recovery sweep run
            app.state.reconciliation_runs_total = getattr(app.state, "reconciliation_runs_total", 0) + 1

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
                        app.state.reconciliation_reclaimed_jobs_total = getattr(app.state, "reconciliation_reclaimed_jobs_total", 0) + 1
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
                            app.state.reconciliation_evicted_jobs_total = getattr(app.state, "reconciliation_evicted_jobs_total", 0) + 1
                            logger.info(
                                "RECONCILIATION: Evicted stale job %s from processing queue (%s)",
                                jid,
                                rec.get("reason"),
                            )
                        elif act == "requeue":
                            queue.requeue_job(jid)
                            app.state.reconciliation_reclaimed_jobs_total = getattr(app.state, "reconciliation_reclaimed_jobs_total", 0) + 1
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
    app.state.submissions_count = {}
    app.state.reconciliation_runs_total = 0
    app.state.reconciliation_reclaimed_jobs_total = 0
    app.state.reconciliation_evicted_jobs_total = 0

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
    version="0.7.0",
    lifespan=lifespan,
)


@app.middleware("http")
async def security_and_tracing_middleware(request: Request, call_next):
    """Enforce request size limits and inject distributed correlation tracing IDs."""
    # 1. Enforce payload size limit (1 MB max)
    content_length = request.headers.get("content-length")
    if content_length:
        try:
            if int(content_length) > 1_048_576:
                return JSONResponse(
                    status_code=413,
                    content={"detail": "Payload Too Large: Maximum allowable request size is 1MB."},
                )
        except ValueError:
            pass

    # 2. Extract or generate distributed correlation ID
    correlation_id = (
        request.headers.get("X-Correlation-ID")
        or request.headers.get("X-Request-ID")
        or f"corr-{uuid4().hex[:8]}"
    )
    set_trace_context(correlation_id=correlation_id)

    response = await call_next(request)
    response.headers["X-Correlation-ID"] = correlation_id
    clear_trace_context()
    return response


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

    # Authoritative worker liveness verification using server-side timestamps
    now_dt = datetime.now(timezone.utc)
    live_workers_count = 0
    for w in workers:
        hb_str = w.get("last_heartbeat")
        try:
            hb_dt = datetime.fromisoformat(hb_str)
            age = (now_dt - hb_dt).total_seconds()
            is_live = (w.get("status") == "online") and (age <= WORKER_STALENESS_TIMEOUT_SECONDS)
        except Exception:
            age = 999999.0
            is_live = False
        w["heartbeat_age_seconds"] = round(age, 1)
        w["is_live"] = is_live
        if is_live:
            live_workers_count += 1
        elif w.get("status") == "online":
            # Observable transition to stale before background recovery sweep
            w["status"] = "stale"

    total_workers = len(workers)
    online_workers = live_workers_count

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

    queue_diagnostics = {
        "redis_pending_depth": queue_len,
        "redis_processing_depth": processing_len,
        "db_pending_count": job_counts["pending"],
        "db_running_count": job_counts["running"],
        "is_synchronized": (queue_len == job_counts["pending"]),
        "discrepancy_explanation": (
            "All broker queue entries match database pending records."
            if queue_len == job_counts["pending"]
            else f"Discrepancy: {queue_len} jobs queued in Redis broker vs {job_counts['pending']} pending in SQLite database (due to in-flight worker claims, pending recovery sweeps, or orphaned test entries)."
        ),
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
        "queue_diagnostics": queue_diagnostics,
    }


@app.post("/api/cleanup_test_records", summary="Administrative endpoint to clean synthetic test records")
def cleanup_test_records() -> dict:
    """Purge synthetic test jobs and test workers from SQLite persistence."""
    from common.database import clean_test_artifacts
    cleaned = clean_test_artifacts()
    return {"status": "ok", "cleaned": cleaned}


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
    recv_ts = time.time()
    send_ts = time.time()
    return {
        "server_time": send_ts,
        "server_receive_time": recv_ts,
        "server_send_time": send_ts,
        "iso": datetime.now(timezone.utc).isoformat(),
        "lamport_clock": lamport_clock.get_time(),
    }


@app.get("/api/clocks/lamport_trace", summary="Lamport Logical Clock Event Traces (Unit III)")
def get_lamport_event_traces(job_id: Optional[str] = None, limit: int = 100) -> dict:
    """Return historical Lamport clock trace events demonstrating causal message ordering."""
    traces = get_lamport_traces(job_id=job_id, limit=limit)
    return {
        "current_coordinator_lamport_ts": lamport_clock.get_time(),
        "total_traces_returned": len(traces),
        "traces": traces,
        "causal_ordering_guarantee": "e1 -> e2 implies L(e1) < L(e2)",
    }


@app.post("/api/clocks/vector/exchange", summary="Exchange and merge vector clocks between independent processes (Unit III)")
def exchange_vector_clock(body: dict) -> dict:
    """Exchange vector timestamps with independent process and return updated vector state."""
    sender_id = body.get("sender_id", "unknown")
    incoming_vector = body.get("vector", {})
    if not isinstance(incoming_vector, dict):
        raise HTTPException(status_code=400, detail="Field 'vector' must be a dict")

    # Explicitly register sender in membership if not already known
    coordinator_vector_clock.add_member(sender_id)

    # Rule 3: merge incoming vector and increment coordinator local entry
    updated = coordinator_vector_clock.receive_event(incoming_vector)
    return {
        "coordinator_id": "scheduler",
        "updated_vector": updated,
        "members": coordinator_vector_clock.get_members(),
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

    correlation_id = (
        correlation_id_ctx.get()
        or request.headers.get("X-Correlation-ID")
        or request.headers.get("X-Request-ID")
        or f"corr-{uuid4().hex[:8]}"
    )
    set_trace_context(correlation_id=correlation_id, job_id=job_id)

    # Track submissions by job type
    submissions = getattr(request.app.state, "submissions_count", {})
    submissions[job.job_type] = submissions.get(job.job_type, 0) + 1
    request.app.state.submissions_count = submissions

    # Record trace event for causal verification
    record_lamport_event(
        process_id="scheduler",
        event_type="job_submitted",
        lamport_ts=logical_ts,
        job_id=job_id,
        details={"job_type": job.job_type},
    )

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

    # 3. Return 202 Accepted with status 'queued' and correlation_id
    return JobResponse(
        job_id=job_id,
        status="queued",
        message="Job successfully accepted and enqueued into Redis message queue.",
        correlation_id=correlation_id,
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


@app.get("/metrics", summary="Prometheus Metrics Exposition", response_class=PlainTextResponse)
def prometheus_metrics(request: Request) -> PlainTextResponse:
    """Expose Prometheus formatted metrics for scrapers (Phases 2 & 7).

    Provides comprehensive gauges, counters, and summaries:
      - job_platform_jobs_total (by status: pending, running, completed, failed, dead_letter)
      - job_platform_job_submissions_total (by job_type)
      - job_platform_jobs_by_type (by job_type, status)
      - job_platform_queue_depth (by queue: pending, processing)
      - job_platform_job_execution_duration_seconds (summary: sum, count, max by job_type)
      - job_platform_queue_wait_duration_seconds (summary: sum, count, max by job_type)
      - job_platform_job_retries_total
      - job_platform_dead_letter_jobs_total
      - job_platform_workers (by status: online, stale, offline, total)
      - job_platform_worker_heartbeat_age_seconds (by worker_id)
      - job_platform_worker_max_heartbeat_age_seconds
      - job_platform_leader_epoch
      - job_platform_is_leader
      - job_platform_leader_lease_ttl_seconds
      - job_platform_dependency_up (by dependency: redis, database)
      - job_platform_dependency_latency_seconds (by dependency: redis, database)
      - job_platform_reconciliation_runs_total
      - job_platform_reconciliation_reclaimed_jobs_total
      - job_platform_reconciliation_evicted_jobs_total
      - job_platform_rpc_operations_total (by service, status)
      - job_platform_dfs_operations_total (by operation, status)
    """
    all_jobs = get_all_jobs()
    workers = get_all_workers()

    counts = {"pending": 0, "running": 0, "completed": 0, "failed": 0, "dead_letter": 0}
    by_type_counts: Dict[str, Dict[str, int]] = {}

    exec_sums: Dict[str, float] = {}
    exec_counts: Dict[str, int] = {}
    exec_maxs: Dict[str, float] = {}

    wait_sums: Dict[str, float] = {}
    wait_counts: Dict[str, int] = {}
    wait_maxs: Dict[str, float] = {}

    total_retries = 0

    for job in all_jobs:
        st = job.get("status", "pending")
        if st == "queued":
            st = "pending"
        if st in counts:
            counts[st] += 1

        jt = job.get("job_type", "unknown")
        if jt not in by_type_counts:
            by_type_counts[jt] = {"pending": 0, "running": 0, "completed": 0, "failed": 0, "dead_letter": 0}
        if st in by_type_counts[jt]:
            by_type_counts[jt][st] += 1

        total_retries += int(job.get("retry_count", 0))

        sub_str = job.get("submitted_at")
        start_str = job.get("started_at")
        comp_str = job.get("completed_at")

        if sub_str and start_str:
            try:
                sub_dt = datetime.fromisoformat(sub_str)
                start_dt = datetime.fromisoformat(start_str)
                wait_sec = max(0.0, (start_dt - sub_dt).total_seconds())
                wait_sums[jt] = wait_sums.get(jt, 0.0) + wait_sec
                wait_counts[jt] = wait_counts.get(jt, 0) + 1
                wait_maxs[jt] = max(wait_maxs.get(jt, 0.0), wait_sec)
            except Exception:
                pass

        if start_str and comp_str and st == "completed":
            try:
                start_dt = datetime.fromisoformat(start_str)
                comp_dt = datetime.fromisoformat(comp_str)
                exec_sec = max(0.0, (comp_dt - start_dt).total_seconds())
                exec_sums[jt] = exec_sums.get(jt, 0.0) + exec_sec
                exec_counts[jt] = exec_counts.get(jt, 0) + 1
                exec_maxs[jt] = max(exec_maxs.get(jt, 0.0), exec_sec)
            except Exception:
                pass

    now_dt = datetime.now(timezone.utc)
    online_count = 0
    stale_count = 0
    offline_count = 0
    worker_heartbeat_ages: Dict[str, float] = {}
    max_hb_age = 0.0

    for w in workers:
        wid = w.get("worker_id", "unknown")
        hb_str = w.get("last_heartbeat")
        age = 999999.0
        try:
            if hb_str:
                hb_dt = datetime.fromisoformat(hb_str)
                age = max(0.0, (now_dt - hb_dt).total_seconds())
                worker_heartbeat_ages[wid] = round(age, 2)
            if w.get("status") == "online" and age <= WORKER_STALENESS_TIMEOUT_SECONDS:
                online_count += 1
                max_hb_age = max(max_hb_age, age)
            elif w.get("status") == "online":
                stale_count += 1
                max_hb_age = max(max_hb_age, age)
            else:
                offline_count += 1
        except Exception:
            offline_count += 1

    queue_len = 0
    proc_len = 0
    q: Optional[JobQueue] = getattr(request.app.state, "queue", None)
    if q:
        try:
            queue_len = q.queue_length()
            proc_len = q.processing_length()
        except Exception:
            queue_len = -1
            proc_len = -1

    lease_mgr: Optional[LeaderLeaseManager] = getattr(request.app.state, "lease_manager", None)
    is_leader = 1 if (lease_mgr and lease_mgr.is_leader) else 0
    epoch = lease_mgr.current_epoch if lease_mgr else 0
    lease_ttl = max(0.0, lease_mgr.lease_ttl_seconds) if (lease_mgr and lease_mgr.is_leader) else 0.0

    # Dependencies health check
    redis_up = 0
    redis_lat = 0.0
    try:
        if q and q.redis_client:
            t0 = time.perf_counter()
            if q.redis_client.ping():
                redis_up = 1
                redis_lat = round(time.perf_counter() - t0, 5)
    except Exception:
        redis_up = 0

    db_up = 0
    db_lat = 0.0
    try:
        from common.database import _get_connection
        t0 = time.perf_counter()
        conn = _get_connection()
        conn.execute("SELECT 1")
        conn.close()
        db_up = 1
        db_lat = round(time.perf_counter() - t0, 5)
    except Exception:
        db_up = 0

    submissions_map: Dict[str, int] = getattr(request.app.state, "submissions_count", {})
    rec_runs = getattr(request.app.state, "reconciliation_runs_total", 0)
    rec_reclaimed = getattr(request.app.state, "reconciliation_reclaimed_jobs_total", 0)
    rec_evicted = getattr(request.app.state, "reconciliation_evicted_jobs_total", 0)

    lines = [
        "# HELP job_platform_jobs_total Authoritative persisted job counts partitioned by status.",
        "# TYPE job_platform_jobs_total gauge",
        f'job_platform_jobs_total{{status="pending"}} {counts["pending"]}',
        f'job_platform_jobs_total{{status="running"}} {counts["running"]}',
        f'job_platform_jobs_total{{status="completed"}} {counts["completed"]}',
        f'job_platform_jobs_total{{status="failed"}} {counts["failed"]}',
        f'job_platform_jobs_total{{status="dead_letter"}} {counts["dead_letter"]}',
        "",
        "# HELP job_platform_queue_depth Live Redis queue backlog depths.",
        "# TYPE job_platform_queue_depth gauge",
        f'job_platform_queue_depth{{queue="pending"}} {queue_len}',
        f'job_platform_queue_depth{{queue="processing"}} {proc_len}',
        "",
        "# HELP job_platform_workers Worker process count by operational health status.",
        "# TYPE job_platform_workers gauge",
        f'job_platform_workers{{status="online"}} {online_count}',
        f'job_platform_workers{{status="stale"}} {stale_count}',
        f'job_platform_workers{{status="offline"}} {offline_count}',
        f'job_platform_workers{{status="total"}} {len(workers)}',
        "",
        "# HELP job_platform_leader_epoch Monotonic leader fencing epoch.",
        "# TYPE job_platform_leader_epoch gauge",
        f"job_platform_leader_epoch {epoch}",
        "",
        "# HELP job_platform_is_leader Indicator of whether this instance holds active leader lease (1=leader, 0=standby).",
        "# TYPE job_platform_is_leader gauge",
        f"job_platform_is_leader {is_leader}",
        "",
        "# HELP job_platform_leader_lease_ttl_seconds Remaining TTL in seconds for active leader lease.",
        "# TYPE job_platform_leader_lease_ttl_seconds gauge",
        f"job_platform_leader_lease_ttl_seconds {lease_ttl:.1f}",
        "",
        "# HELP job_platform_dependency_up Dependency operational availability (1=UP, 0=DOWN).",
        "# TYPE job_platform_dependency_up gauge",
        f'job_platform_dependency_up{{dependency="redis"}} {redis_up}',
        f'job_platform_dependency_up{{dependency="database"}} {db_up}',
        "",
        "# HELP job_platform_dependency_latency_seconds Dependency roundtrip response time in seconds.",
        "# TYPE job_platform_dependency_latency_seconds gauge",
        f'job_platform_dependency_latency_seconds{{dependency="redis"}} {redis_lat:.5f}',
        f'job_platform_dependency_latency_seconds{{dependency="database"}} {db_lat:.5f}',
        "",
        "# HELP job_platform_job_retries_total Cumulative execution retry attempts across all jobs.",
        "# TYPE job_platform_job_retries_total counter",
        f"job_platform_job_retries_total {total_retries}",
        "",
        "# HELP job_platform_dead_letter_jobs_total Total jobs transitioned into terminal dead-letter queue.",
        "# TYPE job_platform_dead_letter_jobs_total gauge",
        f"job_platform_dead_letter_jobs_total {counts['dead_letter']}",
        "",
        "# HELP job_platform_worker_max_heartbeat_age_seconds Maximum heartbeat age across active registered workers in seconds.",
        "# TYPE job_platform_worker_max_heartbeat_age_seconds gauge",
        f"job_platform_worker_max_heartbeat_age_seconds {round(max_hb_age, 2)}",
        "",
        "# HELP job_platform_reconciliation_runs_total Background stale job recovery sweeps executed.",
        "# TYPE job_platform_reconciliation_runs_total counter",
        f"job_platform_reconciliation_runs_total {rec_runs}",
        "",
        "# HELP job_platform_reconciliation_reclaimed_jobs_total Abandoned jobs recovered and requeued.",
        "# TYPE job_platform_reconciliation_reclaimed_jobs_total counter",
        f"job_platform_reconciliation_reclaimed_jobs_total {rec_reclaimed}",
        "",
        "# HELP job_platform_reconciliation_evicted_jobs_total Stale unpersisted jobs evicted from processing queue.",
        "# TYPE job_platform_reconciliation_evicted_jobs_total counter",
        f"job_platform_reconciliation_evicted_jobs_total {rec_evicted}",
        "",
        "# HELP job_platform_rpc_operations_total Distributed RPC invocations by service and outcome.",
        "# TYPE job_platform_rpc_operations_total counter",
        'job_platform_rpc_operations_total{service="distributed_object",status="success"} 0',
        'job_platform_rpc_operations_total{service="distributed_object",status="error"} 0',
        "",
        "# HELP job_platform_dfs_operations_total Distributed File System operations by operation and outcome.",
        "# TYPE job_platform_dfs_operations_total counter",
        'job_platform_dfs_operations_total{operation="read",status="success"} 0',
        'job_platform_dfs_operations_total{operation="write",status="success"} 0',
        'job_platform_dfs_operations_total{operation="re_replicate",status="success"} 0',
    ]

    # Worker heartbeat age by worker_id (bounded by registered workers)
    if worker_heartbeat_ages:
        lines.append("")
        lines.append("# HELP job_platform_worker_heartbeat_age_seconds Seconds elapsed since last heartbeat received from worker.")
        lines.append("# TYPE job_platform_worker_heartbeat_age_seconds gauge")
        for wid, age_val in sorted(worker_heartbeat_ages.items()):
            lines.append(f'job_platform_worker_heartbeat_age_seconds{{worker_id="{wid}"}} {age_val}')

    # Job submissions by type
    if submissions_map:
        lines.append("")
        lines.append("# HELP job_platform_job_submissions_total Cumulative job submissions partitioned by job type.")
        lines.append("# TYPE job_platform_job_submissions_total counter")
        for jt, scnt in sorted(submissions_map.items()):
            lines.append(f'job_platform_job_submissions_total{{job_type="{jt}"}} {scnt}')

    # Jobs partitioned by type and status
    if by_type_counts:
        lines.append("")
        lines.append("# HELP job_platform_jobs_by_type Authoritative jobs partitioned by type and status.")
        lines.append("# TYPE job_platform_jobs_by_type gauge")
        for jt, st_map in sorted(by_type_counts.items()):
            for s_name, s_val in sorted(st_map.items()):
                lines.append(f'job_platform_jobs_by_type{{job_type="{jt}",status="{s_name}"}} {s_val}')

    # Execution duration summaries
    if exec_counts:
        lines.append("")
        lines.append("# HELP job_platform_job_execution_duration_seconds Execution duration of completed jobs by type.")
        lines.append("# TYPE job_platform_job_execution_duration_seconds summary")
        for jt in sorted(exec_counts.keys()):
            lines.append(f'job_platform_job_execution_duration_seconds_sum{{job_type="{jt}"}} {exec_sums[jt]:.4f}')
            lines.append(f'job_platform_job_execution_duration_seconds_count{{job_type="{jt}"}} {exec_counts[jt]}')
            lines.append(f'job_platform_job_execution_duration_seconds_max{{job_type="{jt}"}} {exec_maxs[jt]:.4f}')

    # Queue wait duration summaries
    if wait_counts:
        lines.append("")
        lines.append("# HELP job_platform_queue_wait_duration_seconds Duration jobs spent waiting in queue prior to execution.")
        lines.append("# TYPE job_platform_queue_wait_duration_seconds summary")
        for jt in sorted(wait_counts.keys()):
            lines.append(f'job_platform_queue_wait_duration_seconds_sum{{job_type="{jt}"}} {wait_sums[jt]:.4f}')
            lines.append(f'job_platform_queue_wait_duration_seconds_count{{job_type="{jt}"}} {wait_counts[jt]}')
            lines.append(f'job_platform_queue_wait_duration_seconds_max{{job_type="{jt}"}} {wait_maxs[jt]:.4f}')

    lines.append("")
    return PlainTextResponse("\n".join(lines), media_type="text/plain; version=0.0.4; charset=utf-8")
