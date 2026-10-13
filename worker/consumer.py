"""
Worker Consumer — background task worker process for the distributed job platform.

Architecture role
-----------------
This process represents an autonomous **Compute Worker** in a pull-based
Producer-Consumer architecture with **Heartbeat Liveness Monitoring**.

Stage 4 Additions:
------------------
  1. **Worker Registration**: On startup, calls `register_worker(worker_id)` to initialize its entry in SQLite.
  2. **Background Heartbeat Thread**: Spawns a dedicated `threading.Thread` (daemon) that calls `update_heartbeat(worker_id)` every 5 seconds.
     - Running the heartbeat on a separate thread guarantees that liveness updates continue EVEN IF the worker is busy executing long-running CPU/IO tasks.
  3. **Metrics Tracking**: Calls `increment_jobs_completed(worker_id)` after every successful job.
"""

import json
import os
import random
import threading
import time
from datetime import datetime, timezone
from uuid import uuid4

from common.clocks import LamportClock, record_lamport_event
from common.config import (
    HEARTBEAT_INTERVAL_SECONDS,
    JOB_LEASE_TIMEOUT_SECONDS,
)
from common.logging_config import setup_logger, set_trace_context, clear_trace_context
from common.database import (
    register_worker,
    update_heartbeat,
    increment_jobs_completed,
    get_job,
    update_job_status,
    claim_job_for_worker,
)
from common.queue_client import JobQueue
from worker.handlers import dispatch_task

# ---------------------------------------------------------------------------
# Setup logging & identity
# ---------------------------------------------------------------------------
WORKER_ID: str = os.getenv("WORKER_ID") or f"worker-{uuid4().hex[:8]}"
logger = setup_logger(WORKER_ID)


def heartbeat_loop(stop_event: threading.Event) -> None:
    """Background thread loop that periodically updates this worker's last_heartbeat timestamp.

    Running this loop in a background daemon thread prevents task execution
    (e.g., a long-running CPU/IO operation) from blocking heartbeat delivery.
    """
    logger.info("Heartbeat thread started for %s (interval=%.1fs)", WORKER_ID, HEARTBEAT_INTERVAL_SECONDS)
    while not stop_event.is_set():
        try:
            update_heartbeat(WORKER_ID)
            logger.debug("Sent heartbeat for %s", WORKER_ID)
        except Exception as exc:
            logger.error("Error updating heartbeat for %s: %s", WORKER_ID, exc)
        stop_event.wait(HEARTBEAT_INTERVAL_SECONDS)


def execute_task(job_type: str, payload: dict) -> dict:
    """Execute a task deterministically using registered job handlers."""
    logger.info("Executing job_type='%s' with registered handler...", job_type)
    return dispatch_task(job_type, payload)


def start_consumer() -> None:
    """Run worker registration, start heartbeat thread, and enter consumer event loop."""
    logger.info("=" * 60)
    logger.info("Starting Worker Consumer [%s]", WORKER_ID)
    logger.info("Listening for jobs on Redis 'job_queue'...")
    logger.info("=" * 60)

    # 1. Register worker in database with OS process ID and startup timestamp
    register_worker(
        WORKER_ID,
        pid=os.getpid(),
        startup_time=datetime.now(timezone.utc).isoformat(),
    )

    # 2. Start background heartbeat thread
    stop_event = threading.Event()
    hb_thread = threading.Thread(
        target=heartbeat_loop,
        args=(stop_event,),
        daemon=True,
        name=f"heartbeat-{WORKER_ID}",
    )
    hb_thread.start()

    queue = JobQueue()
    worker_clock = LamportClock()
    idle_count = 0

    try:
        while True:
            # Reliably claim job from Redis: moves atomically from job_queue to job_queue:processing
            job_id = queue.claim_job(timeout=5)

            if job_id is None:
                idle_count += 1
                if idle_count % 6 == 0:
                    logger.debug("No jobs available in queue. Waiting...")
                continue

            idle_count = 0
            # Set contextual correlation and job IDs for structured distributed tracing
            set_trace_context(correlation_id=f"corr-{job_id[:8]}", job_id=job_id)
            logger.info("Claimed job_id=%s into processing list", job_id)

            # Atomically claim job in SQLite with lease and Lamport clock update
            claimed_job = claim_job_for_worker(
                job_id=job_id,
                worker_id=WORKER_ID,
                lease_seconds=JOB_LEASE_TIMEOUT_SECONDS,
                lamport_ts=worker_clock.get_time(),
            )
            if not claimed_job:
                logger.warning(
                    "Job %s could not be claimed in database (already running with active lease or completed)",
                    job_id,
                )
                # Check DB status before evicting: only remove if already terminal
                from common.database import get_job
                existing = get_job(job_id)
                if existing and existing.get("status") in ("completed", "failed", "dead_letter"):
                    queue.remove_from_processing(job_id)
                clear_trace_context()
                continue

            claim_token = claimed_job.get("claim_token")
            # Register active claim token with Redis queue client
            if claim_token:
                queue.register_claim(job_id, claim_token, lease_seconds=JOB_LEASE_TIMEOUT_SECONDS)

            # Update worker logical clock: L = max(L, incoming_ts) + 1
            worker_clock.receive_event(claimed_job.get("lamport_ts", 0))
            record_lamport_event(
                process_id=WORKER_ID,
                event_type="job_claimed",
                lamport_ts=worker_clock.get_time(),
                job_id=job_id,
            )
            logger.info(
                "Job %s running | attempt=%d | Lamport L#%d",
                job_id,
                claimed_job.get("retry_count", 1),
                worker_clock.get_time(),
            )

            # Parse payload
            payload = {}
            if claimed_job.get("payload"):
                try:
                    payload = json.loads(claimed_job["payload"])
                except Exception:
                    payload = {}

            # Execute task safely with real duration tracking
            start_time = time.perf_counter()
            try:
                task_output = execute_task(claimed_job["job_type"], payload)
                elapsed_duration = round(time.perf_counter() - start_time, 4)
                comp_ts = worker_clock.send_event()
                record_lamport_event(
                    process_id=WORKER_ID,
                    event_type="job_completed",
                    lamport_ts=comp_ts,
                    job_id=job_id,
                    details={"duration": elapsed_duration},
                )
                result_data = {
                    "status": "completed",
                    "worker_id": WORKER_ID,
                    "execution_time_seconds": elapsed_duration,
                    "lamport_ts": comp_ts,
                    "output": task_output,
                    "details": f"Job '{claimed_job['job_type']}' completed successfully",
                }

                # 1. First durably persist completion to SQLite with claim fencing
                persisted = update_job_status(
                    job_id,
                    "completed",
                    worker_id=WORKER_ID,
                    result=json.dumps(result_data),
                    lamport_ts=comp_ts,
                    claim_token=claim_token,
                )

                if persisted:
                    increment_jobs_completed(WORKER_ID)
                    # 2. Only after durable fenced persistence succeeds, acknowledge in queue
                    queue.ack_job(job_id, claim_token=claim_token)
                    logger.info(
                        "Job %s COMPLETED and ACKed by %s in %.4fs (L#%d)",
                        job_id,
                        WORKER_ID,
                        elapsed_duration,
                        comp_ts,
                    )
                else:
                    logger.warning(
                        "Job %s completion REJECTED: worker %s lost claim (lease expired or reclaimed)",
                        job_id,
                        WORKER_ID,
                    )

            except Exception as task_exc:
                elapsed_duration = round(time.perf_counter() - start_time, 4)
                fail_ts = worker_clock.send_event()
                record_lamport_event(
                    process_id=WORKER_ID,
                    event_type="job_failed",
                    lamport_ts=fail_ts,
                    job_id=job_id,
                    details={"error": str(task_exc), "duration": elapsed_duration},
                )
                logger.error(
                    "Execution failed for job %s after %.4fs: %s",
                    job_id,
                    elapsed_duration,
                    task_exc,
                    exc_info=True,
                )
                persisted = update_job_status(
                    job_id,
                    "failed",
                    worker_id=WORKER_ID,
                    error_message=str(task_exc),
                    lamport_ts=fail_ts,
                    claim_token=claim_token,
                )
                if persisted:
                    queue.ack_job(job_id, claim_token=claim_token)
                else:
                    logger.warning(
                        "Job %s failure update REJECTED: worker %s lost claim",
                        job_id,
                        WORKER_ID,
                    )
            finally:
                clear_trace_context()

    except KeyboardInterrupt:
        logger.info("Worker consumer [%s] shutting down gracefully...", WORKER_ID)
    finally:
        stop_event.set()
        hb_thread.join(timeout=2.0)
        logger.info("Worker consumer [%s] stopped.", WORKER_ID)


if __name__ == "__main__":
    start_consumer()
