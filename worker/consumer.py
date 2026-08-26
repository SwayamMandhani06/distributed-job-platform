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
import threading
import time
from uuid import uuid4

from common.config import (
    HEARTBEAT_INTERVAL_SECONDS,
    SIMULATED_JOB_DURATION_SECONDS,
)
from common.logging_config import setup_logger
from common.database import (
    register_worker,
    update_heartbeat,
    increment_jobs_completed,
    get_job,
    update_job_status,
)
from common.queue_client import JobQueue

# ---------------------------------------------------------------------------
# Setup logging & identity
# ---------------------------------------------------------------------------
WORKER_ID: str = f"worker-{uuid4().hex[:8]}"
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
    """Simulate task execution based on job type."""
    simulated_duration: float = SIMULATED_JOB_DURATION_SECONDS
    logger.info("Executing job_type='%s' (simulated %.1fs sleep)...", job_type, simulated_duration)
    time.sleep(simulated_duration)

    if job_type == "email":
        message = f"Email sent successfully to {payload.get('to', 'unknown')}"
    elif job_type == "csv_processing":
        message = f"CSV processed at {payload.get('file_path', 'unknown')} with ops {payload.get('operations', [])}"
    elif job_type == "thumbnail":
        message = f"Thumbnails generated for {payload.get('image_url', 'unknown')}"
    else:
        message = f"Generic job '{job_type}' completed successfully"

    return {
        "status": "completed",
        "worker_id": WORKER_ID,
        "execution_time_seconds": simulated_duration,
        "details": message,
    }


def start_consumer() -> None:
    """Run worker registration, start heartbeat thread, and enter consumer event loop."""
    logger.info("=" * 60)
    logger.info("Starting Worker Consumer [%s]", WORKER_ID)
    logger.info("Listening for jobs on Redis 'job_queue'...")
    logger.info("=" * 60)

    # 1. Register worker in database
    register_worker(WORKER_ID)

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
    idle_count = 0

    try:
        while True:
            # Block for up to 5 seconds waiting for a job_id from Redis
            job_id = queue.pop_job(timeout=5)

            if job_id is None:
                idle_count += 1
                if idle_count % 6 == 0:
                    logger.debug("No jobs available in queue. Waiting...")
                continue

            idle_count = 0
            logger.info("Popped job_id=%s from queue", job_id)

            # Fetch job state from SQLite
            job = get_job(job_id)
            if not job:
                logger.error("Job %s popped from queue but not found in SQLite!", job_id)
                continue

            # Update state to 'running'
            update_job_status(job_id, "running", worker_id=WORKER_ID)
            logger.info("Job %s status updated to 'running'", job_id)

            # Parse payload
            payload = {}
            if job.get("payload"):
                try:
                    payload = json.loads(job["payload"])
                except Exception:
                    payload = {}

            # Execute task safely
            try:
                result_data = execute_task(job["job_type"], payload)
                update_job_status(
                    job_id,
                    "completed",
                    worker_id=WORKER_ID,
                    result=json.dumps(result_data),
                )
                increment_jobs_completed(WORKER_ID)
                logger.info("Job %s successfully COMPLETED by worker %s", job_id, WORKER_ID)

            except Exception as task_exc:
                logger.error("Execution failed for job %s: %s", job_id, task_exc, exc_info=True)
                update_job_status(
                    job_id,
                    "failed",
                    worker_id=WORKER_ID,
                    error_message=str(task_exc),
                )

    except KeyboardInterrupt:
        logger.info("Worker consumer [%s] shutting down gracefully...", WORKER_ID)
    finally:
        stop_event.set()
        hb_thread.join(timeout=2.0)
        logger.info("Worker consumer [%s] stopped.", WORKER_ID)


if __name__ == "__main__":
    start_consumer()
