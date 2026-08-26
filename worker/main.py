"""
Worker Service (Legacy HTTP Endpoint for Stage 1/2 RPC).

NOTE ON STAGE 3 ARCHITECTURE:
-----------------------------
In Stage 1 and 2, this FastAPI HTTP server received push-based RPC requests
via `POST /execute_job`.

In Stage 3+, the platform shifted to an event-driven, pull-based queue model.
Active worker processing is now handled by standalone consumer processes:
    python -m worker.consumer

This file is preserved for reference, backwards-compatibility testing, and
direct HTTP health probing.
"""

import time
from uuid import uuid4

from fastapi import FastAPI

from common.logging_config import setup_logger
from scheduler.models import JobRequest
from worker.models import WorkerResponse

# ---------------------------------------------------------------------------
# Application bootstrap
# ---------------------------------------------------------------------------
logger = setup_logger("worker_http_legacy")

WORKER_ID: str = f"worker-http-{uuid4().hex[:8]}"

app = FastAPI(
    title="Worker Service (Legacy HTTP)",
    description="Legacy push-based HTTP RPC worker server for Stage 1/2 reference.",
    version="0.2.0",
)


@app.post(
    "/execute_job",
    response_model=WorkerResponse,
    summary="Execute a submitted job (Stage 1/2 legacy RPC endpoint)",
)
def execute_job(job: JobRequest) -> WorkerResponse:
    """Legacy push endpoint for synchronous RPC."""
    logger.info(
        "Received HTTP job | type=%s  payload_keys=%s",
        job.job_type,
        list(job.payload.keys()),
    )
    simulated_duration: float = 2.0
    time.sleep(simulated_duration)

    return WorkerResponse(
        status="completed",
        worker_id=WORKER_ID,
        execution_time_seconds=simulated_duration,
    )


@app.get("/health", summary="Health check")
def health_check() -> dict:
    """Liveness probe for legacy HTTP worker."""
    return {
        "service": "worker_http",
        "status": "healthy",
        "worker_id": WORKER_ID,
    }
