"""
HTTP client helpers for the Scheduler service.

This module encapsulates every outbound HTTP call the scheduler makes to
downstream workers.  Isolating network I/O here keeps ``main.py`` focused
on request handling and makes it easy to:

  • Swap the transport later (e.g. switch from synchronous ``requests`` to
    an async ``httpx`` client, or to an AMQP producer when we add a message
    queue in Stage 2).
  • Mock the network layer cleanly in unit tests.
  • Centralise retry / timeout / circuit-breaker policies in one place.

WHY synchronous ``requests`` for Stage 1?
FastAPI supports async, but adding ``httpx`` + ``asyncio`` in the first
iteration adds complexity without teaching new distributed-systems concepts.
We deliberately keep it synchronous so the focus stays on RPC semantics.
The upgrade path to async is straightforward and planned for Stage 2.
"""

from typing import Any, Dict

import requests

from common.config import WORKER_HTTP_URL
from common.logging_config import setup_logger

logger = setup_logger("scheduler.client")

WORKER_BASE_URL: str = WORKER_HTTP_URL
REQUEST_TIMEOUT_SECONDS: int = 30  # generous for Stage 1's 2-second sleep


def forward_job_to_worker(job_payload: Dict[str, Any]) -> Dict[str, Any]:
    """Send a job to the worker's ``/execute_job`` endpoint via HTTP POST.

    Parameters
    ----------
    job_payload:
        A JSON-serialisable dictionary representing the job
        (typically ``JobRequest.model_dump()``).

    Returns
    -------
    dict
        The worker's JSON response on success.

    Raises
    ------
    requests.ConnectionError
        Re-raised so the caller (the endpoint handler) can translate it
        into an appropriate HTTP error response.
    requests.Timeout
        Raised if the worker doesn't respond within
        ``REQUEST_TIMEOUT_SECONDS``.
    requests.HTTPError
        Raised for non-2xx responses from the worker.
    """
    url = f"{WORKER_BASE_URL}/execute_job"
    logger.info("Forwarding job to worker at %s", url)

    response = requests.post(
        url,
        json=job_payload,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )

    # Raise on 4xx / 5xx so the scheduler can handle it uniformly.
    response.raise_for_status()

    logger.info("Worker responded with status %s", response.status_code)
    return response.json()
