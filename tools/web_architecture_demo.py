"""
Unit IV Demonstration: Web-Based Distributed Systems Architecture.

Demonstrates:
1. Client-to-API HTTP/REST communication using the FastAPI Scheduler.
2. Decoupling: Asynchronous Job Processing via Message Queue (Redis) and Workers.
3. End-to-end request flow: Submit (202 Accepted) -> Enqueue -> Claim -> Execute -> Ack.
4. Distributed state observability (/health, /api/dashboard_data, /job/{id}).
5. Failure modes, isolation boundaries, and retry semantics.

Usage:
    python -m tools.web_architecture_demo
"""

import json
import logging
from pathlib import Path
import sys
import time
from unittest.mock import MagicMock, patch

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from fastapi.testclient import TestClient

from common.database import (
    claim_job_for_worker,
    get_job,
    init_db,
    register_worker,
    update_job_status,
)
from scheduler.main import app
from worker.consumer import execute_task

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("web_demo")


def run_demo() -> None:
    print("\n" + "=" * 78)
    print(" UNIT IV: WEB-BASED DISTRIBUTED SYSTEMS ARCHITECTURE DEMONSTRATION")
    print("=" * 78)
    print("Component:   FastAPI Web Scheduler, Redis Message Broker, Asynchronous Workers")
    print("Paradigms:   HTTP/REST, Decoupled Job Queues, Asynchronous Workflows, Observability")
    print("Syllabus:    Unit IV -- Emerging Paradigms (Web-Based Distributed Systems)")

    init_db()
    client = TestClient(app)

    # In-memory queue simulation to trace message broker transitions transparently
    simulated_redis_queue = []
    mock_queue = MagicMock()
    mock_queue.push_job.side_effect = lambda jid: simulated_redis_queue.append(jid)
    mock_queue.queue_length.side_effect = lambda: len(simulated_redis_queue)
    mock_queue.processing_length.return_value = 0

    print("\n--- [STEP 1] Web Architecture Overview & Service Boundaries ---")
    arch_diagram = """
  +------------------+         HTTP POST /submit_job        +------------------------+
  | Web Client / App | -----------------------------------> | FastAPI Scheduler      |
  | (Browser/Mobile) | <----------------------------------- | (Coordinator / REST)   |
  +------------------+         202 Accepted {job_id}        +-----------+------------+
           |                                                            |
           | HTTP GET /job/{id} (Poll / Webhook)           1. Insert    | 2. LPUSH
           v                                               Job Record   | job_id
  +------------------+                                                  v
  | SQLite Database  | <------------------------------------+   +--------------------+
  | (Durable State & |                                      |   | Redis Message Queue|
  | Fencing Leases)  | <------------------+                 |   | (Ephemeral Broker) |
  +------------------+                    |                 |   +---------+----------+
                                          |                 |             |
                                  3. Claim Lease     4. Ack |             | BRPOPLPUSH
                                  & Write Result     & LREM |             v
                                          |                 |   +--------------------+
                                    +-----+-----------------+-- | Background Worker  |
                                                                | (Stateless Daemon) |
                                                                +--------------------+
"""
    print(arch_diagram)

    # Step 2: Client Submits Job via HTTP REST
    print("\n--- [STEP 2] Client Request Ingestion (HTTP POST /submit_job) ---")
    payload = {
        "job_type": "text_frequency",
        "payload": {
            "text": "Distributed systems emerge from autonomous nodes communicating over networks."
        },
    }
    print(f"  [HTTP Client] POST /submit_job -> {json.dumps(payload)}")

    with patch.object(app.state, "queue", mock_queue, create=True):
        resp = client.post("/submit_job", json=payload)

    if resp.status_code != 202:
        print(f"  [ERROR] Job submission failed: {resp.text}")
        return

    data = resp.json()
    job_id = data["job_id"]
    print(f"  [HTTP Server] HTTP Status: {resp.status_code} Accepted")
    print(f"  * Response Payload: {json.dumps(data)}")
    print(f"  * Redis Queue Depth: {len(simulated_redis_queue)} job(s) in pending list")
    print("  * Architectural Rationale: HTTP 202 prevents blocking the client's connection")
    print("    while long-running computational jobs are queued asynchronously.")

    # Step 3: Worker Claims and Executes Job
    print("\n--- [STEP 3] Asynchronous Worker Processing (Decoupled Execution) ---")
    worker_id = "worker-node-alpha"
    register_worker(worker_id)
    print(f"  [Worker] Worker '{worker_id}' registered and listening on Redis queue...")

    popped_job_id = simulated_redis_queue.pop(0)
    print(f"  [Broker] Redis popped job '{popped_job_id}' to worker '{worker_id}'")

    # Atomic lease claim with fencing token
    claim = claim_job_for_worker(
        job_id=popped_job_id,
        worker_id=worker_id,
        lease_seconds=30.0,
        lamport_ts=10,
    )
    print(f"  [Worker] Acquired durable claim token '{claim['claim_token']}' (Attempt {claim['retry_count']})")

    # Worker executes task
    job_record = get_job(popped_job_id)
    result = execute_task(job_record["job_type"], json.loads(job_record["payload"]))
    print(f"  [Worker] Task execution completed. Top frequencies: {result['top_frequencies'][:3]}")

    # Worker persists result conditional on claim token
    success = update_job_status(
        job_id=popped_job_id,
        status="completed",
        result=result,
        claim_token=claim["claim_token"],
    )
    print(f"  [Database] Fenced status update persisted (success={success})")

    # Step 4: Client Polls for Completion
    print("\n--- [STEP 4] Client Status Query (HTTP GET /job/{job_id}) ---")
    query_resp = client.get(f"/job/{job_id}")
    print(f"  [HTTP Client] GET /job/{job_id} -> Status {query_resp.status_code}")
    job_status = query_resp.json()
    print(f"  * Job Status:       {job_status['status']}")
    print(f"  * Retry Count:      {job_status['retry_count']}")
    print(f"  * Completed By:     {job_status['worker_id']}")
    res_obj = job_status.get("result")
    if isinstance(res_obj, str):
        res_obj = json.loads(res_obj)
    print(f"  * Result Snippet:   Total words={res_obj.get('total_words')}, Unique={res_obj.get('unique_words')}")

    # Step 5: Web-Based Observability Probes
    print("\n--- [STEP 5] Web Observability Endpoints ---")
    with patch.object(app.state, "queue", mock_queue, create=True):
        health_resp = client.get("/health")
        dash_resp = client.get("/api/dashboard_data")

    print(f"  [GET /health]             -> {json.dumps(health_resp.json())}")
    dash_data = dash_resp.json()
    print(f"  [GET /api/dashboard_data] -> Completed Jobs: {dash_data['job_counts']['completed']}, Active Workers: {dash_data['online_workers']}")

    # Step 6: Failure Isolation and Retry Behavior
    print("\n--- [STEP 6] Failure Points & Fault Tolerance Summary ---")
    failures_summary = """
1. Client-to-API Disconnection:
   - Handled via HTTP timeouts, idempotent job UUIDs, and 202 Accepted status.
2. Redis Queue Unavailability:
   - Scheduler catches Redis dispatch errors, atomically transitions job to 'failed'
     in SQLite, and returns HTTP 500 so jobs never get stranded silently in 'pending'.
3. Worker Crash During Execution:
   - Background stale-job recovery loop detects expired worker lease tokens (JOB_LEASE_TIMEOUT_SECONDS)
     and requeues the job up to MAX_RETRIES (3) before routing to the DEAD_LETTER queue.
4. Stale Worker Result Race (Network Partition):
   - Fencing tokens ensure an expired or revived worker cannot overwrite state
     once the scheduler reclaims the job for another worker.
"""
    print(failures_summary)

    print("=" * 78)
    print(" WEB-BASED DISTRIBUTED SYSTEMS DEMONSTRATION COMPLETE")
    print("=" * 78 + "\n")


if __name__ == "__main__":
    run_demo()
