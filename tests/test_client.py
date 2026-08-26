"""
Integration test client for the distributed job platform (Stage 4).

Demonstrates:
  1. Asynchronous Queue-based Execution (Stage 3).
  2. Worker Health Check & Liveness Monitoring (Stage 4) via ``GET /workers``.

Run this *after* starting Redis, the Scheduler, and at least one Worker Consumer:
  Terminal 1: uvicorn scheduler.main:app --port 8000
  Terminal 2: python -m worker.consumer
  Terminal 3: python -m tests.test_client
"""

import sys
import json
import time

import requests

from common.config import SCHEDULER_URL

SAMPLE_JOBS = [
    {
        "job_type": "email",
        "payload": {
            "to": "user@example.com",
            "subject": "Welcome!",
            "body": "Thanks for signing up.",
        },
    },
    {
        "job_type": "csv_processing",
        "payload": {
            "file_path": "/data/uploads/sales_q3.csv",
            "operations": ["deduplicate", "aggregate"],
        },
    },
    {
        "job_type": "thumbnail",
        "payload": {
            "image_url": "https://example.com/photos/landscape.jpg",
            "sizes": [128, 256, 512],
        },
    },
]


def poll_job_until_finished(job_id: str, max_wait_seconds: int = 15) -> dict:
    """Poll GET /job/{job_id} every 1s until status is no longer pending/queued/running."""
    start_time = time.time()
    while time.time() - start_time < max_wait_seconds:
        try:
            res = requests.get(f"{SCHEDULER_URL}/job/{job_id}", timeout=5)
            res.raise_for_status()
            data = res.json()
            status = data.get("status")

            if status in ("completed", "failed"):
                return data

        except Exception as exc:
            print(f"    [Warning] Polling error for {job_id}: {exc}")

        time.sleep(1)

    res = requests.get(f"{SCHEDULER_URL}/job/{job_id}", timeout=5)
    return res.json() if res.status_code == 200 else {}


def main() -> None:
    """Submit sample jobs, poll for completion, display job history, and query worker health."""
    print("=" * 70)
    print("  Distributed Job Platform - Stage 4 Integration & Health Test")
    print("=" * 70)

    # Check scheduler health
    try:
        health = requests.get(f"{SCHEDULER_URL}/health", timeout=5)
        health.raise_for_status()
        print(f"\n[OK] Scheduler is healthy: {health.json()}\n")
    except requests.ConnectionError:
        print(
            "\n[FAIL] Cannot reach the scheduler at "
            f"{SCHEDULER_URL}. Is it running?\n"
        )
        sys.exit(1)

    # ------------------------------------------------------------------
    # Phase 1: Submit jobs (Immediate Async Acceptance)
    # ------------------------------------------------------------------
    print("-" * 70)
    print("  Phase 1: Submit jobs (Immediate Async Scheduler Response)")
    print("-" * 70)

    submitted_jobs = []

    for idx, job in enumerate(SAMPLE_JOBS, start=1):
        print(f"\n--- Job {idx}/{len(SAMPLE_JOBS)}: {job['job_type']} ---")
        print(f"  Payload: {json.dumps(job['payload'], indent=2)}")

        try:
            response = requests.post(
                f"{SCHEDULER_URL}/submit_job",
                json=job,
                timeout=10,
            )
            response.raise_for_status()
            data = response.json()

            job_id = data.get("job_id", "N/A")
            submitted_jobs.append(job_id)

            print(f"  HTTP Status     : {response.status_code} (Accepted)")
            print(f"  Assigned Job ID : {job_id}")
            print(f"  Immediate Status: '{data.get('status', 'N/A')}' (Enqueued in Redis)")
            print(f"  Message         : {data.get('message', 'N/A')}")

        except requests.ConnectionError:
            print("  [FAIL] Scheduler unreachable - skipping.")
        except requests.HTTPError as exc:
            print(f"  [FAIL] HTTP error: {exc}")
            print(f"  Response body: {response.text}")

    # ------------------------------------------------------------------
    # Phase 2: Poll for Worker Execution Completion
    # ------------------------------------------------------------------
    print("\n" + "-" * 70)
    print("  Phase 2: Poll GET /job/{job_id} for Worker Execution")
    print("-" * 70)

    for job_id in submitted_jobs:
        print(f"\n  Polling job_id={job_id}...")
        final_state = poll_job_until_finished(job_id, max_wait_seconds=15)

        print(f"  Final Status   : {final_state.get('status', 'N/A')}")
        print(f"  Executed By    : {final_state.get('worker_id', 'N/A')}")
        print(f"  Submitted At   : {final_state.get('submitted_at', 'N/A')}")
        print(f"  Started At     : {final_state.get('started_at', 'N/A')}")
        print(f"  Completed At   : {final_state.get('completed_at', 'N/A')}")
        if final_state.get("result"):
            print(f"  Worker Result  : {final_state.get('result')}")

    # ------------------------------------------------------------------
    # Phase 3: Fetch and display full job history from SQLite
    # ------------------------------------------------------------------
    print("\n" + "-" * 70)
    print("  Phase 3: Full job history from SQLite (GET /jobs)")
    print("-" * 70)

    try:
        history = requests.get(f"{SCHEDULER_URL}/jobs", timeout=5)
        history.raise_for_status()
        jobs = history.json()

        print(f"\n  Total jobs in database: {len(jobs)}\n")

        print(
            f"  {'Job ID':<38} {'Type':<18} {'Status':<12} {'Worker'}"
        )
        print(f"  {'-' * 36}  {'-' * 16}  {'-' * 10}  {'-' * 16}")

        for j in jobs:
            print(
                f"  {j.get('job_id', 'N/A'):<38} "
                f"{j.get('job_type', 'N/A'):<18} "
                f"{j.get('status', 'N/A'):<12} "
                f"{j.get('worker_id', 'N/A')}"
            )

    except requests.ConnectionError:
        print("  [FAIL] Scheduler unreachable.")
    except requests.HTTPError as exc:
        print(f"  [FAIL] HTTP error: {exc}")

    # ------------------------------------------------------------------
    # Phase 4: Worker Health Check & Registry Status (Stage 4)
    # ------------------------------------------------------------------
    print("\n" + "-" * 70)
    print("  Phase 4: Worker Health Check & Registry Status (GET /workers)")
    print("-" * 70)

    try:
        workers_resp = requests.get(f"{SCHEDULER_URL}/workers", timeout=5)
        workers_resp.raise_for_status()
        workers = workers_resp.json()

        print(f"\n  Total registered workers: {len(workers)}\n")

        print(
            f"  {'Worker ID':<22} {'Status':<10} {'Jobs Done':<11} {'Last Heartbeat':<30}"
        )
        print(f"  {'-' * 20}  {'-' * 8}  {'-' * 9}  {'-' * 28}")

        for w in workers:
            print(
                f"  {w.get('worker_id', 'N/A'):<22} "
                f"{w.get('status', 'N/A'):<10} "
                f"{w.get('jobs_completed', 0):<11} "
                f"{w.get('last_heartbeat', 'N/A'):<30}"
            )

    except requests.ConnectionError:
        print("  [FAIL] Scheduler unreachable.")
    except requests.HTTPError as exc:
        print(f"  [FAIL] HTTP error: {exc}")

    print("\n" + "=" * 70)
    print("  All Stage 4 tests complete. Worker health monitoring verified!")
    print("=" * 70)


if __name__ == "__main__":
    main()
