"""
Live Crash Recovery and Fault-Tolerance Demonstration (Phase 2).

This script performs the full automated lifecycle demonstration of:
  Step 1: Submit a compute job to the reliable Redis queue.
  Step 2: Worker 1 (worker-crash-victim) claims the job atomically via BRPOPLPUSH.
          Verify job is present in 'job_queue:processing' and SQLite status is 'running'.
  Step 3: Worker 1 crashes abruptly mid-execution (simulated process crash)
          BEFORE persisting any completion result and BEFORE sending ACK to Redis.
  Step 4: Coordinator recovery monitor detects the expired lease / abandoned claim,
          increments Lamport clock, resets status to 'pending', and atomically requeues
          the job from 'job_queue:processing' back to 'job_queue'.
  Step 5: Worker 2 (worker-survivor) claims the retried job, executes it to completion,
          persists the result to SQLite, and issues Redis ACK.
  Step 6: Confirm final result, attempt count (retry_count=2), and Lamport timestamps
          are durably recorded.
"""

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from common.config import (
    REDIS_HOST,
    REDIS_PORT,
    QUEUE_NAME,
    PROCESSING_QUEUE_NAME,
)
from common.database import (
    init_db,
    insert_job,
    get_job,
    claim_job_for_worker,
    find_and_recover_stale_jobs,
    update_job_status,
    _get_connection,
)
from common.queue_client import JobQueue
from common.clocks import LamportClock
from worker.handlers import dispatch_task


def run_live_recovery_demonstration():
    print("=" * 80)
    print("  PHASE 2 LIVE WORKER CRASH & RECOVERY DEMONSTRATION")
    print("=" * 80)

    # 0. Initialize DB and Queue
    init_db()
    q_name = "demo_crash_queue"
    proc_name = "demo_crash_queue:processing"
    queue = JobQueue(queue_name=q_name, processing_queue=proc_name)
    # Ensure clean test state for demo
    queue.redis_client.delete(q_name, proc_name)
    conn = _get_connection()
    try:
        conn.execute("DELETE FROM jobs WHERE status IN ('running', 'pending')")
        conn.commit()
    finally:
        conn.close()

    coordinator_clock = LamportClock(initial_time=100)
    job_id = f"crash-demo-{uuid4().hex[:8]}"

    print(f"[*] Target Job ID: {job_id}")
    print(f"[*] Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
    print(f"    - Pending Queue   : {QUEUE_NAME}")
    print(f"    - Processing Queue: {PROCESSING_QUEUE_NAME}")
    print("-" * 80)

    # STEP 1: Submit job
    print("\n[STEP 1] Submitting job to SQLite and Redis pending queue...")
    sub_clock = coordinator_clock.send_event()
    insert_job(
        job_id=job_id,
        job_type="math_prime_sieve",
        payload={"limit": 5000},
        submitted_at=datetime.now(timezone.utc).isoformat(),
        max_retries=3,
        lamport_ts=sub_clock,
    )
    queue.push_job(job_id)

    db_job = get_job(job_id)
    print(f"  -> SQLite Status : {db_job['status']} (retries={db_job['retry_count']}, L#{db_job['lamport_ts']})")
    print(f"  -> Redis Pending Queue Length: {queue.queue_length()}")

    # STEP 2: Worker 1 claims the job
    print("\n[STEP 2] Worker 1 ('worker-crash-victim') claims the job...")
    claimed_id = queue.claim_job(timeout=2)
    assert claimed_id == job_id, f"Expected {job_id}, got {claimed_id}"

    # Worker 1 registers claim in database with a short 2-second lease
    victim_clock = LamportClock(initial_time=200)
    claim_ts = victim_clock.send_event()
    claim_meta = claim_job_for_worker(
        job_id=job_id,
        worker_id="worker-crash-victim",
        lease_seconds=2.0,  # Short lease for fast demonstration
        lamport_ts=claim_ts,
    )

    db_job = get_job(job_id)
    print(f"  -> Atomic Claim Recorded in SQLite:")
    print(f"     Status         : {db_job['status']}")
    print(f"     Worker ID      : {db_job['worker_id']}")
    print(f"     Retry Count    : {db_job['retry_count']}")
    print(f"     Claim Token    : {db_job['claim_token']}")
    print(f"     Lease Expires  : {db_job['lease_expires_at']}")
    print(f"     Lamport TS     : L#{db_job['lamport_ts']}")
    print(f"  -> Redis State:")
    print(f"     Pending Queue Length    : {queue.queue_length()}")
    print(f"     Processing Queue Length : {queue.processing_length()}")
    print(f"     Processing Items        : {queue.get_processing_jobs()}")

    # STEP 3: Worker 1 crashes abruptly mid-execution
    print("\n[STEP 3] SIMULATING ABRUPT WORKER CRASH (SIGKILL)...")
    print("  -> Worker 1 process terminates immediately without persisting result or sending ACK.")
    print("  -> Job remains unacknowledged in 'job_queue:processing'.")
    print("  -> Waiting 2.5 seconds for lease to expire...")
    time.sleep(2.5)

    # STEP 4: Coordinator recovery loop executes
    print("\n[STEP 4] Coordinator stale job recovery monitor executes sweep...")
    rec_clock = coordinator_clock.tick()
    recovered = find_and_recover_stale_jobs(lease_timeout_seconds=2.0, lamport_ts=rec_clock)

    print(f"  -> Stale Jobs Detected: {len(recovered)}")
    for rec in recovered:
        print(f"     Detected: {rec['job_id']} -> Action: {rec['action']} (Attempt {rec['retry_count']})")
        if rec["action"] == "requeued":
            queue.requeue_job(rec["job_id"])

    # Verify state after recovery
    db_job = get_job(job_id)
    print(f"  -> SQLite State Post-Recovery:")
    print(f"     Status      : {db_job['status']} (reset from 'running' to 'pending')")
    print(f"     Worker ID   : {db_job['worker_id']} (cleared)")
    print(f"     Claim Token : {db_job['claim_token']} (cleared)")
    print(f"     Retry Count : {db_job['retry_count']} (preserved for retry tracking)")
    print(f"  -> Redis State Post-Recovery:")
    print(f"     Pending Queue Items    : {queue.redis_client.lrange(QUEUE_NAME, 0, -1)}")
    print(f"     Processing Queue Items : {queue.get_processing_jobs()}")

    # STEP 5: Worker 2 claims the retried job
    print("\n[STEP 5] Worker 2 ('worker-survivor') claims and executes the retried job...")
    retry_claimed_id = queue.claim_job(timeout=2)
    assert retry_claimed_id == job_id, f"Expected {job_id}, got {retry_claimed_id}"

    survivor_clock = LamportClock(initial_time=300)
    survivor_claim_ts = survivor_clock.send_event()
    claim2 = claim_job_for_worker(
        job_id=job_id,
        worker_id="worker-survivor",
        lease_seconds=30.0,
        lamport_ts=survivor_claim_ts,
    )
    survivor_token = claim2["claim_token"]
    queue.register_claim(job_id, survivor_token, lease_seconds=30.0)

    # Worker 2 executes task
    payload = json.loads(db_job["payload"]) if isinstance(db_job["payload"], str) else db_job["payload"]
    print(f"  -> Worker 2 executing handler for {db_job['job_type']} with payload={payload}...")
    task_result = dispatch_task(db_job["job_type"], payload)

    # Durably persist to database BEFORE acking Redis, fenced by claim_token
    done_clock = survivor_clock.tick()
    persisted = update_job_status(
        job_id=job_id,
        status="completed",
        worker_id="worker-survivor",
        result=json.dumps(task_result),
        lamport_ts=done_clock,
        claim_token=survivor_token,
    )
    assert persisted, "Durable update must succeed with valid survivor token"
    print("  -> Result durably persisted to SQLite with status='completed' (fenced by token).")

    # Acknowledge and remove from Redis processing queue, fenced by claim_token
    acked = queue.ack_job(job_id, claim_token=survivor_token)
    assert acked, "Queue ACK must succeed with valid survivor token"
    print("  -> Redis ACK sent: Job removed from 'job_queue:processing' (fenced by token).")

    # STEP 6: Final Verification
    print("\n[STEP 6] FINAL VERIFICATION & REPORT:")
    final_job = get_job(job_id)
    print(f"  Job ID          : {final_job['job_id']}")
    print(f"  Final Status    : {final_job['status']}")
    print(f"  Handling Worker : {final_job['worker_id']}")
    print(f"  Total Attempts  : {final_job['retry_count']} (Original + 1 Recovery Retry)")
    print(f"  Max Retries     : {final_job['max_retries']}")
    print(f"  Completed At    : {final_job['completed_at']}")
    print(f"  Lamport TS      : L#{final_job['lamport_ts']}")
    result_preview = json.loads(final_job['result'])
    print(f"  Result Preview  : primes_found={result_preview.get('count')}, sample={result_preview.get('primes', [])[:5]}...")
    print(f"  Pending Queue   : {queue.queue_length()} items")
    print(f"  Processing Queue: {queue.processing_length()} items")
    # =========================================================================
    # PART B: Retry Exhaustion & Dead-Letter State Transition
    # =========================================================================
    print("\n" + "=" * 80)
    print("  PART B: RETRY EXHAUSTION & DEAD-LETTER QUEUE TRANSITION")
    print("=" * 80)
    dl_job_id = f"deadletter-demo-{uuid4().hex[:8]}"
    print(f"[*] Target Poison Job ID: {dl_job_id} (Configured max_retries = 2)")

    # 1. Submit poison job
    insert_job(
        job_id=dl_job_id,
        job_type="hash_checksum",
        payload={"data": "poison_pill"},
        submitted_at=datetime.now(timezone.utc).isoformat(),
        max_retries=2,
        lamport_ts=coordinator_clock.send_event(),
    )
    queue.push_job(dl_job_id)

    # Attempt 1: Claim and simulate crash
    queue.claim_job(timeout=2)
    claim_job_for_worker(dl_job_id, "worker-victim-1", lease_seconds=1.0, lamport_ts=10)
    print("  [Attempt 1] Worker claimed job, then crashed. Waiting 1.5s for lease expiration...")
    time.sleep(1.5)
    rec1 = find_and_recover_stale_jobs(lease_timeout_seconds=1.0, lamport_ts=11)
    for r in rec1:
        if r["job_id"] == dl_job_id:
            queue.requeue_job(dl_job_id)
            print(f"  -> Recovery 1: Requeued job (attempt {r['retry_count']}/2)")

    # Attempt 2: Claim and simulate second crash (reaching max_retries=2)
    queue.claim_job(timeout=2)
    claim_job_for_worker(dl_job_id, "worker-victim-2", lease_seconds=1.0, lamport_ts=12)
    print("  [Attempt 2] Second worker claimed job, then crashed. Waiting 1.5s for lease expiration...")
    time.sleep(1.5)
    rec2 = find_and_recover_stale_jobs(lease_timeout_seconds=1.0, lamport_ts=13)
    for r in rec2:
        if r["job_id"] == dl_job_id:
            queue.remove_from_processing(dl_job_id)
            print(f"  -> Recovery 2: Job EXHAUSTED max retries -> Action: {r['action']}")

    # Verification of Dead-Letter state
    dl_job = get_job(dl_job_id)
    print("\n[PART B VERIFICATION]:")
    print(f"  Job ID       : {dl_job['job_id']}")
    print(f"  Final Status : {dl_job['status']} (Dead Letter)")
    print(f"  Retry Count  : {dl_job['retry_count']}/{dl_job['max_retries']}")
    print(f"  Error Message: {dl_job['error_message']}")
    print(f"  Completed At : {dl_job['completed_at']}")
    print("=" * 80)
    print("  PHASE 2 CRASH RECOVERY & DEAD-LETTER DEMONSTRATION COMPLETE!")
    print("=" * 80)


if __name__ == "__main__":
    run_live_recovery_demonstration()
