"""
Phase 2.1 Reliability Hardening Regression Tests.

Tests verify:
1. A stale worker attempting completion after another worker has reclaimed the job.
2. A stale worker attempting failure after another worker has reclaimed the job.
3. A stale worker attempting to acknowledge another attempt's processing entry.
4. Duplicate queue entries while a valid lease is active.
5. A completed job stranded in processing.
6. A pending job stranded in processing.
7. A running job missing from the queues (recovers only on valid stale lease).
8. Repeated reconciliation without duplicate enqueueing (idempotence & deduplication).
9. Crash windows between database transitions and Redis operations.
10. Retry-limit behavior under the documented convention (max_retries = total attempts).
"""

import json
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock
from uuid import uuid4

from common.config import JOB_LEASE_TIMEOUT_SECONDS
from common.database import (
    init_db,
    insert_job,
    get_job,
    update_job_status,
    claim_job_for_worker,
    find_and_recover_stale_jobs,
    reconcile_processing_queue,
    _get_connection,
)
from common.queue_client import JobQueue


class TestStaleWorkerFencing(unittest.TestCase):
    """Test suite verifying claim-token fencing on worker database transitions."""

    @classmethod
    def setUpClass(cls):
        init_db()

    def setUp(self):
        self.job_id = f"fence-job-{uuid4().hex[:8]}"
        insert_job(
            job_id=self.job_id,
            job_type="math_prime_sieve",
            payload={"limit": 100},
            submitted_at=datetime.now(timezone.utc).isoformat(),
            max_retries=3,
        )

    def test_1_stale_worker_completion_rejected_after_reclaim(self):
        """Worker 1 loses claim after lease expires. Worker 2 reclaims.

        Worker 1's late completion MUST be rejected and must not overwrite Worker 2.
        """
        # Worker 1 claims job
        claim1 = claim_job_for_worker(self.job_id, "worker-1", lease_seconds=1.0)
        self.assertIsNotNone(claim1)
        token1 = claim1["claim_token"]

        # Simulate lease expiry in SQLite
        past_time = (datetime.now(timezone.utc) - timedelta(seconds=10)).isoformat()
        conn = _get_connection()
        try:
            conn.execute("UPDATE jobs SET lease_expires_at = ? WHERE job_id = ?", (past_time, self.job_id))
            conn.commit()
        finally:
            conn.close()

        # Worker 2 claims the expired job
        claim2 = claim_job_for_worker(self.job_id, "worker-2", lease_seconds=30.0)
        self.assertIsNotNone(claim2)
        token2 = claim2["claim_token"]
        self.assertNotEqual(token1, token2)

        # Worker 1 finishes late and attempts completion with stale token1
        result1 = json.dumps({"output": "worker-1-stale-result"})
        accepted1 = update_job_status(
            self.job_id,
            "completed",
            worker_id="worker-1",
            result=result1,
            claim_token=token1,
        )
        self.assertFalse(accepted1, "Worker 1 completion must be rejected due to stale claim token")

        # Database state MUST still be 'running' under Worker 2 with token2
        db_job = get_job(self.job_id)
        self.assertEqual(db_job["status"], "running")
        self.assertEqual(db_job["worker_id"], "worker-2")
        self.assertEqual(db_job["claim_token"], token2)

        # Worker 2 finishes and updates with valid token2
        result2 = json.dumps({"output": "worker-2-legitimate-result"})
        accepted2 = update_job_status(
            self.job_id,
            "completed",
            worker_id="worker-2",
            result=result2,
            claim_token=token2,
        )
        self.assertTrue(accepted2, "Worker 2 completion with current token must be accepted")

        # Database state is now 'completed' with Worker 2's result
        final_job = get_job(self.job_id)
        self.assertEqual(final_job["status"], "completed")
        self.assertEqual(final_job["worker_id"], "worker-2")
        self.assertIn("worker-2-legitimate-result", final_job["result"])

    def test_2_stale_worker_failure_rejected_after_reclaim(self):
        """Worker 1 crashes late and attempts failure report.

        Worker 2 reclaims. Worker 1's failure update MUST be rejected.
        """
        claim1 = claim_job_for_worker(self.job_id, "worker-1", lease_seconds=1.0)
        token1 = claim1["claim_token"]

        past_time = (datetime.now(timezone.utc) - timedelta(seconds=10)).isoformat()
        conn = _get_connection()
        try:
            conn.execute("UPDATE jobs SET lease_expires_at = ? WHERE job_id = ?", (past_time, self.job_id))
            conn.commit()
        finally:
            conn.close()

        claim2 = claim_job_for_worker(self.job_id, "worker-2", lease_seconds=30.0)
        token2 = claim2["claim_token"]

        # Worker 1 late failure report
        accepted1 = update_job_status(
            self.job_id,
            "failed",
            worker_id="worker-1",
            error_message="Worker 1 timeout error",
            claim_token=token1,
        )
        self.assertFalse(accepted1, "Worker 1 failure update must be rejected")

        # Job must still be running under Worker 2
        db_job = get_job(self.job_id)
        self.assertEqual(db_job["status"], "running")
        self.assertEqual(db_job["claim_token"], token2)


class TestQueueFencingAndOwnership(unittest.TestCase):
    """Test suite verifying claim-token-aware queue operations and lease protection."""

    def test_3_stale_worker_ack_rejected_with_token_mismatch(self):
        """Verify ack_job with claim_token rejects ACK when token does not match active claim."""
        mock_redis = MagicMock()
        # Recorded claim in Redis belongs to worker-2
        mock_redis.get.return_value = "token-worker-2"
        queue = JobQueue(redis_client=mock_redis)

        # Worker 1 attempts to ACK with stale token-worker-1
        acked = queue.ack_job("job-100", claim_token="token-worker-1")

        self.assertFalse(acked)
        # Verify LREM was NOT called because token mismatched!
        mock_redis.lrem.assert_not_called()

        # Worker 2 attempts to ACK with matching token-worker-2
        mock_redis.get.return_value = "token-worker-2"
        mock_redis.lrem.return_value = 1
        acked_valid = queue.ack_job("job-100", claim_token="token-worker-2")

        self.assertTrue(acked_valid)
        mock_redis.lrem.assert_called_once_with("job_queue:processing", 1, "job-100")

    def test_4_duplicate_queue_entries_cannot_steal_active_lease(self):
        """If a duplicate queue entry is popped while a job is running with a valid lease,

        claim_job_for_worker MUST reject the second claim.
        """
        init_db()
        job_id = f"active-lease-{uuid4().hex[:8]}"
        insert_job(job_id, "hash_checksum", {"data": "test"}, datetime.now(timezone.utc).isoformat())

        # Worker 1 claims with active 30s lease
        claim1 = claim_job_for_worker(job_id, "worker-1", lease_seconds=30.0)
        self.assertIsNotNone(claim1)

        # Duplicate entry arrives in queue -> Worker 2 attempts claim while lease is active
        claim2 = claim_job_for_worker(job_id, "worker-2", lease_seconds=30.0)
        self.assertIsNone(claim2, "Active running job with valid lease must not be stolen by duplicate claim")

        # Job remains intact under Worker 1
        db_job = get_job(job_id)
        self.assertEqual(db_job["worker_id"], "worker-1")
        self.assertEqual(db_job["retry_count"], 1)


class TestQueueReconciliation(unittest.TestCase):
    """Test suite verifying idempotent Redis processing queue reconciliation."""

    @classmethod
    def setUpClass(cls):
        init_db()

    def test_5_completed_job_stranded_in_processing_is_evicted(self):
        """A completed job left in processing (worker died before ACK) must be evicted."""
        job_id = f"stranded-comp-{uuid4().hex[:8]}"
        insert_job(job_id, "test", {}, datetime.now(timezone.utc).isoformat())
        update_job_status(job_id, "completed", result=json.dumps({"ok": True}))

        actions = reconcile_processing_queue([job_id])
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0]["action"], "evict")
        self.assertEqual(actions[0]["job_id"], job_id)
        self.assertIn("already_completed", actions[0]["reason"])

    def test_6_pending_job_stranded_in_processing_is_requeued(self):
        """A pending job left in processing (crash after reset before requeue) must be requeued."""
        job_id = f"stranded-pend-{uuid4().hex[:8]}"
        insert_job(job_id, "test", {}, datetime.now(timezone.utc).isoformat())

        actions = reconcile_processing_queue([job_id])
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0]["action"], "requeue")
        self.assertEqual(actions[0]["job_id"], job_id)

    def test_7_running_job_missing_from_queues_recovers_only_when_stale(self):
        """Running job missing from queues recovers ONLY when lease has expired."""
        job_id = f"running-missing-{uuid4().hex[:8]}"
        insert_job(job_id, "test", {}, datetime.now(timezone.utc).isoformat())
        claim_job_for_worker(job_id, "worker-active", lease_seconds=30.0)

        # Valid lease: recovery must NOT touch it
        stale_none = find_and_recover_stale_jobs(lease_timeout_seconds=30.0)
        self.assertNotIn(job_id, [item["job_id"] for item in stale_none])

        # Expire the lease manually
        past_time = (datetime.now(timezone.utc) - timedelta(seconds=10)).isoformat()
        conn = _get_connection()
        try:
            conn.execute("UPDATE jobs SET lease_expires_at = ? WHERE job_id = ?", (past_time, job_id))
            conn.commit()
        finally:
            conn.close()

        # Now recovery detects and resets to 'pending'
        stale_recovered = find_and_recover_stale_jobs(lease_timeout_seconds=5.0)
        self.assertIn(job_id, [item["job_id"] for item in stale_recovered])

        db_job = get_job(job_id)
        self.assertEqual(db_job["status"], "pending")

    def test_8_repeated_reconciliation_is_idempotent_and_deduplicates(self):
        """Reconciliation deduplicates duplicate queue IDs and repeated runs are safe."""
        job_id = f"reconcile-dup-{uuid4().hex[:8]}"
        insert_job(job_id, "test", {}, datetime.now(timezone.utc).isoformat())
        update_job_status(job_id, "dead_letter", error_message="exhausted")

        # Pass duplicate entries in processing list
        actions = reconcile_processing_queue([job_id, job_id, job_id])
        # Must return exactly ONE evict action for this job_id
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0]["action"], "evict")

    def test_9_crash_window_between_db_commit_and_redis_ack(self):
        """Simulate crash window: worker commits 'completed' to SQLite, but crashes before ACK.

        Reconciliation successfully repairs this window by evicting the stranded job.
        """
        job_id = f"crash-window-{uuid4().hex[:8]}"
        insert_job(job_id, "test", {}, datetime.now(timezone.utc).isoformat())
        claim = claim_job_for_worker(job_id, "worker-crashed", lease_seconds=30.0)
        token = claim["claim_token"]

        # Worker commits completed status
        persisted = update_job_status(
            job_id,
            "completed",
            worker_id="worker-crashed",
            result=json.dumps({"done": True}),
            claim_token=token,
        )
        self.assertTrue(persisted)

        # Worker dies before calling queue.ack_job() -> job is still in processing_jobs list
        mock_redis = MagicMock()
        mock_redis.lrange.return_value = [job_id]
        mock_redis.lrem.return_value = 1
        queue = JobQueue(redis_client=mock_redis)

        # Scheduler recovery executes reconciliation pass
        processing_jobs = queue.get_processing_jobs()
        reconciled = reconcile_processing_queue(processing_jobs)

        self.assertEqual(len(reconciled), 1)
        self.assertEqual(reconciled[0]["action"], "evict")

        # Evict action executed
        queue.remove_from_processing(job_id)
        mock_redis.lrem.assert_called_with("job_queue:processing", 1, job_id)

    def test_10_retry_limit_behavior_under_documented_convention(self):
        """Documented convention: max_retries = 3 means maximum 3 total execution attempts.

        Attempt 1: Fail -> recover to pending (attempt 1 of 3)
        Attempt 2: Fail -> recover to pending (attempt 2 of 3)
        Attempt 3: Fail -> dead_letter (attempt 3 of 3 exhausted)
        """
        job_id = f"retry-limit-{uuid4().hex[:8]}"
        insert_job(job_id, "test", {}, datetime.now(timezone.utc).isoformat(), max_retries=3)

        # Attempt 1
        c1 = claim_job_for_worker(job_id, "w1", lease_seconds=0.01)
        self.assertEqual(c1["retry_count"], 1)

        past_time = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
        conn = _get_connection()
        conn.execute("UPDATE jobs SET lease_expires_at = ? WHERE job_id = ?", (past_time, job_id))
        conn.commit()
        conn.close()

        rec1 = find_and_recover_stale_jobs()
        self.assertEqual(next(r for r in rec1 if r["job_id"] == job_id)["action"], "requeued")

        # Attempt 2
        c2 = claim_job_for_worker(job_id, "w2", lease_seconds=0.01)
        self.assertEqual(c2["retry_count"], 2)

        conn = _get_connection()
        conn.execute("UPDATE jobs SET lease_expires_at = ? WHERE job_id = ?", (past_time, job_id))
        conn.commit()
        conn.close()

        rec2 = find_and_recover_stale_jobs()
        self.assertEqual(next(r for r in rec2 if r["job_id"] == job_id)["action"], "requeued")

        # Attempt 3 (Reaching max_retries = 3)
        c3 = claim_job_for_worker(job_id, "w3", lease_seconds=0.01)
        self.assertEqual(c3["retry_count"], 3)

        conn = _get_connection()
        conn.execute("UPDATE jobs SET lease_expires_at = ? WHERE job_id = ?", (past_time, job_id))
        conn.commit()
        conn.close()

        # Recovery on Attempt 3 triggers dead_letter!
        rec3 = find_and_recover_stale_jobs()
        self.assertEqual(next(r for r in rec3 if r["job_id"] == job_id)["action"], "dead_letter")

        db_job = get_job(job_id)
        self.assertEqual(db_job["status"], "dead_letter")
        self.assertEqual(db_job["retry_count"], 3)
        self.assertEqual(db_job["max_retries"], 3)
        self.assertIn("Exhausted maximum retry attempts (3/3)", db_job["error_message"])


class TestPhase1TruthfulObservabilityAndWorkerLiveness(unittest.TestCase):
    """Regression test suite for Phase 1: Real worker liveness, truthful metrics, and handler idempotency."""

    @classmethod
    def setUpClass(cls):
        init_db()

    def test_11_worker_registration_stores_process_identity(self):
        """Worker registration stores OS process ID and startup timestamp."""
        from common.database import register_worker, get_all_workers
        worker_id = f"worker-proc-{uuid4().hex[:8]}"
        register_worker(worker_id, pid=12345, startup_time="2026-10-13T10:00:00+00:00")

        workers = get_all_workers()
        w = next(w for w in workers if w["worker_id"] == worker_id)
        self.assertEqual(w["pid"], 12345)
        self.assertEqual(w["startup_time"], "2026-10-13T10:00:00+00:00")
        self.assertEqual(w["status"], "online")

    def test_12_worker_heartbeat_freshness_filter(self):
        """Workers with heartbeats older than threshold transition offline."""
        from common.database import register_worker, mark_stale_workers_offline, get_all_workers
        w_live = f"worker-live-{uuid4().hex[:8]}"
        w_stale = f"worker-stale-{uuid4().hex[:8]}"

        register_worker(w_live, pid=1111)
        register_worker(w_stale, pid=2222)

        # Manually backdate w_stale heartbeat to 30 seconds ago
        stale_time = (datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat()
        conn = _get_connection()
        conn.execute("UPDATE workers SET last_heartbeat = ? WHERE worker_id = ?", (stale_time, w_stale))
        conn.commit()
        conn.close()

        marked = mark_stale_workers_offline(timeout_seconds=15.0)
        self.assertIn(w_stale, marked)
        self.assertNotIn(w_live, marked)

        workers = get_all_workers()
        stale_node = next(w for w in workers if w["worker_id"] == w_stale)
        live_node = next(w for w in workers if w["worker_id"] == w_live)
        self.assertEqual(stale_node["status"], "offline")
        self.assertEqual(live_node["status"], "online")

    def test_13_idempotent_email_handler_replay(self):
        """At-least-once handler replay generates identical message_id for identical inputs."""
        from worker.handlers import handle_email
        payload = {
            "to": "alice@example.com",
            "subject": "System Alert",
            "body": "Node 3 rebooted.",
        }
        res1 = handle_email(payload)
        res2 = handle_email(payload)

        self.assertEqual(res1["status"], "delivered")
        self.assertEqual(res1["message_id"], res2["message_id"])
        self.assertEqual(res1["idempotency_key"], res2["idempotency_key"])

    def test_14_clean_test_artifacts_utility(self):
        """Test artifact cleaner purges synthetic test jobs and workers."""
        from common.database import clean_test_artifacts
        test_job_id = f"test-synthetic-{uuid4().hex[:8]}"
        test_worker_id = f"worker-test-synthetic-{uuid4().hex[:8]}"

        insert_job(test_job_id, "test", {}, datetime.now(timezone.utc).isoformat())
        from common.database import register_worker
        register_worker(test_worker_id)

        cleaned = clean_test_artifacts()
        self.assertGreaterEqual(cleaned["deleted_jobs"], 1)
        self.assertGreaterEqual(cleaned["deleted_workers"], 1)

        db_job = get_job(test_job_id)
        self.assertIsNone(db_job)
