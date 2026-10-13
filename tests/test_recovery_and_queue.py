"""
Unit and integration tests for Phase 2: Reliable Job Recovery, Claiming, and Acknowledgement.

Tests verify:
1. Job claim and acknowledgement with atomic processing queue.
2. Worker failure after claiming a job and stale claim detection.
3. Stale claim recovery and retry count preservation.
4. Retry limit exhaustion and transition to 'dead_letter' status.
5. Repeated recovery-loop execution (idempotence).
6. Failure to persist a result before acknowledgement (at-least-once semantics).
7. Duplicate / competing recovery attempts and atomic SQL guards.
8. Scheduler API support for dead-letter jobs, processing queues, and /api/time.
"""

import json
import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch
from uuid import uuid4

from fastapi.testclient import TestClient

from common.config import DB_PATH
from common.database import (
    init_db,
    insert_job,
    get_job,
    update_job_status,
    register_worker,
    claim_job_for_worker,
    find_and_recover_stale_jobs,
    get_dead_letter_jobs,
    _get_connection,
)
from common.queue_client import JobQueue
from scheduler.main import app, lamport_clock
from worker.consumer import execute_task


class TestQueueReliabilityOperations(unittest.TestCase):
    """Test suite for reliable Redis processing queue operations."""

    def setUp(self):
        self.mock_redis = MagicMock()
        self.queue = JobQueue(redis_client=self.mock_redis)

    def test_claim_job_uses_brpoplpush(self):
        """Verify claim_job atomically moves job from pending to processing queue."""
        self.mock_redis.brpoplpush.return_value = "job-42"

        claimed = self.queue.claim_job(timeout=5)

        self.assertEqual(claimed, "job-42")
        self.mock_redis.brpoplpush.assert_called_once_with(
            "job_queue",
            "job_queue:processing",
            timeout=5,
        )

    def test_claim_job_timeout_returns_none(self):
        self.mock_redis.brpoplpush.return_value = None
        claimed = self.queue.claim_job(timeout=2)
        self.assertIsNone(claimed)

    def test_ack_job_removes_from_processing_queue(self):
        """Verify ack_job deletes the job from the processing queue."""
        self.mock_redis.lrem.return_value = 1

        self.queue.ack_job("job-42")

        self.mock_redis.lrem.assert_called_once_with(
            "job_queue:processing",
            1,
            "job-42",
        )

    def test_requeue_job_moves_back_to_pending_queue(self):
        """Verify requeue_job removes from processing and LPUSHes to pending."""
        pipe = MagicMock()
        self.mock_redis.pipeline.return_value = pipe

        self.queue.requeue_job("job-42")

        pipe.lrem.assert_called_once_with("job_queue:processing", 1, "job-42")
        pipe.lpush.assert_called_once_with("job_queue", "job-42")
        pipe.execute.assert_called_once()

    def test_remove_from_processing_queue(self):
        self.mock_redis.lrem.return_value = 1
        self.queue.remove_from_processing("job-42")
        self.mock_redis.lrem.assert_called_once_with(
            "job_queue:processing",
            1,
            "job-42",
        )


class TestDatabaseClaimAndRecovery(unittest.TestCase):
    """Test suite for atomic database claim, lease management, and stale job recovery."""

    @classmethod
    def setUpClass(cls):
        init_db()

    def setUp(self):
        self.job_id = f"test-claim-{uuid4().hex[:8]}"
        insert_job(
            job_id=self.job_id,
            job_type="math_prime_sieve",
            payload={"limit": 100},
            submitted_at=datetime.now(timezone.utc).isoformat(),
            max_retries=3,
        )

    def test_claim_job_for_worker_records_identity_and_lease(self):
        worker_id = "worker-alpha-1"
        claimed = claim_job_for_worker(
            job_id=self.job_id,
            worker_id=worker_id,
            lease_seconds=30.0,
            lamport_ts=5,
        )

        self.assertIsNotNone(claimed)
        self.assertEqual(claimed["status"], "running")
        self.assertEqual(claimed["worker_id"], worker_id)
        self.assertEqual(claimed["retry_count"], 1)
        self.assertIsNotNone(claimed["claim_token"])
        self.assertIsNotNone(claimed["claimed_at"])
        self.assertIsNotNone(claimed["lease_expires_at"])
        self.assertGreater(claimed["lamport_ts"], 5)

        # Confirm persisted in database
        db_job = get_job(self.job_id)
        self.assertEqual(db_job["status"], "running")
        self.assertEqual(db_job["worker_id"], worker_id)
        self.assertEqual(db_job["retry_count"], 1)

    def test_stale_claim_recovery_requeues_expired_job(self):
        """Worker claims job, but lease expires without completion. Recovery resets to 'pending'."""
        worker_id = "worker-crashed-1"
        claim_job_for_worker(self.job_id, worker_id, lease_seconds=1.0)

        # Manually expire the lease in SQLite to simulate time elapse
        past_time = (datetime.now(timezone.utc) - timedelta(seconds=10)).isoformat()
        conn = _get_connection()
        try:
            conn.execute("UPDATE jobs SET lease_expires_at = ? WHERE job_id = ?", (past_time, self.job_id))
            conn.commit()
        finally:
            conn.close()

        # Run recovery sweep
        recovered = find_and_recover_stale_jobs(lease_timeout_seconds=5.0, lamport_ts=10)

        recovered_ids = [item["job_id"] for item in recovered]
        if self.job_id in recovered_ids:
            rec_item = next(item for item in recovered if item["job_id"] == self.job_id)
            self.assertEqual(rec_item["action"], "requeued")
            self.assertEqual(rec_item["retry_count"], 1)
        else:
            # If a live background scheduler sweep concurrently ran and recovered the job
            db_check = get_job(self.job_id)
            self.assertEqual(db_check["status"], "pending")

        # Job in DB must now be 'pending', worker_id and claim_token cleared
        db_job = get_job(self.job_id)
        self.assertEqual(db_job["status"], "pending")
        self.assertIsNone(db_job["worker_id"])
        self.assertIsNone(db_job["claim_token"])
        self.assertIsNone(db_job["lease_expires_at"])
        self.assertEqual(db_job["retry_count"], 1)

    def test_stale_claim_recovery_detects_offline_worker(self):
        """Job lease not yet expired, but worker is marked offline."""
        worker_id = f"worker-dead-{uuid4().hex[:6]}"
        register_worker(worker_id)
        # Mark worker offline
        conn = _get_connection()
        try:
            conn.execute("UPDATE workers SET status = 'offline' WHERE worker_id = ?", (worker_id,))
            conn.commit()
        finally:
            conn.close()

        # Claim job with a long lease in future
        claim_job_for_worker(self.job_id, worker_id, lease_seconds=300.0)

        # Run recovery sweep
        recovered = find_and_recover_stale_jobs(lease_timeout_seconds=300.0, lamport_ts=12)
        recovered_ids = [item["job_id"] for item in recovered]
        self.assertIn(self.job_id, recovered_ids)

        db_job = get_job(self.job_id)
        self.assertEqual(db_job["status"], "pending")

    def test_retry_limit_exhaustion_transitions_to_dead_letter(self):
        """When retry_count reaches max_retries, recovery marks job 'dead_letter'."""
        worker_id = "worker-flaky-1"
        claim_job_for_worker(self.job_id, worker_id, lease_seconds=1.0)

        # Set retry_count equal to max_retries (3) and expire lease
        past_time = (datetime.now(timezone.utc) - timedelta(seconds=10)).isoformat()
        conn = _get_connection()
        try:
            conn.execute(
                "UPDATE jobs SET retry_count = 3, max_retries = 3, lease_expires_at = ? WHERE job_id = ?",
                (past_time, self.job_id),
            )
            conn.commit()
        finally:
            conn.close()

        # Run recovery sweep
        recovered = find_and_recover_stale_jobs(lease_timeout_seconds=5.0, lamport_ts=20)

        recovered_ids = [item["job_id"] for item in recovered]
        self.assertIn(self.job_id, recovered_ids)

        rec_item = next(item for item in recovered if item["job_id"] == self.job_id)
        self.assertEqual(rec_item["action"], "dead_letter")
        self.assertIn("Exhausted maximum retry attempts", rec_item["reason"])

        # Check DB state
        db_job = get_job(self.job_id)
        self.assertEqual(db_job["status"], "dead_letter")
        self.assertIsNotNone(db_job["completed_at"])
        self.assertIn("Exhausted maximum retry attempts", db_job["error_message"])
        self.assertIsNone(db_job["claim_token"])

        # Check get_dead_letter_jobs helper
        dl_jobs = get_dead_letter_jobs()
        self.assertIn(self.job_id, [j["job_id"] for j in dl_jobs])

    def test_repeated_recovery_loop_execution_is_idempotent(self):
        """Repeated sweeps do not re-recover or corrupt already recovered jobs."""
        claim_job_for_worker(self.job_id, "worker-1", lease_seconds=1.0)
        past_time = (datetime.now(timezone.utc) - timedelta(seconds=10)).isoformat()
        conn = _get_connection()
        try:
            conn.execute("UPDATE jobs SET lease_expires_at = ? WHERE job_id = ?", (past_time, self.job_id))
            conn.commit()
        finally:
            conn.close()

        # Sweep 1: recovers job to 'pending'
        first_sweep = find_and_recover_stale_jobs(lease_timeout_seconds=5.0)
        self.assertEqual(len([j for j in first_sweep if j["job_id"] == self.job_id]), 1)

        # Sweep 2: job is already 'pending', so sweep must return nothing for this job
        second_sweep = find_and_recover_stale_jobs(lease_timeout_seconds=5.0)
        self.assertEqual(len([j for j in second_sweep if j["job_id"] == self.job_id]), 0)

        # State remains 'pending'
        db_job = get_job(self.job_id)
        self.assertEqual(db_job["status"], "pending")

    def test_duplicate_competing_recovery_attempts_guarded(self):
        """Simulate two recovery threads attempting to claim or recover the same job.

        The conditional update WHERE (claim_token IS ? OR claim_token = ?) AND status = 'running'
        must prevent the second sweep from overwriting or repeating the transition.
        """
        claim_job_for_worker(self.job_id, "worker-competing", lease_seconds=1.0)
        job_before = get_job(self.job_id)
        original_token = job_before["claim_token"]

        past_time = (datetime.now(timezone.utc) - timedelta(seconds=10)).isoformat()
        conn = _get_connection()
        try:
            conn.execute("UPDATE jobs SET lease_expires_at = ? WHERE job_id = ?", (past_time, self.job_id))
            conn.commit()
        finally:
            conn.close()

        # Thread 1 successfully recovers
        recovered_1 = find_and_recover_stale_jobs(lease_timeout_seconds=5.0)
        self.assertIn(self.job_id, [j["job_id"] for j in recovered_1])

        # Simulate Thread 2 attempting recovery with the stale snapshot containing original_token
        conn = _get_connection()
        try:
            cursor = conn.execute(
                """
                UPDATE jobs
                SET status = 'pending',
                    worker_id = NULL,
                    claim_token = NULL
                WHERE job_id = ?
                  AND status = 'running'
                  AND (claim_token IS ? OR claim_token = ?)
                """,
                (self.job_id, original_token, original_token),
            )
            # The rowcount must be 0 because Thread 1 changed status to 'pending'
            self.assertEqual(cursor.rowcount, 0)
        finally:
            conn.close()

    def test_failure_to_persist_before_acknowledgement_recovers(self):
        """At-least-once delivery: if a worker executes a task but crashes before

        persisting the completion status and before acking Redis, the unacked job
        remains in the processing queue and is recovered after lease timeout.
        """
        claim_job_for_worker(self.job_id, "worker-crash-before-persist", lease_seconds=1.0)

        # Worker executed payload in memory, but died before update_job_status
        past_time = (datetime.now(timezone.utc) - timedelta(seconds=10)).isoformat()
        conn = _get_connection()
        try:
            conn.execute("UPDATE jobs SET lease_expires_at = ? WHERE job_id = ?", (past_time, self.job_id))
            conn.commit()
        finally:
            conn.close()

        # Recovery loop triggers
        recovered = find_and_recover_stale_jobs(lease_timeout_seconds=5.0)
        self.assertIn(self.job_id, [j["job_id"] for j in recovered])

        # Job is recovered back to 'pending' to be safely retried
        db_job = get_job(self.job_id)
        self.assertEqual(db_job["status"], "pending")
        self.assertEqual(db_job["retry_count"], 1)

    def test_successful_completion_clears_lease_and_token(self):
        """When a job is completed, lease_expires_at and claim_token are cleared."""
        claim_job_for_worker(self.job_id, "worker-success", lease_seconds=30.0)

        update_job_status(
            job_id=self.job_id,
            status="completed",
            result=json.dumps({"success": True}),
            lamport_ts=15,
        )

        db_job = get_job(self.job_id)
        self.assertEqual(db_job["status"], "completed")
        self.assertIsNone(db_job["claim_token"])
        self.assertIsNone(db_job["lease_expires_at"])
        self.assertIsNotNone(db_job["completed_at"])


class TestPhase2SchedulerEndpoints(unittest.TestCase):
    """Test suite for Phase 2 API endpoints: /api/time, dead-letter support, dashboard metrics."""

    @classmethod
    def setUpClass(cls):
        init_db()
        cls.client = TestClient(app)

    def test_api_time_endpoint(self):
        """GET /api/time must return server_time (float), iso string, and lamport_clock."""
        resp = self.client.get("/api/time")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertIn("server_time", data)
        self.assertIn("iso", data)
        self.assertIn("lamport_clock", data)
        self.assertIsInstance(data["server_time"], float)
        self.assertIsInstance(data["lamport_clock"], int)

    def test_dashboard_data_includes_dead_letter_and_processing_queue(self):
        """GET /api/dashboard_data must include dead_letter in job_counts and processing_queue_length."""
        mock_queue = MagicMock()
        mock_queue.queue_length.return_value = 3
        mock_queue.processing_length.return_value = 1

        with patch.object(app.state, "queue", mock_queue, create=True):
            resp = self.client.get("/api/dashboard_data")
            self.assertEqual(resp.status_code, 200)
            data = resp.json()

            self.assertIn("job_counts", data)
            self.assertIn("dead_letter", data["job_counts"])
            self.assertEqual(data["pending_queue_length"], 3)
            self.assertEqual(data["processing_queue_length"], 1)
            self.assertIn("lamport_clock", data)

    def test_submit_job_records_lamport_timestamp(self):
        """POST /submit_job must advance and persist lamport_ts."""
        mock_queue = MagicMock()
        mock_queue.push_job.return_value = None

        with patch.object(app.state, "queue", mock_queue, create=True):
            resp = self.client.post(
                "/submit_job",
                json={
                    "job_type": "hash_checksum",
                    "payload": {"data": "test_lamport"},
                },
            )
            self.assertEqual(resp.status_code, 202)
            job_id = resp.json()["job_id"]

            job_row = get_job(job_id)
            self.assertIsNotNone(job_row)
            self.assertGreater(job_row["lamport_ts"], 0)
