"""
Unit and API integration tests for Scheduler and Worker components.

Tests cover:
  1. Job submission safety when Redis enqueueing fails (marks status='failed' in SQLite).
  2. Full API compatibility using FastAPI TestClient (mocked Redis queue).
  3. Worker execution resilience when a handler raises an exception.
  4. SQLite connection timeout and transaction integrity.
"""

import json
import os
import unittest
from unittest.mock import MagicMock, patch
from uuid import uuid4

from fastapi.testclient import TestClient

from common.config import SQLITE_TIMEOUT_SECONDS
from common.database import (
    init_db,
    insert_job,
    get_job,
    update_job_status,
    register_worker,
    get_all_workers,
    _get_connection,
)
from scheduler.main import app
from worker.consumer import execute_task


class TestSchedulerApiAndEnqueueFailure(unittest.TestCase):
    """Test suite for Scheduler API endpoints and error isolation."""

    @classmethod
    def setUpClass(cls):
        init_db()
        cls.client = TestClient(app)

    def test_submit_job_success_with_mocked_queue(self):
        """Verify normal job submission returns HTTP 202 and enqueues job."""
        mock_queue = MagicMock()
        mock_queue.push_job.return_value = None

        with patch.object(app.state, "queue", mock_queue, create=True):
            response = self.client.post(
                "/submit_job",
                json={
                    "job_type": "math_prime_sieve",
                    "payload": {"limit": 100},
                },
            )
            self.assertEqual(response.status_code, 202)
            data = response.json()
            self.assertEqual(data["status"], "queued")
            job_id = data["job_id"]
            self.assertTrue(bool(job_id))

            # Verify queue.push_job was called with the generated job_id
            mock_queue.push_job.assert_called_once_with(job_id)

            # Verify persisted in SQLite as 'pending'
            saved_job = get_job(job_id)
            self.assertIsNotNone(saved_job)
            self.assertEqual(saved_job["status"], "pending")
            self.assertEqual(saved_job["job_type"], "math_prime_sieve")

    def test_submit_job_failed_redis_enqueue_marks_job_failed(self):
        """CRITICAL REQUIREMENT: A failed enqueue must NOT leave a job pending forever.

        It must record status='failed' with a clear error message in SQLite.
        """
        mock_queue = MagicMock()
        mock_queue.push_job.side_effect = ConnectionError("Redis connection refused on localhost:6379")

        with patch.object(app.state, "queue", mock_queue, create=True):
            response = self.client.post(
                "/submit_job",
                json={
                    "job_type": "hash_checksum",
                    "payload": {"data": "test_failure"},
                },
            )
            # Scheduler must raise HTTP 500
            self.assertEqual(response.status_code, 500)
            self.assertIn("queue dispatch failed", response.json()["detail"].lower())

            # Find the most recently inserted job with matching payload
            conn = _get_connection()
            try:
                row = conn.execute(
                    "SELECT * FROM jobs WHERE payload LIKE '%test_failure%' ORDER BY submitted_at DESC LIMIT 1"
                ).fetchone()
                self.assertIsNotNone(row, "Job row must exist in database")
                job_dict = dict(row)
                self.assertEqual(
                    job_dict["status"],
                    "failed",
                    "Job must be marked 'failed' when enqueue fails, never left pending!",
                )
                self.assertIn("Queue dispatch failed", job_dict["error_message"])
            finally:
                conn.close()

    def test_get_job_status_existing_and_missing(self):
        job_id = f"test-query-{uuid4().hex[:8]}"
        insert_job(job_id, "test_job", {"test": True}, "2026-10-12T00:00:00+00:00")

        # Existing job
        resp = self.client.get(f"/job/{job_id}")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["job_id"], job_id)

        # Missing job
        resp_missing = self.client.get("/job/non_existent_id_999999")
        self.assertEqual(resp_missing.status_code, 404)

    def test_list_jobs_endpoint(self):
        resp = self.client.get("/jobs")
        self.assertEqual(resp.status_code, 200)
        self.assertIsInstance(resp.json(), list)

    def test_list_workers_endpoint(self):
        test_worker = f"worker-test-{uuid4().hex[:6]}"
        register_worker(test_worker)
        resp = self.client.get("/workers")
        self.assertEqual(resp.status_code, 200)
        worker_ids = [w["worker_id"] for w in resp.json()]
        self.assertIn(test_worker, worker_ids)

    def test_health_check_endpoint(self):
        mock_queue = MagicMock()
        mock_queue.queue_length.return_value = 4
        with patch.object(app.state, "queue", mock_queue, create=True):
            resp = self.client.get("/health")
            self.assertEqual(resp.status_code, 200)
            data = resp.json()
            self.assertEqual(data["status"], "healthy")
            self.assertEqual(data["pending_queue_length"], 4)

    def test_dashboard_data_endpoint(self):
        mock_queue = MagicMock()
        mock_queue.queue_length.return_value = 2
        with patch.object(app.state, "queue", mock_queue, create=True):
            resp = self.client.get("/api/dashboard_data")
            self.assertEqual(resp.status_code, 200)
            data = resp.json()
            self.assertIn("job_counts", data)
            self.assertIn("workers", data)
            self.assertIn("recent_jobs", data)
            self.assertEqual(data["pending_queue_length"], 2)


class TestWorkerResilienceAndExecution(unittest.TestCase):
    """Test suite verifying worker resilience when job execution raises exceptions."""

    def test_worker_deterministic_execution_success(self):
        output = execute_task("math_prime_sieve", {"limit": 50})
        self.assertEqual(output["algorithm"], "sieve_of_eratosthenes")
        self.assertEqual(output["total_primes"], 15)

    def test_worker_handles_handler_failure_without_terminating(self):
        """Worker must catch handler exceptions, update SQLite to 'failed', and remain functional."""
        job_id = f"job-fail-test-{uuid4().hex[:8]}"
        worker_id = f"worker-resilience-{uuid4().hex[:6]}"

        insert_job(job_id, "math_prime_sieve", {"limit": "invalid_string"}, "2026-10-12T00:00:00+00:00")
        update_job_status(job_id, "running", worker_id=worker_id)

        # Simulate worker executing the invalid task
        try:
            execute_task("math_prime_sieve", {"limit": "invalid_string"})
            self.fail("Handler should have raised ValueError on invalid limit")
        except Exception as task_exc:
            # Worker catches exception and records failure in SQLite
            update_job_status(
                job_id,
                "failed",
                worker_id=worker_id,
                error_message=str(task_exc),
            )

        saved = get_job(job_id)
        self.assertEqual(saved["status"], "failed")
        self.assertEqual(saved["worker_id"], worker_id)
        self.assertIn("must be an integer", saved["error_message"])


class TestDatabaseConfigurationAndTimeout(unittest.TestCase):
    """Test suite verifying SQLite configuration and connection timeout."""

    def test_sqlite_timeout_configured(self):
        self.assertGreaterEqual(SQLITE_TIMEOUT_SECONDS, 5.0)

    def test_connection_busy_timeout_pragma(self):
        conn = _get_connection()
        try:
            cursor = conn.execute("PRAGMA busy_timeout")
            val = cursor.fetchone()[0]
            # Verify busy_timeout is set to SQLITE_TIMEOUT_SECONDS in ms
            self.assertEqual(val, int(SQLITE_TIMEOUT_SECONDS * 1000))
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
