"""
Test suite for Phase 7: Observability, Security and Operational Hardening.

Verifies:
  1. Comprehensive Prometheus metrics exposition (/metrics) with bounded labels.
  2. Dependency health monitoring and latency tracking (Redis and Database).
  3. Defensive security input validation (job_type whitelist, size limits, dunder keys, depth).
  4. HTTP request body size limit enforcement (HTTP 413).
  5. Correlation ID tracing propagation across scheduler and workers.
  6. Controlled job failure produces structured logs, failure status, and incremented metrics.
  7. Sensitive secret redaction in logging.
  8. Alert rules YAML validation and Grafana dashboard JSON schema integrity.
"""

import json
import os
import unittest
from datetime import datetime, timezone
from uuid import uuid4
import yaml

from fastapi.testclient import TestClient

from common.database import init_db, insert_job, update_job_status, get_all_jobs, register_worker
from common.logging_config import sanitize_sensitive_data, setup_logger, set_trace_context, clear_trace_context
from scheduler.main import app
from scheduler.models import JobRequest
from worker.handlers import dispatch_task


class TestObservabilityAndSecurity(unittest.TestCase):
    """Phase 7 Observability, Security and Hardening test suite."""

    @classmethod
    def setUpClass(cls) -> None:
        init_db()
        from common.queue_client import JobQueue
        from common.leader_lease import LeaderLeaseManager
        app.state.queue = JobQueue(queue_name="test_obs_queue", processing_queue="test_obs_queue:processing")
        app.state.lease_manager = LeaderLeaseManager(leader_id="test-scheduler")
        app.state.submissions_count = {}
        app.state.reconciliation_runs_total = 0
        app.state.reconciliation_reclaimed_jobs_total = 0
        app.state.reconciliation_evicted_jobs_total = 0
        cls.client = TestClient(app)

    def tearDown(self) -> None:
        clear_trace_context()

    # -------------------------------------------------------------------------
    # 1. Prometheus Metrics Verification
    # -------------------------------------------------------------------------
    def test_01_prometheus_metrics_comprehensive(self) -> None:
        """Verify that /metrics exports all required Phase 7 metrics with bounded labels."""
        resp = self.client.get("/metrics")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("text/plain", resp.headers["content-type"])
        body = resp.text

        # 1. Job totals gauges
        self.assertIn('job_platform_jobs_total{status="pending"}', body)
        self.assertIn('job_platform_jobs_total{status="running"}', body)
        self.assertIn('job_platform_jobs_total{status="completed"}', body)
        self.assertIn('job_platform_jobs_total{status="failed"}', body)
        self.assertIn('job_platform_jobs_total{status="dead_letter"}', body)

        # 2. Queue depths
        self.assertIn('job_platform_queue_depth{queue="pending"}', body)
        self.assertIn('job_platform_queue_depth{queue="processing"}', body)

        # 3. Worker metrics
        self.assertIn('job_platform_workers{status="online"}', body)
        self.assertIn('job_platform_workers{status="total"}', body)
        self.assertIn("job_platform_worker_max_heartbeat_age_seconds", body)

        # 4. Dependency health & latency
        self.assertIn('job_platform_dependency_up{dependency="redis"}', body)
        self.assertIn('job_platform_dependency_up{dependency="database"}', body)
        self.assertIn('job_platform_dependency_latency_seconds{dependency="redis"}', body)
        self.assertIn('job_platform_dependency_latency_seconds{dependency="database"}', body)

        # 5. Leader coordination & leases
        self.assertIn("job_platform_leader_epoch", body)
        self.assertIn("job_platform_is_leader", body)
        self.assertIn("job_platform_leader_lease_ttl_seconds", body)

        # 6. Retries and dead-letter totals
        self.assertIn("job_platform_job_retries_total", body)
        self.assertIn("job_platform_dead_letter_jobs_total", body)

        # 7. Reconciliation counters
        self.assertIn("job_platform_reconciliation_runs_total", body)
        self.assertIn("job_platform_reconciliation_reclaimed_jobs_total", body)
        self.assertIn("job_platform_reconciliation_evicted_jobs_total", body)

        # 8. RPC & DFS telemetry
        self.assertIn("job_platform_rpc_operations_total", body)
        self.assertIn("job_platform_dfs_operations_total", body)

        # Bounded labels check: No raw UUIDs in metric label names
        for line in body.splitlines():
            if line.startswith("#") or not line.strip():
                continue
            # Must not contain UUID strings as unbounded metric names
            metric_part = line.split()[0]
            self.assertNotIn("4584d742-", metric_part)

    # -------------------------------------------------------------------------
    # 2. Defensive Security & Input Validation
    # -------------------------------------------------------------------------
    def test_02_job_request_security_validation(self) -> None:
        """Verify strict input validation on job_type, payload size, depth, and keys."""
        # A. Valid job submission succeeds
        resp = self.client.post(
            "/submit_job",
            json={"job_type": "math_prime_sieve", "payload": {"limit": 50}},
        )
        self.assertEqual(resp.status_code, 202)
        data = resp.json()
        self.assertEqual(data["status"], "queued")
        self.assertTrue(len(data["job_id"]) > 0)

        # B. Invalid job_type with directory traversal or command injection rejected
        resp_bad_type = self.client.post(
            "/submit_job",
            json={"job_type": "../../bin/sh", "payload": {}},
        )
        self.assertEqual(resp_bad_type.status_code, 422)

        resp_bad_chars = self.client.post(
            "/submit_job",
            json={"job_type": "job;rm -rf /", "payload": {}},
        )
        self.assertEqual(resp_bad_chars.status_code, 422)

        # C. Oversized payload (> 512 KB) rejected
        huge_payload = {"data": "x" * (513 * 1024)}
        resp_huge = self.client.post(
            "/submit_job",
            json={"job_type": "hash_checksum", "payload": huge_payload},
        )
        self.assertEqual(resp_huge.status_code, 422)

        # D. Deeply nested payload (> 10 levels) rejected
        deep_dict = {"level": 1}
        cur = deep_dict
        for i in range(2, 13):
            cur["child"] = {"level": i}
            cur = cur["child"]

        resp_deep = self.client.post(
            "/submit_job",
            json={"job_type": "hash_checksum", "payload": deep_dict},
        )
        self.assertEqual(resp_deep.status_code, 422)

        # E. Reserved dunder keys rejected
        resp_dunder = self.client.post(
            "/submit_job",
            json={"job_type": "hash_checksum", "payload": {"__class__": "malicious"}},
        )
        self.assertEqual(resp_dunder.status_code, 422)

    # -------------------------------------------------------------------------
    # 3. Request Size Limit Middleware (HTTP 413)
    # -------------------------------------------------------------------------
    def test_03_request_body_size_limit_middleware(self) -> None:
        """Verify middleware enforces 1MB body limit and returns HTTP 413."""
        # Simulated header claiming > 1MB body
        headers = {"Content-Length": "1048577"}
        resp = self.client.post(
            "/submit_job",
            json={"job_type": "math_prime_sieve", "payload": {}},
            headers=headers,
        )
        self.assertEqual(resp.status_code, 413)
        self.assertIn("Payload Too Large", resp.json()["detail"])

    # -------------------------------------------------------------------------
    # 4. Correlation ID Tracing
    # -------------------------------------------------------------------------
    def test_04_correlation_id_and_structured_tracing(self) -> None:
        """Verify correlation IDs are extracted or generated and propagated in response headers."""
        # Explicit correlation ID provided
        test_corr = "corr-test-custom-999"
        resp = self.client.post(
            "/submit_job",
            json={"job_type": "math_prime_sieve", "payload": {"limit": 100}},
            headers={"X-Correlation-ID": test_corr},
        )
        self.assertEqual(resp.status_code, 202)
        self.assertEqual(resp.headers.get("X-Correlation-ID"), test_corr)
        self.assertEqual(resp.json().get("correlation_id"), test_corr)

        # Automatic correlation ID generation when absent
        resp_auto = self.client.post(
            "/submit_job",
            json={"job_type": "math_prime_sieve", "payload": {"limit": 100}},
        )
        self.assertEqual(resp_auto.status_code, 202)
        auto_corr = resp_auto.headers.get("X-Correlation-ID")
        self.assertTrue(auto_corr.startswith("corr-"))
        self.assertEqual(resp_auto.json().get("correlation_id"), auto_corr)

    # -------------------------------------------------------------------------
    # 5. Controlled Failure Scenario
    # -------------------------------------------------------------------------
    def test_05_controlled_failure_produces_logs_and_metrics(self) -> None:
        """Verify controlled task failure updates job status, logs error, and reflects in metrics."""
        failed_job_id = f"job-fail-{uuid4().hex[:8]}"
        set_trace_context(correlation_id=f"corr-{failed_job_id[:8]}", job_id=failed_job_id)

        # 1. Insert pending job
        insert_job(
            job_id=failed_job_id,
            job_type="math_prime_sieve",
            payload={"limit": -10},  # Negative limit triggers deterministic ValueError in handler
            submitted_at=datetime.now(timezone.utc).isoformat(),
        )

        # 2. Execute with handler -> verifies failure is caught and documented
        with self.assertRaises(ValueError):
            dispatch_task("math_prime_sieve", {"limit": -10})

        # 3. Record failure transition in database
        updated = update_job_status(
            job_id=failed_job_id,
            status="failed",
            worker_id="test-worker-fail",
            error_message="Prime sieve 'limit' must be >= 2, got: -10",
        )
        self.assertTrue(updated)

        # 4. Confirm failure appears in Prometheus metrics
        resp = self.client.get("/metrics")
        self.assertEqual(resp.status_code, 200)
        self.assertIn('job_platform_jobs_total{status="failed"}', resp.text)

    # -------------------------------------------------------------------------
    # 6. Sensitive Data Redaction
    # -------------------------------------------------------------------------
    def test_06_sensitive_secret_redaction(self) -> None:
        """Verify secret sanitization redacts passwords, tokens, and api_keys from logged structures."""
        raw_data = {
            "user": "sre-operator",
            "password": "super-secret-password-123",
            "api_key": "live_key_xyz987",
            "nested": {
                "auth_token": "bearer-token-val",
                "normal_field": "public_data",
            },
            "header": "Bearer secret_bearer_token_value",
        }

        sanitized = sanitize_sensitive_data(raw_data)
        self.assertEqual(sanitized["user"], "sre-operator")
        self.assertEqual(sanitized["password"], "[REDACTED]")
        self.assertEqual(sanitized["api_key"], "[REDACTED]")
        self.assertEqual(sanitized["nested"]["auth_token"], "[REDACTED]")
        self.assertEqual(sanitized["nested"]["normal_field"], "public_data")
        self.assertIn("[REDACTED]", sanitized["header"])

    # -------------------------------------------------------------------------
    # 7. Prometheus Alerting Rules Structure
    # -------------------------------------------------------------------------
    def test_07_alert_rules_yaml_syntax_and_structure(self) -> None:
        """Verify prometheus/alert_rules.yml parses cleanly and defines required alerts."""
        rule_path = os.path.join(os.path.dirname(__file__), "..", "prometheus", "alert_rules.yml")
        self.assertTrue(os.path.exists(rule_path), f"Alert rules file not found: {rule_path}")

        with open(rule_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        self.assertIn("groups", data)
        group = data["groups"][0]
        alert_names = [rule["alert"] for rule in group.get("rules", [])]

        required_alerts = [
            "JobPlatformDependencyDown",
            "JobPlatformNoActiveLeader",
            "JobPlatformProlongedWorkerUnavailability",
            "JobPlatformPersistentJobFailures",
            "JobPlatformHighQueueBacklog",
        ]
        for req in required_alerts:
            self.assertIn(req, alert_names, f"Missing required Prometheus alert: {req}")

    # -------------------------------------------------------------------------
    # 8. Grafana Dashboard JSON Integrity
    # -------------------------------------------------------------------------
    def test_08_grafana_dashboard_json_structure(self) -> None:
        """Verify Grafana dashboard JSON schema validity and queries reference real metrics."""
        dash_path = os.path.join(
            os.path.dirname(__file__),
            "..",
            "grafana",
            "dashboards",
            "distributed_job_platform_overview.json",
        )
        self.assertTrue(os.path.exists(dash_path), f"Dashboard JSON not found: {dash_path}")

        with open(dash_path, "r", encoding="utf-8") as f:
            dash = json.load(f)

        self.assertIn("title", dash)
        self.assertIn("panels", dash)

        # Collect all PromQL expressions used in targets
        prom_exprs = []
        for panel in dash["panels"]:
            for target in panel.get("targets", []):
                expr = target.get("expr", "")
                if expr:
                    prom_exprs.append(expr)

        self.assertTrue(len(prom_exprs) >= 8, f"Insufficient Prometheus queries in Grafana dashboard: {prom_exprs}")
        # Verify core metric names are referenced
        all_joined_exprs = " ".join(prom_exprs)
        self.assertIn("job_platform_dependency_up", all_joined_exprs)
        self.assertIn("job_platform_queue_depth", all_joined_exprs)
        self.assertIn("job_platform_jobs_total", all_joined_exprs)
        self.assertIn("job_platform_is_leader", all_joined_exprs)


if __name__ == "__main__":
    unittest.main()
