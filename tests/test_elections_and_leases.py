"""
Unit and Integration Tests for Unit III: Leader Elections and Leader Leases.

Covers:
1. Garcia-Molina Bully Election:
   - Highest available priority winning.
   - Lower-priority process initiating election.
   - Higher-priority process failure during election.
   - Recovered high-priority process reclaiming leadership.
2. Chang-Roberts Logical Ring Election:
   - Message circulation and highest priority candidate winning.
   - Dead/failed participant bypass without hanging.
   - Multiple failures in ring.
3. Redis-backed Leader Lease Manager:
   - Lease acquisition, TTL expiration, and renewal.
   - Handover between two scheduler instances with monotonic fencing epoch increment.
4. Persistent Fencing Epoch Protection (SQLite):
   - Stale leader rejected when writing with an older epoch (StaleLeaderError).
   - Authoritative stale job recovery rejection for stale leaders.
   - Concurrent scheduler recovery attempts.
5. Worker Processing Independence:
   - Workers continue claiming and executing tasks independently of scheduler leadership.
6. API Verification:
   - GET /api/leader and GET /api/elections.
"""

import json
import os
import unittest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch
from uuid import uuid4

import redis
from fastapi.testclient import TestClient

from common.config import DB_PATH
from common.database import (
    init_db,
    insert_job,
    get_job,
    update_job_status,
    claim_job_for_worker,
    find_and_recover_stale_jobs,
    reconcile_processing_queue,
    validate_and_record_leader_epoch,
    get_current_fencing_epoch,
    register_worker,
    update_heartbeat,
    mark_stale_workers_offline,
    StaleLeaderError,
    _get_connection,
)
from common.election_bully import BullyCluster, BullyProcess
from common.election_ring import RingCluster, RingProcess
from common.leader_lease import LeaderLeaseManager
from common.queue_client import JobQueue
from scheduler.main import app
from worker.consumer import execute_task


class SimulatedRedisLeaseStore:
    """Simulates Redis keyspace and atomic Lua scripts used by LeaderLeaseManager."""

    def __init__(self):
        self.keys = {}

    def eval(self, script, numkeys, *args):
        lease_key = args[0]
        if "local current_epoch = tonumber(redis.call('GET', KEYS[2]))" in script:
            # try_acquire_lease Lua script
            epoch_key = args[1]
            leader_id = str(args[2])
            ttl = int(args[3])
            min_epoch = int(args[4]) if len(args) > 4 else 0

            current = self.keys.get(lease_key)
            if not current:
                current_epoch = int(self.keys.get(epoch_key, 0))
                if current_epoch < min_epoch:
                    self.keys[epoch_key] = min_epoch
                self.keys[epoch_key] = int(self.keys.get(epoch_key, 0)) + 1
                new_epoch = self.keys[epoch_key]
                payload = json.dumps({"leader_id": leader_id, "epoch": new_epoch})
                self.keys[lease_key] = payload
                return [1, new_epoch]
            else:
                data = json.loads(current)
                if data["leader_id"] == leader_id:
                    return [1, data["epoch"]]
                else:
                    return [0, data["epoch"]]

        elif "if data.leader_id == ARGV[1] and data.epoch == tonumber(ARGV[2]) then" in script and "EXPIRE" in script:
            # renew_lease Lua script
            leader_id = str(args[1])
            epoch = int(args[2])
            current = self.keys.get(lease_key)
            if current:
                data = json.loads(current)
                if data["leader_id"] == leader_id and int(data["epoch"]) == epoch:
                    return 1
            return 0

        elif "if data.leader_id == ARGV[1] and data.epoch == tonumber(ARGV[2]) then" in script and "DEL" in script:
            # release_lease Lua script
            leader_id = str(args[1])
            epoch = int(args[2])
            current = self.keys.get(lease_key)
            if current:
                data = json.loads(current)
                if data["leader_id"] == leader_id and int(data["epoch"]) == epoch:
                    del self.keys[lease_key]
                    return 1
            return 0

        return 0


class TestBullyElection(unittest.TestCase):
    """Unit tests for Garcia-Molina Bully Election module."""

    def test_highest_priority_wins(self):
        """When election is triggered, highest priority available process becomes coordinator."""
        cluster = BullyCluster()
        cluster.add_process("P1", priority=10)
        cluster.add_process("P2", priority=20)
        cluster.add_process("P3", priority=30)
        cluster.add_process("P4", priority=40)

        p1 = cluster.get_process("P1")
        p1.start_election()

        coordinator = cluster.get_active_coordinator()
        self.assertEqual(coordinator, "P4")

    def test_lower_priority_initiates_election(self):
        """A low-priority process (P2) can initiate election and higher-priority processes respond."""
        cluster = BullyCluster()
        cluster.add_process("P1", priority=10)
        cluster.add_process("P2", priority=20)
        cluster.add_process("P3", priority=30)

        p2 = cluster.get_process("P2")
        p2.start_election()

        coordinator = cluster.get_active_coordinator()
        self.assertEqual(coordinator, "P3")

    def test_highest_process_fails_mid_election(self):
        """If highest process crashes, the next highest available process wins."""
        cluster = BullyCluster()
        cluster.add_process("P1", priority=10)
        cluster.add_process("P2", priority=20)
        cluster.add_process("P3", priority=30)
        p4 = cluster.add_process("P4", priority=40)

        # P4 crashes
        p4.fail()

        p2 = cluster.get_process("P2")
        p2.start_election()

        coordinator = cluster.get_active_coordinator()
        self.assertEqual(coordinator, "P3")
        self.assertFalse(p4.is_alive)

    def test_recovered_highest_process_bullies_leadership(self):
        """When highest priority process recovers, it triggers an election and reclaims coordinator."""
        cluster = BullyCluster()
        cluster.add_process("P1", priority=10)
        cluster.add_process("P2", priority=20)
        cluster.add_process("P3", priority=30)
        p4 = cluster.add_process("P4", priority=40)

        p4.fail()
        p1 = cluster.get_process("P1")
        p1.start_election()
        self.assertEqual(cluster.get_active_coordinator(), "P3")

        # P4 recovers and bullies
        p4.recover()
        p4.start_election()
        self.assertEqual(cluster.get_active_coordinator(), "P4")


class TestRingElection(unittest.TestCase):
    """Unit tests for Chang-Roberts Logical Ring Election module."""

    def test_ring_circulation_and_highest_candidate_selected(self):
        """Candidate IDs circulate around ring and highest priority wins."""
        ring = RingCluster()
        ring.add_process("P1", priority=10)
        ring.add_process("P2", priority=20)
        ring.add_process("P3", priority=30)
        ring.add_process("P4", priority=40)

        p1 = ring.get_process("P1")
        elected = p1.start_election()

        self.assertEqual(elected, "P4")
        self.assertEqual(ring.get_active_coordinator(), "P4")

    def test_dead_participant_bypassed_in_ring(self):
        """When an intermediate node crashes, messages bypass it to the next live successor."""
        ring = RingCluster()
        ring.add_process("P1", priority=10)
        ring.add_process("P2", priority=20)
        ring.add_process("P3", priority=30)
        p4 = ring.add_process("P4", priority=40)

        # P4 crashes
        p4.fail()

        p2 = ring.get_process("P2")
        elected = p2.start_election()

        self.assertEqual(elected, "P3")
        self.assertEqual(ring.get_active_coordinator(), "P3")

    def test_multiple_failures_in_ring(self):
        """Ring functions properly when multiple nodes fail as long as at least 1 remains."""
        ring = RingCluster()
        ring.add_process("P1", priority=10)
        ring.add_process("P2", priority=20)
        ring.add_process("P3", priority=30)
        p4 = ring.add_process("P4", priority=40)
        p5 = ring.add_process("P5", priority=50)

        p4.fail()
        p5.fail()

        p1 = ring.get_process("P1")
        elected = p1.start_election()

        self.assertEqual(elected, "P3")
        self.assertEqual(ring.get_active_coordinator(), "P3")


class TestLeaderLeaseAndFencing(unittest.TestCase):
    """Tests for Redis-backed Leader Leases and SQLite Fencing Epoch validation."""

    def setUp(self):
        init_db()
        conn = _get_connection()
        try:
            conn.execute("DELETE FROM jobs")
            conn.execute("DELETE FROM workers")
            conn.execute("DELETE FROM leader_fencing")
            conn.commit()
        finally:
            conn.close()

    def test_lease_acquisition_and_renewal_with_mock_redis(self):
        """Verify leader lease acquisition and renewal Lua script execution."""
        mock_redis = MagicMock()
        mock_redis.eval.side_effect = [
            [1, 1],  # acquire
            1,       # renew
            1,       # release
        ]

        manager = LeaderLeaseManager(leader_id="scheduler-A", redis_client=mock_redis)
        acquired, epoch = manager.try_acquire_lease()
        self.assertTrue(acquired)
        self.assertEqual(epoch, 1)
        self.assertTrue(manager.is_leader)

        renewed = manager.renew_lease()
        self.assertTrue(renewed)

        released = manager.release_lease()
        self.assertTrue(released)
        self.assertFalse(manager.is_leader)

    def test_stale_leader_fencing_epoch_rejection(self):
        """Verify that SQLite persistent layer rejects a stale leader attempting writes."""
        conn = _get_connection()
        try:
            # Newer leader has established epoch 5
            validate_and_record_leader_epoch(
                conn,
                leader_id="scheduler-B",
                epoch=5,
                scope="scheduler_recovery",
            )
            conn.commit()

            # Stale leader wakes up and attempts write with epoch 4
            with self.assertRaises(StaleLeaderError) as ctx:
                validate_and_record_leader_epoch(
                    conn,
                    leader_id="scheduler-A",
                    epoch=4,
                    scope="scheduler_recovery",
                )
            self.assertIn("Fencing violation", str(ctx.exception))
            self.assertIn("superseded by durable epoch 5", str(ctx.exception))
        finally:
            conn.close()

    def test_stale_leader_rejected_during_recovery_sweep(self):
        """find_and_recover_stale_jobs raises StaleLeaderError if leader epoch is stale."""
        conn = _get_connection()
        try:
            validate_and_record_leader_epoch(
                conn,
                leader_id="scheduler-new",
                epoch=10,
                scope="scheduler_recovery",
            )
            conn.commit()
        finally:
            conn.close()

        # Zombie scheduler with epoch 9 attempts recovery
        with self.assertRaises(StaleLeaderError):
            find_and_recover_stale_jobs(
                lease_timeout_seconds=30.0,
                lamport_ts=100,
                leader_id="scheduler-old",
                leader_epoch=9,
            )

    def test_valid_leader_succeeds_in_recovery_sweep(self):
        """A leader with epoch >= current durable epoch succeeds in recovery sweep."""
        job_id = str(uuid4())
        insert_job(job_id=job_id, job_type="email", payload={}, submitted_at=datetime.now(timezone.utc).isoformat())
        claim_job_for_worker(job_id=job_id, worker_id="worker-dead", lease_seconds=-1.0)

        # Leader with epoch 15 recovers the stale job
        recovered = find_and_recover_stale_jobs(
            lease_timeout_seconds=0.001,
            lamport_ts=101,
            leader_id="scheduler-valid",
            leader_epoch=15,
        )
        self.assertEqual(len(recovered), 1)
        self.assertEqual(recovered[0]["job_id"], job_id)

        # Verify epoch was recorded in durable storage
        durable = get_current_fencing_epoch("scheduler_recovery")
        self.assertIsNotNone(durable)
        self.assertEqual(durable["epoch"], 15)
        self.assertEqual(durable["leader_id"], "scheduler-valid")

    def test_two_schedulers_attempting_recovery_concurrently(self):
        """When two schedulers run recovery, higher epoch establishes authority and lower is rejected."""
        # Scheduler 2 with Epoch 20 runs
        find_and_recover_stale_jobs(
            lease_timeout_seconds=30.0,
            lamport_ts=1,
            leader_id="scheduler-2",
            leader_epoch=20,
        )

        # Delayed Scheduler 1 with Epoch 19 arrives later
        with self.assertRaises(StaleLeaderError):
            find_and_recover_stale_jobs(
                lease_timeout_seconds=30.0,
                lamport_ts=2,
                leader_id="scheduler-1",
                leader_epoch=19,
            )

    def test_interleaved_stale_leader_cannot_overwrite_newer_recovery(self):
        """Interleaving test: Scheduler A (epoch 1) is paused; Scheduler B (epoch 2) runs recovery.
        When Scheduler A resumes, its recovery is rejected and cannot overwrite durable state."""
        job_id = str(uuid4())
        insert_job(job_id=job_id, job_type="email", payload={"to": "a@test.com"}, submitted_at=datetime.now(timezone.utc).isoformat())
        claim_job_for_worker(job_id=job_id, worker_id="worker-paused", lease_seconds=-1.0)

        # Scheduler A has epoch 1.
        # Scheduler A starts, but pauses before completing recovery.
        # Scheduler B acquires epoch 2 and performs authoritative recovery.
        rec_b = find_and_recover_stale_jobs(
            lease_timeout_seconds=0.001,
            lamport_ts=10,
            leader_id="scheduler-B",
            leader_epoch=2,
        )
        self.assertEqual(len(rec_b), 1)
        self.assertEqual(rec_b[0]["job_id"], job_id)
        self.assertEqual(rec_b[0]["action"], "requeued")

        # Verify durable epoch in SQLite is now 2
        durable = get_current_fencing_epoch("scheduler_recovery")
        self.assertEqual(durable["epoch"], 2)
        self.assertEqual(durable["leader_id"], "scheduler-B")

        # Scheduler A resumes and attempts recovery with stale epoch 1
        with self.assertRaises(StaleLeaderError) as ctx:
            find_and_recover_stale_jobs(
                lease_timeout_seconds=0.001,
                lamport_ts=11,
                leader_id="scheduler-A",
                leader_epoch=1,
            )
        self.assertIn("Fencing violation", str(ctx.exception))

        # Durable state remains owned by Scheduler B (epoch 2)
        durable_after = get_current_fencing_epoch("scheduler_recovery")
        self.assertEqual(durable_after["epoch"], 2)
        self.assertEqual(durable_after["leader_id"], "scheduler-B")

    def test_stale_worker_demotion_rejected_after_valid_heartbeat(self):
        """A worker must NOT be marked offline if it sent a valid heartbeat before demotion."""
        worker_id = f"worker-hb-{uuid4().hex[:6]}"
        register_worker(worker_id)

        # Manually backdate worker heartbeat to 30 seconds ago (appear stale)
        past_iso = (datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat()
        conn = _get_connection()
        try:
            conn.execute("UPDATE workers SET last_heartbeat = ? WHERE worker_id = ?", (past_iso, worker_id))
            conn.commit()
        finally:
            conn.close()

        # Worker sends a fresh heartbeat right before leader demotes
        update_heartbeat(worker_id)

        # Stale checker runs with 15s timeout
        newly_offline = mark_stale_workers_offline(
            timeout_seconds=15.0,
            leader_id="scheduler-leader",
            leader_epoch=1,
        )
        self.assertNotIn(worker_id, newly_offline)

        # Verify worker is still online in SQLite
        conn = _get_connection()
        try:
            row = conn.execute("SELECT status FROM workers WHERE worker_id = ?", (worker_id,)).fetchone()
            self.assertEqual(row["status"], "online")
        finally:
            conn.close()

    def test_stale_worker_demotion_rejected_after_newer_leader_operation(self):
        """A worker must NOT be marked offline by an outdated leader after newer leader takes authority."""
        worker_id = f"worker-stale-{uuid4().hex[:6]}"
        register_worker(worker_id)

        past_iso = (datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat()
        conn = _get_connection()
        try:
            conn.execute("UPDATE workers SET last_heartbeat = ? WHERE worker_id = ?", (past_iso, worker_id))
            conn.commit()
        finally:
            conn.close()

        # Newer Leader B establishes epoch 2
        conn = _get_connection()
        try:
            validate_and_record_leader_epoch(conn, leader_id="scheduler-B", epoch=2, scope="scheduler_recovery")
        finally:
            conn.close()

        # Outdated Leader A (epoch 1) attempts to demote stale worker
        with self.assertRaises(StaleLeaderError):
            mark_stale_workers_offline(
                timeout_seconds=15.0,
                leader_id="scheduler-A",
                leader_epoch=1,
            )

        # Verify worker status was not altered by outdated leader
        conn = _get_connection()
        try:
            row = conn.execute("SELECT status FROM workers WHERE worker_id = ?", (worker_id,)).fetchone()
            self.assertEqual(row["status"], "online")
        finally:
            conn.close()

    def test_redis_restart_epoch_recovery_from_sqlite_floor(self):
        """Simulate established SQLite epoch, wiped Redis counter, and verify new lease acquires >= min_epoch + 1."""
        # 1. Establish SQLite durable epoch 25
        conn = _get_connection()
        try:
            validate_and_record_leader_epoch(conn, leader_id="prev-leader", epoch=25, scope="scheduler_recovery")
        finally:
            conn.close()

        # 2. Redis restart simulated (empty Redis store)
        sim_redis = SimulatedRedisLeaseStore()

        # 3. Scheduler 1 acquires lease
        mgr1 = LeaderLeaseManager(leader_id="scheduler-1", redis_client=sim_redis)
        acquired, epoch = mgr1.try_acquire_lease()

        self.assertTrue(acquired)
        self.assertEqual(epoch, 26, "Epoch after Redis restart must advance from durable SQLite floor (25 -> 26)")
        self.assertTrue(mgr1.is_leader)

        # 4. Concurrent Scheduler 2 attempting acquisition while Scheduler 1 holds lease
        mgr2 = LeaderLeaseManager(leader_id="scheduler-2", redis_client=sim_redis)
        acquired2, epoch2 = mgr2.try_acquire_lease()
        self.assertFalse(acquired2)
        self.assertEqual(epoch2, 26)
        self.assertFalse(mgr2.is_leader)

        # 5. Old leader with epoch 20 cannot renew or release Scheduler 1's lease
        mgr_old = LeaderLeaseManager(leader_id="scheduler-old", redis_client=sim_redis)
        mgr_old.force_leadership(epoch=20)
        self.assertFalse(mgr_old.renew_lease())
        self.assertFalse(mgr_old.release_lease())

        # Verify Scheduler 1 can successfully renew its lease
        self.assertTrue(mgr1.renew_lease())

    def test_lease_loss_behavior_on_failure_and_exception(self):
        """Verify that renewal failure or connection exception transitions is_leader to False."""
        mock_redis = MagicMock()
        mock_redis.eval.side_effect = [
            [1, 5],  # acquire succeeds with epoch 5
            0,       # renewal returns failure (0)
        ]

        mgr = LeaderLeaseManager(leader_id="sched-test", redis_client=mock_redis)
        acquired, epoch = mgr.try_acquire_lease()
        self.assertTrue(acquired)
        self.assertTrue(mgr.is_leader)
        self.assertEqual(epoch, 5)

        # Renewal fails
        renewed = mgr.renew_lease()
        self.assertFalse(renewed)
        self.assertFalse(mgr.is_leader, "Instance must step down immediately when renewal fails")

        # Test connection exception during renewal
        mock_redis.eval.side_effect = [
            [1, 6],                                 # acquire
            redis.ConnectionError("Redis down"),    # renewal raises ConnectionError
        ]
        acquired2, _ = mgr.try_acquire_lease()
        self.assertTrue(acquired2)
        self.assertTrue(mgr.is_leader)

        renewed2 = mgr.renew_lease()
        self.assertFalse(renewed2)
        self.assertFalse(mgr.is_leader, "Instance must step down immediately on connection error")


class TestWorkerLeadershipIndependence(unittest.TestCase):
    """Verify that worker nodes continue processing jobs independently of scheduler leadership."""

    def setUp(self):
        init_db()

    def test_worker_processes_job_regardless_of_scheduler_leadership(self):
        """Worker claims and commits job results directly, unaffected by scheduler leadership status."""
        job_id = str(uuid4())
        insert_job(
            job_id=job_id,
            job_type="hash_checksum",
            payload={"data": "test_decoupling", "algorithm": "sha256"},
            submitted_at=datetime.now(timezone.utc).isoformat(),
        )

        # Worker claims job
        claimed = claim_job_for_worker(job_id=job_id, worker_id="worker-indep", lease_seconds=30.0)
        self.assertIsNotNone(claimed)
        token = claimed["claim_token"]

        # Worker executes task independently
        res = execute_task("hash_checksum", {"data": "test_decoupling", "algorithm": "sha256"})
        self.assertIn("checksum", res)

        # Worker updates status with claim token
        ok = update_job_status(
            job_id=job_id,
            status="completed",
            result=res,
            claim_token=token,
        )
        self.assertTrue(ok)
        j = get_job(job_id)
        self.assertEqual(j["status"], "completed")


class TestElectionAndLeaderApi(unittest.TestCase):
    """Test API endpoints for leader lease diagnostics and election metadata."""

    @classmethod
    def setUpClass(cls):
        init_db()
        cls.client = TestClient(app)

    def test_api_leader_endpoint(self):
        """GET /api/leader returns lease diagnostics."""
        res = self.client.get("/api/leader")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertIn("is_leader", data)
        self.assertIn("epoch", data)
        self.assertIn("local_instance_id", data)

    def test_api_elections_endpoint(self):
        """GET /api/elections returns syllabus algorithms and cluster status."""
        res = self.client.get("/api/elections")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertIn("active_consensus", data)
        self.assertIn("algorithms", data)
        self.assertIn("bully", data["algorithms"])
        self.assertIn("ring", data["algorithms"])
        self.assertIn("mutual_exclusion", data["algorithms"])
        self.assertEqual(data["algorithms"]["bully"]["message_complexity"], "O(N^2)")

    def test_api_dashboard_data_includes_leader(self):
        """GET /api/dashboard_data payload includes leader diagnostics."""
        res = self.client.get("/api/dashboard_data")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertIn("leader", data)
        self.assertIn("job_counts", data)
        self.assertIn("dead_letter", data["job_counts"])
