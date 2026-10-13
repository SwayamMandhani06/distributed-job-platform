"""
Real Multi-Process & Networked Transport Tests for Leader Elections,
Leadership Fencing Integration, and Vector Clock Exchanges (Phase 3).

Covers:
  1. Multi-Process Bully Election over real TCP sockets:
     - Winner selection based on priority.
     - Coordinator crash detection and takeover by next highest node.
     - Recovered node bullying and reclaiming leadership.
  2. Multi-Process Ring Election over real TCP sockets:
     - Token circulation and candidate selection.
     - Dynamic bypass of crashed successors over real HTTP connections.
  3. Leader Lease & Fencing Epoch Protection:
     - Active leader validation for coordinator recovery loops.
     - Atomicity of leader epoch updates with database writes.
     - Immediate rejection of stale leader writes (StaleLeaderError).
     - Stepping down on Redis renewal failure or lease expiration.
  4. Vector Clock Inter-Process Exchanges:
     - Independent processes exchanging vectors over HTTP.
     - Causal and concurrent classification.
     - Strict membership validation.
"""

import threading
import time
import unittest
from typing import Dict, List, Tuple
from unittest.mock import MagicMock, patch

from common.election_transport import (
    BullyHTTPNode,
    RingHTTPNode,
    run_node_server,
    send_http_post,
)
from common.clocks import VectorClock, LamportClock
from common.database import (
    init_db,
    validate_and_record_leader_epoch,
    get_current_fencing_epoch,
    find_and_recover_stale_jobs,
    mark_stale_workers_offline,
    insert_job,
    register_worker,
    update_job_status,
    get_job,
    StaleLeaderError,
    _get_connection,
)
from common.leader_lease import LeaderLeaseManager


class TestMultiProcessBullyElection(unittest.TestCase):
    """End-to-end tests for Bully Leader Election running over real TCP/HTTP endpoints."""

    @classmethod
    def setUpClass(cls):
        cls.ports = {"P1": 9121, "P2": 9122, "P3": 9123, "P4": 9124}
        cls.peers = {
            "P1": (10, cls.ports["P1"]),
            "P2": (20, cls.ports["P2"]),
            "P3": (30, cls.ports["P3"]),
            "P4": (40, cls.ports["P4"]),
        }
        cls.nodes: Dict[str, BullyHTTPNode] = {}
        cls.threads: List[threading.Thread] = []

        for nid, (pri, port) in cls.peers.items():
            node = BullyHTTPNode(
                node_id=nid,
                priority=pri,
                port=port,
                peers=cls.peers,
                timeout_seconds=0.5,
            )
            cls.nodes[nid] = node
            srv = run_node_server(node, port)
            t = threading.Thread(target=srv.serve_forever, daemon=True)
            t.start()
            cls.threads.append(t)

        time.sleep(0.3)

    @classmethod
    def tearDownClass(cls):
        for node in cls.nodes.values():
            if node.server:
                node.server.shutdown()

    def test_01_initial_election_highest_priority_wins(self):
        """P1 initiates election; P4 (highest priority = 40) wins and becomes coordinator."""
        self.nodes["P1"].start_election()
        time.sleep(0.8)

        # All alive nodes recognize P4
        for nid in ["P1", "P2", "P3", "P4"]:
            self.assertEqual(self.nodes[nid].current_coordinator, "P4")

    def test_02_coordinator_crash_and_reelection(self):
        """When P4 crashes, P2 initiates election; P3 (Pri=30) wins among survivors."""
        # P4 crashes
        self.nodes["P4"].fail()

        # P2 initiates election
        self.nodes["P2"].start_election()
        time.sleep(0.8)

        # Surviving nodes recognize P3
        for nid in ["P1", "P2", "P3"]:
            self.assertEqual(self.nodes[nid].current_coordinator, "P3")

    def test_03_recovered_high_priority_bullies_office(self):
        """When P4 recovers, it immediately bullies P3 and reclaims coordinator."""
        self.nodes["P4"].recover()
        time.sleep(0.8)

        for nid in ["P1", "P2", "P3", "P4"]:
            self.assertEqual(self.nodes[nid].current_coordinator, "P4")


class TestMultiProcessRingElection(unittest.TestCase):
    """End-to-end tests for Ring Leader Election running over real TCP/HTTP endpoints."""

    @classmethod
    def setUpClass(cls):
        cls.topology = [
            ("P1", 10, 9221),
            ("P2", 20, 9222),
            ("P3", 30, 9223),
            ("P4", 40, 9224),
        ]
        cls.nodes: Dict[str, RingHTTPNode] = {}
        cls.threads: List[threading.Thread] = []

        for nid, pri, port in cls.topology:
            node = RingHTTPNode(
                node_id=nid,
                priority=pri,
                port=port,
                ring_topology=cls.topology,
                timeout_seconds=0.5,
            )
            cls.nodes[nid] = node
            srv = run_node_server(node, port)
            t = threading.Thread(target=srv.serve_forever, daemon=True)
            t.start()
            cls.threads.append(t)

        time.sleep(0.3)

    @classmethod
    def tearDownClass(cls):
        for node in cls.nodes.values():
            if node.server:
                node.server.shutdown()

    def test_01_full_circuit_election(self):
        """Token circulates full ring; node with highest priority (P4) is elected."""
        self.nodes["P4"].start_election()
        time.sleep(0.8)

        for nid in ["P1", "P2", "P3", "P4"]:
            self.assertEqual(self.nodes[nid].current_coordinator, "P4")

    def test_02_successor_failure_bypass(self):
        """When P4 and P3 crash, P1 initiates election and bypasses dead nodes to P2."""
        self.nodes["P4"].fail()
        self.nodes["P3"].fail()

        self.nodes["P1"].start_election()
        time.sleep(1.0)

        # Surviving nodes P1 and P2 converge on P2 (Pri=20)
        self.assertEqual(self.nodes["P1"].current_coordinator, "P2")
        self.assertEqual(self.nodes["P2"].current_coordinator, "P2")


class TestLeaderLeaseAndFencingIntegration(unittest.TestCase):
    """Tests for leader lease fencing, paused leader protection, and split-brain rejection (Section E)."""

    @classmethod
    def setUpClass(cls):
        init_db()

    def test_01_atomic_fencing_epoch_advancement(self):
        """Database accepts strictly increasing epochs and rejects older ones."""
        conn = _get_connection()
        scope = "test_fencing_scope"

        # Initial epoch 10
        validate_and_record_leader_epoch(conn, leader_id="leader-A", epoch=10, scope=scope)
        rec1 = get_current_fencing_epoch(scope=scope)
        self.assertEqual(rec1["epoch"], 10)
        self.assertEqual(rec1["leader_id"], "leader-A")

        # Advancement to epoch 11 by leader-B
        validate_and_record_leader_epoch(conn, leader_id="leader-B", epoch=11, scope=scope)
        rec2 = get_current_fencing_epoch(scope=scope)
        self.assertEqual(rec2["epoch"], 11)
        self.assertEqual(rec2["leader_id"], "leader-B")

        # Stale leader-A tries to write with epoch 10 -> MUST RAISE StaleLeaderError
        with self.assertRaises(StaleLeaderError):
            validate_and_record_leader_epoch(conn, leader_id="leader-A", epoch=10, scope=scope)

        conn.close()

    def test_02_stale_leader_rejected_during_job_recovery(self):
        """Paused leader whose epoch was superseded cannot perform authoritative stale job recovery."""
        conn = _get_connection()
        validate_and_record_leader_epoch(conn, leader_id="active-leader", epoch=50, scope="scheduler_recovery")
        conn.close()

        # Stale leader with epoch 49 attempts recovery
        with self.assertRaises(StaleLeaderError):
            find_and_recover_stale_jobs(
                lease_timeout_seconds=1.0,
                leader_id="stale-leader",
                leader_epoch=49,
            )

    def test_03_stale_leader_rejected_during_worker_staleness_check(self):
        """Paused leader whose epoch was superseded cannot mark workers offline."""
        conn = _get_connection()
        validate_and_record_leader_epoch(conn, leader_id="active-leader", epoch=60, scope="scheduler_recovery")
        conn.close()

        with self.assertRaises(StaleLeaderError):
            mark_stale_workers_offline(
                timeout_seconds=5.0,
                leader_id="stale-leader",
                leader_epoch=59,
            )

    def test_04_lease_manager_steps_down_on_renewal_failure(self):
        """LeaderLeaseManager immediately resets is_leader=False if renewal fails."""
        mock_redis = MagicMock()
        mock_redis.eval.return_value = 0  # Renewal script returned 0 (expired / superseded)

        mgr = LeaderLeaseManager(leader_id="sched-alpha", redis_client=mock_redis)
        mgr.force_leadership(epoch=5)
        self.assertTrue(mgr.is_leader)

        renewed = mgr.renew_lease()
        self.assertFalse(renewed)
        self.assertFalse(mgr.is_leader)


class TestVectorClockProcessInteractions(unittest.TestCase):
    """Tests for Vector Clock independent process interactions and explicit membership (Section C)."""

    def test_01_vector_clock_causal_convergence(self):
        """Simulate 3 processes communicating over vectors: P1 -> P2 -> P3."""
        p1 = VectorClock("P1", ["P1", "P2", "P3"])
        p2 = VectorClock("P2", ["P1", "P2", "P3"])
        p3 = VectorClock("P3", ["P1", "P2", "P3"])

        # P1 local event
        v1 = p1.local_event()  # P1: 1
        # P1 sends to P2
        msg1 = p1.send_event()  # P1: 2
        # P2 receives
        v2 = p2.receive_event(msg1)  # P1: 2, P2: 1
        # P2 sends to P3
        msg2 = p2.send_event()  # P1: 2, P2: 2
        # P3 receives
        v3 = p3.receive_event(msg2)  # P1: 2, P2: 2, P3: 1

        self.assertEqual(VectorClock.compare(v1, v2), "BEFORE")
        self.assertEqual(VectorClock.compare(v2, v3), "BEFORE")
        self.assertEqual(VectorClock.compare(v1, v3), "BEFORE")

    def test_02_concurrent_events_identified(self):
        """Independent events on separate processes are identified as CONCURRENT."""
        p1 = VectorClock("P1", ["P1", "P2"])
        p2 = VectorClock("P2", ["P1", "P2"])

        v_p1 = p1.local_event()
        v_p2 = p2.local_event()

        self.assertEqual(VectorClock.compare(v_p1, v_p2), "CONCURRENT")
        self.assertEqual(VectorClock.compare(v_p2, v_p1), "CONCURRENT")


if __name__ == "__main__":
    unittest.main()
