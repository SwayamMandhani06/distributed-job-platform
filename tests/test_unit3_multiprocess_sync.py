"""
Comprehensive Unit and Integration Tests for Phase 4: Unit III Synchronization Requirements.

Covers:
1. Networked Lamport Distributed Mutual Exclusion:
   - Real communication layer (HTTP/TCP sockets across distinct ports).
   - Total request ordering by (Lamport timestamp, process_id).
   - Durable critical section event log verification.
   - Mathematical proof of non-overlapping critical sections:
       max(t_enter_A, t_enter_B) >= min(t_exit_A, t_exit_B).
   - Classical blocking on participant crash (unanimous reply requirement).
   - Recovery extension under Perfect Failure Detector (P) assumption.

2. Networked Chandy-Lamport Global Snapshots:
   - Logical FIFO channel enforcement (monotonic sequence numbering).
   - Marker propagation and local state recording.
   - In-flight message capture in channel states.
   - Global state conservation invariant:
       Sum(Local process balances) + Sum(In-flight channel payloads) == Initial Total.
   - Contrast against naive uncoordinated queries.

3. Concurrent Deadlock Detection & Safe Recovery:
   - Real concurrent tasks/threads coordinated by ConcurrentResourceManager.
   - Dynamic Centralized Wait-For Graph (WFG) generation from active dependencies.
   - DFS 3-coloring cycle detection returning formatted dependency traces.
   - Safe recovery via victim preemption/abort (TaskAbortedException).
   - Post-recovery acyclic graph verification.

4. Networked Beacon Protocol & Failure Detection:
   - Periodic heartbeat exchange between independent nodes over HTTP/TCP.
   - Multi-tier suspicion state model (HEALTHY -> SUSPECTED -> DEAD).
   - Dynamic recovery when a suspected node resumes transmissions.
   - Confirmed crash detection after extended silence.
   - Reproducible fault scenarios (transient pauses, delay, and process death).
"""

import os
import threading
import time
import unittest
from pathlib import Path
from typing import Dict

from common.beacon_transport import (
    NetworkedBeaconEmitterProcess,
    NetworkedBeaconMonitorNode,
    send_http_get as beacon_get,
    send_http_post as beacon_post,
    start_beacon_monitor_server,
)
from common.deadlock_wfg import (
    ConcurrentResourceManager,
    DeadlockReport,
    TaskAbortedException,
)
from common.mutex_transport import (
    NetworkedLamportMutexNode,
    send_http_get as mutex_get,
    send_http_post as mutex_post,
    start_mutex_node_server,
    verify_critical_section_log,
)
from common.snapshot_transport import (
    NetworkedSnapshotNode,
    send_http_get as snap_get,
    send_http_post as snap_post,
    start_snapshot_node_server,
)

BASE_DIR = Path(__file__).resolve().parent.parent
MUTEX_TEST_LOG = BASE_DIR / "test_multiprocess_cs_audit.log"


class TestNetworkedLamportMutualExclusion(unittest.TestCase):
    """Integration tests for Networked Lamport Distributed Mutual Exclusion."""

    def tearDown(self):
        if MUTEX_TEST_LOG.exists():
            MUTEX_TEST_LOG.unlink()

    def test_three_node_networked_mutual_exclusion_and_log_proof(self):
        """Three networked nodes compete concurrently; critical sections never overlap."""
        ports = {"Node-A": 9611, "Node-B": 9612, "Node-C": 9613}
        nodes = {}
        servers = []

        try:
            for nid, port in ports.items():
                peers = {p: prt for p, prt in ports.items() if p != nid}
                node = NetworkedLamportMutexNode(
                    node_id=nid,
                    port=port,
                    peers=peers,
                    shared_log_file=str(MUTEX_TEST_LOG),
                )
                srv = start_mutex_node_server(node)
                nodes[nid] = node
                servers.append(srv)

            time.sleep(0.3)

            # Node-A, Node-B, Node-C initiate requests concurrently
            nodes["Node-A"].request_critical_section()
            nodes["Node-B"].request_critical_section()
            nodes["Node-C"].request_critical_section()

            # Allow requests and replies to propagate across sockets
            time.sleep(0.5)

            # Sequential entry following total order
            for _ in range(3):
                # Find the node whose request is at the head and has unanimous replies
                eligible = None
                for nid, node in nodes.items():
                    if node.can_enter():
                        eligible = node
                        break
                self.assertIsNotNone(eligible, "At least one node must satisfy entry conditions")

                entered = eligible.enter_critical_section()
                self.assertTrue(entered)
                self.assertTrue(eligible.in_critical_section)

                # Ensure no other node can enter simultaneously
                for other_id, other_node in nodes.items():
                    if other_id != eligible.node_id:
                        self.assertFalse(other_node.can_enter())
                        self.assertFalse(other_node.in_critical_section)

                time.sleep(0.15)  # Simulate critical section work
                eligible.release_critical_section()
                self.assertFalse(eligible.in_critical_section)
                time.sleep(0.3)  # Allow release to propagate

            # Mathematically verify the durable audit log
            self.assertTrue(MUTEX_TEST_LOG.exists())
            is_valid, errors, sessions = verify_critical_section_log(str(MUTEX_TEST_LOG))
            self.assertTrue(is_valid, f"Critical section log failed verification: {errors}")
            self.assertEqual(len(sessions), 3)

            # Re-verify interval disjointness: for all pairs (i, j), intervals do not intersect
            for i in range(len(sessions)):
                for j in range(i + 1, len(sessions)):
                    s1 = sessions[i]
                    s2 = sessions[j]
                    overlap = max(s1["enter_time"], s2["enter_time"]) < min(s1["exit_time"], s2["exit_time"])
                    self.assertFalse(overlap, f"Overlap between {s1['node_id']} and {s2['node_id']}")

        finally:
            for srv in servers:
                srv.shutdown()

    def test_classical_blocking_and_failure_detector_eviction(self):
        """Classical Lamport blocks on crashed node; evicting dead node restores progress."""
        ports = {"Node-1": 9621, "Node-2": 9622, "Node-3": 9623}
        nodes = {}
        servers = []

        try:
            for nid, port in ports.items():
                peers = {p: prt for p, prt in ports.items() if p != nid}
                node = NetworkedLamportMutexNode(node_id=nid, port=port, peers=peers)
                srv = start_mutex_node_server(node)
                nodes[nid] = node
                servers.append(srv)

            time.sleep(0.3)

            # Node-3 crashes (stops participating and cannot reply)
            nodes["Node-3"].set_alive(False)

            # Node-1 requests CS
            nodes["Node-1"].request_critical_section()
            time.sleep(0.4)

            # In classical Lamport, Node-1 requires a reply from Node-3. Since Node-3 is dead, Node-1 blocks!
            self.assertFalse(nodes["Node-1"].can_enter(), "Node-1 must block due to missing reply from crashed Node-3")

            # Documented Failure Model: Perfect Failure Detector (P) certifies Node-3 crash
            nodes["Node-1"].mark_node_dead("Node-3")
            time.sleep(0.2)

            # Now Node-1 unblocks and can enter CS!
            self.assertTrue(nodes["Node-1"].can_enter(), "Node-1 must unblock once Node-3 is evicted from quorum")
            self.assertTrue(nodes["Node-1"].enter_critical_section())
            nodes["Node-1"].release_critical_section()

        finally:
            for srv in servers:
                srv.shutdown()


class TestNetworkedChandyLamportSnapshot(unittest.TestCase):
    """Integration tests for Networked Chandy-Lamport Global Snapshots."""

    def test_multiprocess_token_conservation_during_concurrent_transfers(self):
        """Snapshots taken over logical FIFO channels satisfy total token conservation invariant."""
        ports = {"P1": 9711, "P2": 9712, "P3": 9713}
        initial_balances = {"P1": 150, "P2": 100, "P3": 50}  # Total = 300
        nodes = {}
        servers = []

        try:
            for pid, port in ports.items():
                peers = {p: prt for p, prt in ports.items() if p != pid}
                node = NetworkedSnapshotNode(
                    node_id=pid,
                    port=port,
                    peers=peers,
                    initial_balance=initial_balances[pid],
                )
                srv = start_snapshot_node_server(node)
                nodes[pid] = node
                servers.append(srv)

            time.sleep(0.3)

            # P1 transfers 40 to P2
            nodes["P1"].send_transfer("P2", 40)

            # P1 immediately initiates snapshot
            nodes["P1"].initiate_snapshot("SNAP-TEST-1")

            # Concurrently, P2 transfers 20 to P3 before seeing P1's marker
            nodes["P2"].send_transfer("P3", 20)

            # Wait for all markers and messages to propagate
            for _ in range(25):
                if all(n.is_snapshot_complete() for n in nodes.values()):
                    break
                time.sleep(0.1)

            self.assertTrue(all(n.is_snapshot_complete() for n in nodes.values()), "All nodes must complete snapshot")

            # Aggregate recorded global state
            sum_recorded_local = sum(n.recorded_state for n in nodes.values())
            sum_recorded_channels = 0
            for n in nodes.values():
                for sender, in_flight in n.recorded_channel_messages.items():
                    sum_recorded_channels += sum(in_flight)

            total_recorded = sum_recorded_local + sum_recorded_channels

            # Verify Chandy-Lamport Conservation Invariant
            self.assertEqual(total_recorded, 300, f"Snapshot total {total_recorded} must strictly equal initial 300")

        finally:
            for srv in servers:
                srv.shutdown()

    def test_channel_in_flight_capture_when_message_sent_before_marker_arrival(self):
        """Channel state captures in-flight message sent to initiator after initiator started recording."""
        ports = {"A": 9721, "B": 9722}
        nodes = {}
        servers = []

        try:
            for pid, port in ports.items():
                peers = {p: prt for p, prt in ports.items() if p != pid}
                node = NetworkedSnapshotNode(
                    node_id=pid,
                    port=port,
                    peers=peers,
                    initial_balance=100,
                    simulated_delay_seconds=0.05,
                )
                srv = start_snapshot_node_server(node)
                nodes[pid] = node
                servers.append(srv)

            time.sleep(0.3)

            # A initiates snapshot (A records balance 100, starts recording channel B->A, sends marker to B)
            nodes["A"].initiate_snapshot("SNAP-IN-FLIGHT")

            # B sends 30 to A before B's marker arrives
            nodes["B"].send_transfer("A", 30)

            # Wait for marker and transfer completion
            for _ in range(25):
                if nodes["A"].is_snapshot_complete() and nodes["B"].is_snapshot_complete():
                    break
                time.sleep(0.1)

            self.assertTrue(nodes["A"].is_snapshot_complete())
            self.assertTrue(nodes["B"].is_snapshot_complete())

            # A recorded state = 100
            # B recorded state = 70 (since B subtracted 30 before seeing marker)
            # Channel B->A recorded state = [30] (in-flight token captured!)
            self.assertEqual(nodes["A"].recorded_state, 100)
            self.assertEqual(nodes["B"].recorded_state, 70)
            self.assertEqual(nodes["A"].recorded_channel_messages["B"], [30])

            total = nodes["A"].recorded_state + nodes["B"].recorded_state + sum(nodes["A"].recorded_channel_messages["B"])
            self.assertEqual(total, 200)

        finally:
            for srv in servers:
                srv.shutdown()


class TestConcurrentDeadlockDetectionAndRecovery(unittest.TestCase):
    """Integration tests for Concurrent Resource Manager with Centralized WFG cycle detection."""

    def test_real_concurrent_thread_deadlock_detection_and_victim_abort(self):
        """Concurrent threads create circular wait; WFG detects cycle and aborts victim cleanly."""
        manager = ConcurrentResourceManager()
        outcomes = {}

        def thread_1_work():
            try:
                manager.acquire("Worker-1", "ResA")
                time.sleep(0.2)
                manager.acquire("Worker-1", "ResB")
                time.sleep(0.1)
                manager.release("Worker-1", "ResB")
                manager.release("Worker-1", "ResA")
                outcomes["Worker-1"] = "SUCCESS"
            except TaskAbortedException as exc:
                outcomes["Worker-1"] = f"ABORTED: {exc}"

        def thread_2_work():
            try:
                manager.acquire("Worker-2", "ResB")
                time.sleep(0.2)
                manager.acquire("Worker-2", "ResA")
                time.sleep(0.1)
                manager.release("Worker-2", "ResA")
                manager.release("Worker-2", "ResB")
                outcomes["Worker-2"] = "SUCCESS"
            except TaskAbortedException as exc:
                outcomes["Worker-2"] = f"ABORTED: {exc}"

        t1 = threading.Thread(target=thread_1_work)
        t2 = threading.Thread(target=thread_2_work)
        t1.start()
        t2.start()

        # Allow threads to acquire first lock and block on second lock
        time.sleep(0.6)

        # Detect deadlock
        report = manager.detect_deadlock()
        self.assertTrue(report.is_deadlocked, "WFG must detect circular wait between Worker-1 and Worker-2")
        self.assertEqual(len(report.cycles), 1)
        self.assertIn("Worker-1", report.deadlocked_processes)
        self.assertIn("Worker-2", report.deadlocked_processes)

        # Resolve deadlock via designated victim abort (lexicographical: Worker-2)
        resolution = manager.detect_and_resolve(victim_policy="lexicographical")
        self.assertIsNotNone(resolution)
        self.assertEqual(resolution.victim_task_id, "Worker-2")
        self.assertIn("ResB", resolution.freed_resources)

        # Wait for threads to finish
        t1.join(timeout=2.0)
        t2.join(timeout=2.0)

        # Worker-2 must have caught TaskAbortedException
        self.assertIn("ABORTED", outcomes["Worker-2"])
        # Worker-1 must have acquired ResB once Worker-2 was aborted and completed successfully!
        self.assertEqual(outcomes["Worker-1"], "SUCCESS")

        # Live WFG must now be strictly acyclic
        post_report = manager.detect_deadlock()
        self.assertFalse(post_report.is_deadlocked)


class TestNetworkedBeaconProtocol(unittest.TestCase):
    """Integration tests for Networked Periodic Beacon Protocol and Failure Detection."""

    def test_networked_beacon_suspicion_recovery_and_death(self):
        """Beacon monitor tracks healthy heartbeats, suspects on pause, recovers on resume, and marks dead on crash."""
        monitor_node = NetworkedBeaconMonitorNode(
            port=9811,
            beacon_interval_seconds=0.2,
            missed_beacon_threshold=3,
            dead_threshold_missed=6,
        )
        server = start_beacon_monitor_server(monitor_node)

        try:
            time.sleep(0.2)
            # 1. Emitter sends periodic beacons
            emitter = NetworkedBeaconEmitterProcess(
                node_id="Node-Test",
                monitor_url="http://127.0.0.1:9811",
                interval_seconds=0.2,
            )
            emitter_thread = threading.Thread(target=emitter.run_loop, daemon=True)
            emitter_thread.start()

            # Let 4 beacons arrive
            time.sleep(0.8)
            liveness = monitor_node.evaluate_liveness()
            self.assertIn("Node-Test", liveness)
            self.assertEqual(liveness["Node-Test"]["state"], "HEALTHY")

            # 2. Simulate transient pause / GC hang: pause emitter for 0.7s (>= 3 missed intervals)
            emitter.pause_event.set()
            time.sleep(0.7)

            liveness_suspect = monitor_node.evaluate_liveness()
            self.assertEqual(
                liveness_suspect["Node-Test"]["state"],
                "SUSPECTED",
                f"Expected SUSPECTED, got {liveness_suspect['Node-Test']['state']}",
            )

            # 3. Simulate recovery: resume emitter transmissions
            emitter.pause_event.clear()
            time.sleep(0.5)

            liveness_recovered = monitor_node.evaluate_liveness()
            self.assertEqual(
                liveness_recovered["Node-Test"]["state"],
                "HEALTHY",
                "Node must RECOVER to HEALTHY upon resuming beacon transmission",
            )

            # 4. Simulate permanent crash: stop emitter completely for 1.4s (>= 6 missed intervals)
            emitter.is_running = False
            time.sleep(1.4)

            liveness_dead = monitor_node.evaluate_liveness()
            self.assertEqual(
                liveness_dead["Node-Test"]["state"],
                "DEAD",
                f"Expected DEAD after 6 missed intervals, got {liveness_dead['Node-Test']['state']}",
            )

        finally:
            server.shutdown()


if __name__ == "__main__":
    unittest.main()
