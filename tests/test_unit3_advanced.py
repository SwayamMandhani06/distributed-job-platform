"""
Comprehensive Unit and Integration Tests for Phase 3.3 Advanced Unit III Concepts:
1. Chandy-Lamport Distributed Snapshot & Consistent Cuts
2. Wait-For Graph (WFG) Deadlock Detection & Preemption
3. Beacon Protocol, Failure Suspicions & Node Recovery
"""

import unittest
from typing import Dict

from common.snapshot_chandy_lamport import ChandyLamportCluster, FIFOChannel, ChannelMessage
from common.deadlock_wfg import WaitForGraph, DeadlockReport
from common.beacon_protocol import BeaconEmitter, BeaconMonitor, BeaconPacket, NodeLivenessStatus


class TestChandyLamportSnapshot(unittest.TestCase):
    """Automated tests for Chandy-Lamport Distributed Snapshot and Consistent Cuts."""

    def test_snapshot_with_no_in_flight_messages(self):
        """When no messages are in-flight, snapshot captures pure process local states."""
        cluster = ChandyLamportCluster({"P1": 150, "P2": 150})
        p1 = cluster.get_process("P1")

        # P1 initiates snapshot
        p1.initiate_snapshot("SNAP-NO-FLIGHT")

        # Deliver all markers
        steps = cluster.step_all()
        self.assertTrue(cluster.is_snapshot_complete())

        snap = cluster.get_global_snapshot("SNAP-NO-FLIGHT")
        self.assertTrue(snap.is_complete)
        self.assertEqual(snap.process_states["P1"], 150)
        self.assertEqual(snap.process_states["P2"], 150)
        self.assertEqual(snap.total_value, 300)
        # All channels must have empty recorded lists
        for chan_key, msgs in snap.channel_states.items():
            self.assertEqual(msgs, [])

    def test_snapshot_captures_in_flight_channel_messages(self):
        """Messages sent before marker arrival are correctly captured in channel states."""
        cluster = ChandyLamportCluster({"P1": 100, "P2": 100, "P3": 100})
        p1 = cluster.get_process("P1")
        p2 = cluster.get_process("P2")
        p3 = cluster.get_process("P3")

        # P1 sends 30 to P2
        p1.send_transfer("P2", 30)

        # P1 initiates snapshot
        p1.initiate_snapshot("SNAP-FLIGHT")

        # P2 sends 20 to P3 before seeing any marker
        p2.send_transfer("P3", 20)

        # Step until all messages and markers are completely processed
        cluster.step_all()

        self.assertTrue(cluster.is_snapshot_complete())
        snap = cluster.get_global_snapshot("SNAP-FLIGHT")

        # Total value (processes + in-flight channels) must strictly equal 300
        self.assertEqual(snap.total_value, 300)
        self.assertTrue(snap.is_complete)

    def test_fifo_channel_order_preservation(self):
        """FIFOChannel preserves strict First-In, First-Out queue ordering."""
        chan = FIFOChannel(sender="P1", receiver="P2")
        m1 = ChannelMessage(msg_type="APP", sender="P1", receiver="P2", payload=10)
        m2 = ChannelMessage(msg_type="MARKER", sender="P1", receiver="P2", marker_id="M1")
        m3 = ChannelMessage(msg_type="APP", sender="P1", receiver="P2", payload=20)

        chan.send(m1)
        chan.send(m2)
        chan.send(m3)

        self.assertEqual(chan.receive(), m1)
        self.assertEqual(chan.receive(), m2)
        self.assertEqual(chan.receive(), m3)
        self.assertIsNone(chan.receive())

    def test_multi_hop_token_conservation_invariant(self):
        """Complex interleaving of multi-hop transfers satisfies consistent cut invariant."""
        cluster = ChandyLamportCluster({"A": 200, "B": 200, "C": 200})
        a = cluster.get_process("A")
        b = cluster.get_process("B")

        a.send_transfer("B", 50)
        b.send_transfer("C", 25)

        # B initiates snapshot
        b.initiate_snapshot("SNAP-MULTI")

        cluster.step_all()
        snap = cluster.get_global_snapshot("SNAP-MULTI")

        self.assertEqual(snap.total_value, 600, "Sum of all process and channel balances must equal 600")


class TestWaitForGraphDeadlockDetection(unittest.TestCase):
    """Automated tests for Wait-For Graph (WFG) cycle detection and deadlock resolution."""

    def test_empty_and_disconnected_graphs_are_acyclic(self):
        """An empty graph or disconnected processes with no edges has no deadlock."""
        wfg = WaitForGraph()
        self.assertFalse(wfg.is_deadlocked())

        wfg.add_process("P1")
        wfg.add_process("P2")
        self.assertFalse(wfg.is_deadlocked())
        report = wfg.detect_deadlock()
        self.assertFalse(report.is_deadlocked)
        self.assertEqual(report.cycles, [])

    def test_linear_wait_chain_is_acyclic(self):
        """A linear dependency chain P1 -> P2 -> P3 -> P4 has no circular wait."""
        wfg = WaitForGraph()
        wfg.add_wait_dependency("P1", "P2", resource_id="R1")
        wfg.add_wait_dependency("P2", "P3", resource_id="R2")
        wfg.add_wait_dependency("P3", "P4", resource_id="R3")

        report = wfg.detect_deadlock()
        self.assertFalse(report.is_deadlocked)
        self.assertEqual(report.cycles, [])

    def test_direct_two_process_deadlock(self):
        """Two processes in mutual wait P1 -> P2 and P2 -> P1 form a deadlock."""
        wfg = WaitForGraph()
        wfg.add_wait_dependency("P1", "P2", resource_id="LockB")
        wfg.add_wait_dependency("P2", "P1", resource_id="LockA")

        self.assertTrue(wfg.is_deadlocked())
        report = wfg.detect_deadlock()
        self.assertTrue(report.is_deadlocked)
        self.assertEqual(len(report.cycles), 1)
        self.assertIn("P1", report.deadlocked_processes)
        self.assertIn("P2", report.deadlocked_processes)

    def test_self_deadlock_edge_case(self):
        """A process waiting on itself P1 -> P1 constitutes a deadlock cycle."""
        wfg = WaitForGraph()
        wfg.add_wait_dependency("P1", "P1", resource_id="SelfLock")

        self.assertTrue(wfg.is_deadlocked())
        cycles = wfg.find_all_cycles()
        self.assertEqual(len(cycles), 1)
        self.assertEqual(cycles[0], ["P1", "P1"])

    def test_three_process_resource_manager_deadlock_and_recovery(self):
        """Simulation of resource allocation with circular wait and recovery via victim abort."""
        wfg = WaitForGraph()
        # P1 holds R1, P2 holds R2, P3 holds R3
        self.assertTrue(wfg.request_resource("P1", "R1"))
        self.assertTrue(wfg.request_resource("P2", "R2"))
        self.assertTrue(wfg.request_resource("P3", "R3"))

        # P1 wants R2 (held by P2) -> blocks
        self.assertFalse(wfg.request_resource("P1", "R2"))
        # P2 wants R3 (held by P3) -> blocks
        self.assertFalse(wfg.request_resource("P2", "R3"))
        # P3 wants R1 (held by P1) -> blocks -> creates cycle!
        self.assertFalse(wfg.request_resource("P3", "R1"))

        report = wfg.detect_deadlock()
        self.assertTrue(report.is_deadlocked)
        self.assertEqual(len(report.cycles), 1)

        # Abort P2
        wfg.resolve_deadlock_by_aborting("P2")
        post_report = wfg.detect_deadlock()
        self.assertFalse(post_report.is_deadlocked, "Aborting P2 must break circular wait")

    def test_multiple_independent_cycles(self):
        """Multiple disjoint cycles in the same graph are all identified."""
        wfg = WaitForGraph()
        # Cycle 1: P1 <-> P2
        wfg.add_wait_dependency("P1", "P2")
        wfg.add_wait_dependency("P2", "P1")

        # Cycle 2: P3 -> P4 -> P5 -> P3
        wfg.add_wait_dependency("P3", "P4")
        wfg.add_wait_dependency("P4", "P5")
        wfg.add_wait_dependency("P5", "P3")

        cycles = wfg.find_all_cycles()
        self.assertEqual(len(cycles), 2, "Both disjoint cycles must be detected")


class TestBeaconProtocol(unittest.TestCase):
    """Automated tests for Beacon Protocol, failure suspicion, and node recovery."""

    def test_normal_beacon_receipt_maintains_healthy(self):
        """Continuous beacon receipts within interval maintain HEALTHY status."""
        monitor = BeaconMonitor(beacon_interval_seconds=1.0, missed_beacon_threshold=3, dead_threshold_missed=6)
        emitter = BeaconEmitter("Node-1", beacon_interval_seconds=1.0)

        for t in [1.0, 2.0, 3.0, 4.0]:
            pkt = emitter.emit_beacon(current_time=t)
            self.assertIsNotNone(pkt)
            status = monitor.record_beacon(pkt)
            self.assertEqual(status.state, "HEALTHY")

        evaluation = monitor.evaluate_liveness(current_time=4.5)
        self.assertEqual(evaluation["Node-1"].state, "HEALTHY")
        self.assertEqual(evaluation["Node-1"].missed_beacons, 0)

    def test_missed_beacons_transitions_to_suspected(self):
        """Missing beacons >= threshold transitions node to SUSPECTED."""
        monitor = BeaconMonitor(beacon_interval_seconds=1.0, missed_beacon_threshold=3, dead_threshold_missed=6)
        emitter = BeaconEmitter("Node-1", beacon_interval_seconds=1.0)

        # Beacons at t=1.0, 2.0
        monitor.record_beacon(emitter.emit_beacon(current_time=1.0))
        monitor.record_beacon(emitter.emit_beacon(current_time=2.0))

        # Node goes silent. At t=5.5 (3.5s elapsed):
        evaluation = monitor.evaluate_liveness(current_time=5.5)
        self.assertEqual(evaluation["Node-1"].state, "SUSPECTED")
        self.assertEqual(evaluation["Node-1"].missed_beacons, 3)

    def test_recovery_from_suspected_back_to_healthy(self):
        """A SUSPECTED node that resumes beacon emission recovers back to HEALTHY."""
        monitor = BeaconMonitor(beacon_interval_seconds=1.0, missed_beacon_threshold=3, dead_threshold_missed=6)
        emitter = BeaconEmitter("Node-1", beacon_interval_seconds=1.0)

        monitor.record_beacon(emitter.emit_beacon(current_time=1.0))

        # Silence until t=4.5 -> SUSPECTED
        eval_suspect = monitor.evaluate_liveness(current_time=4.5)
        self.assertEqual(eval_suspect["Node-1"].state, "SUSPECTED")

        # Node resumes at t=5.0
        resume_pkt = emitter.emit_beacon(current_time=5.0)
        rec_status = monitor.record_beacon(resume_pkt)

        self.assertTrue(rec_status.state_changed)
        self.assertEqual(rec_status.previous_state, "SUSPECTED")
        self.assertEqual(rec_status.state, "HEALTHY")

        eval_healthy = monitor.evaluate_liveness(current_time=5.0)
        self.assertEqual(eval_healthy["Node-1"].state, "HEALTHY")

    def test_exceeding_dead_threshold_marks_node_dead(self):
        """Silence exceeding dead threshold transitions node to DEAD."""
        monitor = BeaconMonitor(beacon_interval_seconds=1.0, missed_beacon_threshold=3, dead_threshold_missed=6)
        emitter = BeaconEmitter("Node-1", beacon_interval_seconds=1.0)

        monitor.record_beacon(emitter.emit_beacon(current_time=1.0))

        # Evaluate at t=8.0 (7.0s elapsed >= 6 missed beacons)
        evaluation = monitor.evaluate_liveness(current_time=8.0)
        self.assertEqual(evaluation["Node-1"].state, "DEAD")
        self.assertGreaterEqual(evaluation["Node-1"].missed_beacons, 6)

    def test_stopped_emitter_produces_none(self):
        """Stopping an emitter halts beacon generation."""
        emitter = BeaconEmitter("Node-1")
        emitter.stop()
        self.assertIsNone(emitter.emit_beacon(current_time=1.0))

        emitter.resume()
        self.assertIsNotNone(emitter.emit_beacon(current_time=2.0))
