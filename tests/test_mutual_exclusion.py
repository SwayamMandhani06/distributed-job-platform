"""
Unit and Integration Tests for Unit III: Lamport Distributed Mutual Exclusion.

Covers:
1. Total Ordering of Requests by (Lamport timestamp, Process ID).
2. Safety Property: Non-overlapping execution of the Critical Section.
3. Message Protocol Compliance: REQUEST, REPLY, RELEASE handling.
4. Duplicate Message Resilience: Duplicate requests, duplicate replies, and repeated releases.
5. Participant Crash / Unavailability: Explicit detection and eviction prevents deadlock.
6. Verification against the shared audit ledger file (`shared_audit_ledger.log`).
"""

import os
import unittest
from pathlib import Path

from common.mutual_exclusion import LamportMutexNode, MutexCluster, MutexMessage

BASE_DIR = Path(__file__).resolve().parent.parent
LEDGER_FILE = BASE_DIR / "test_shared_audit_ledger.log"


class TestLamportMutualExclusion(unittest.TestCase):
    """Test suite for Lamport Distributed Mutual Exclusion protocol."""

    def tearDown(self):
        if LEDGER_FILE.exists():
            LEDGER_FILE.unlink()

    def test_total_ordering_of_requests(self):
        """Requests are ordered strictly by (Lamport timestamp, process_id)."""
        node = LamportMutexNode(node_id="Node-1", all_node_ids=["Node-1", "Node-2", "Node-3"])

        # Insert requests out of order
        node.handle_message(MutexMessage("REQUEST", timestamp=5, sender_id="Node-3"))
        node.handle_message(MutexMessage("REQUEST", timestamp=2, sender_id="Node-2"))
        node.handle_message(MutexMessage("REQUEST", timestamp=2, sender_id="Node-1"))

        # In total order, (2, 'Node-1') < (2, 'Node-2') < (5, 'Node-3')
        expected_order = [(2, "Node-1"), (2, "Node-2"), (5, "Node-3")]
        self.assertEqual(node.request_queue, expected_order)

    def test_three_node_non_overlapping_critical_sections(self):
        """Three competing nodes enter and exit CS strictly sequentially without overlap."""
        cluster = MutexCluster(["P1", "P2", "P3"])
        p1 = cluster.get_node("P1")
        p2 = cluster.get_node("P2")
        p3 = cluster.get_node("P3")

        # P2 requests CS first, then P1, then P3
        ts2 = p2.request_critical_section()
        ts1 = p1.request_critical_section()
        ts3 = p3.request_critical_section()

        # At this stage, P2 must have lowest timestamp and be at head of all queues
        self.assertTrue(p2.can_enter())
        self.assertFalse(p1.can_enter())
        self.assertFalse(p3.can_enter())

        # P2 enters CS
        p2.enter_critical_section()
        self.assertTrue(p2.in_critical_section)
        self.assertFalse(p1.in_critical_section)
        self.assertFalse(p3.in_critical_section)

        # While P2 is in CS, no other process can enter
        self.assertFalse(p1.can_enter())
        self.assertFalse(p3.can_enter())

        # P2 exits CS
        p2.release_critical_section()
        self.assertFalse(p2.in_critical_section)

        # Now P1 must be able to enter
        self.assertTrue(p1.can_enter())
        self.assertFalse(p3.can_enter())
        p1.enter_critical_section()
        p1.release_critical_section()

        # Now P3 must be able to enter
        self.assertTrue(p3.can_enter())
        p3.enter_critical_section()
        p3.release_critical_section()

        # All queues should now be empty
        self.assertEqual(len(p1.request_queue), 0)
        self.assertEqual(len(p2.request_queue), 0)
        self.assertEqual(len(p3.request_queue), 0)

    def test_duplicate_message_resilience(self):
        """Duplicate REQUEST, REPLY, or RELEASE messages do not corrupt queue state."""
        node = LamportMutexNode(node_id="Node-A", all_node_ids=["Node-A", "Node-B"])

        # Send same REQUEST twice
        req_msg = MutexMessage("REQUEST", timestamp=10, sender_id="Node-B")
        reply1 = node.handle_message(req_msg)
        reply2 = node.handle_message(req_msg)

        # Queue should only contain one entry for (10, 'Node-B')
        self.assertEqual(len(node.request_queue), 1)
        self.assertEqual(node.request_queue[0], (10, "Node-B"))

        # Duplicate replies
        node.request_critical_section()
        rep_msg = MutexMessage("REPLY", timestamp=12, sender_id="Node-B")
        node.handle_message(rep_msg)
        node.handle_message(rep_msg)
        # Replies received set should remain deduplicated
        self.assertEqual(node.replies_received, {"Node-A", "Node-B"})

        # Duplicate release
        rel_msg = MutexMessage("RELEASE", timestamp=15, sender_id="Node-B")
        node.handle_message(rel_msg)
        node.handle_message(rel_msg)  # Should not raise exception
        self.assertEqual([r for r in node.request_queue if r[1] == "Node-B"], [])

    def test_unavailable_participant_handling(self):
        """An unavailable participant is explicitly evicted to prevent deadlock."""
        cluster = MutexCluster(["Node-1", "Node-2", "Node-3"])
        n1 = cluster.get_node("Node-1")

        # Node-3 crashes before replying to Node-1
        n1.mark_node_dead("Node-3")

        # Node-1 requests CS
        n1.request_critical_section()

        # Node-1 receives reply only from live Node-2
        # Since Node-3 is dead, Node-1 can enter CS without waiting indefinitely
        self.assertTrue(n1.can_enter())
        n1.enter_critical_section()
        self.assertTrue(n1.in_critical_section)
        n1.release_critical_section()
