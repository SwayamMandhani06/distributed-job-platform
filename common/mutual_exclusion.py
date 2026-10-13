"""
Lamport Distributed Mutual Exclusion Protocol (Unit III — Synchronization).

Theoretical Background (Leslie Lamport, 1978):
----------------------------------------------
"Time, Clocks, and the Ordering of Events in a Distributed System" (CACM 1978).

In a distributed system without shared memory or a global clock, processes coordinate
access to a shared critical section (CS) using logical clocks and message passing.

Algorithm Rules:
----------------
1. Requesting Critical Section:
   - Node P_i advances its Lamport clock C_i.
   - P_i inserts (C_i, P_i) into its local priority request queue.
   - P_i sends REQUEST(C_i, P_i) to all other participants.

2. Receiving REQUEST(C_j, P_j):
   - Node P_i updates its clock: C_i = max(C_i, C_j) + 1.
   - P_i inserts (C_j, P_j) into its local priority request queue.
   - P_i sends REPLY(C_i, P_i) back to P_j.

3. Receiving REPLY(C_j, P_j):
   - Node P_i updates its clock: C_i = max(C_i, C_j) + 1.
   - P_i records that it has received a reply from P_j.

4. Entering Critical Section:
   Node P_i enters CS when BOTH conditions hold:
   a) P_i's own request (C_req, P_i) is at the HEAD of its local request queue
      under the total order: (T_a, P_a) < (T_b, P_b) iff T_a < T_b or (T_a == T_b and P_a < P_b).
   b) P_i has received a message (REPLY or later timestamp) from every other participant.

5. Releasing Critical Section:
   - Node P_i advances its clock C_i.
   - P_i removes its request from its local queue.
   - P_i broadcasts RELEASE(C_i, P_i) to all participants.

6. Receiving RELEASE(C_j, P_j):
   - Node P_i updates its clock: C_i = max(C_i, C_j) + 1.
   - P_i removes (C_req_j, P_j) from its local request queue.

Fault Tolerance, Failure Models & Simulation Scope:
----------------------------------------------------
1. In-Process Educational Simulation:
   This module provides an educational simulation and reference implementation of
   Lamport's classical mutual exclusion algorithm. Nodes run within the same Python
   process and exchange messages via thread-safe callbacks for deterministic verification.
   It demonstrates algorithmic correctness but does NOT establish OS-level advisory file
   locking across separate operating system processes.

2. Classical Algorithm Assumptions:
   The classical Lamport (1978) protocol strictly assumes:
   - Reliable FIFO channels: Messages are never lost, duplicated, or reordered.
   - Non-failing nodes: Classical Lamport mutex does NOT tolerate process crashes. If any
     node crashes before sending a REPLY or RELEASE, all other nodes block indefinitely.

3. Failure Detector Assumptions for `mark_node_dead()`:
   To avoid indefinite blocking, this implementation provides `mark_node_dead(dead_id)`.
   In formal distributed systems theory (Chandra & Toueg, 1996), this mechanism assumes
   a Perfect Failure Detector (P) under a Crash-Stop model:
   - Strong Completeness: Every crashed process is eventually detected.
   - Strong Accuracy: No live process is ever falsely suspected.
   WARNING: In an asynchronous network with unpredictable latency or GC pauses,
   a live node may be delayed and falsely suspected. If a falsely suspected node enters the
   critical section while remaining nodes have evicted it, safety is violated!

4. Inter-Process Concurrency:
   To coordinate shared filesystem or storage resources across independent OS processes,
   distributed systems require distributed coordination services (e.g., Redis leader leases
   with fencing tokens, ZooKeeper ephemeral sequential nodes) or OS-level advisory file locks.
"""

import heapq
import queue
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from common.clocks import LamportClock
from common.logging_config import setup_logger

logger = setup_logger("mutual_exclusion")


@dataclass(order=True)
class RequestItem:
    """Entry in the local Lamport request priority queue."""
    timestamp: int
    process_id: str


@dataclass
class MutexMessage:
    """Message exchanged during distributed mutual exclusion."""
    msg_type: str  # 'REQUEST', 'REPLY', 'RELEASE'
    timestamp: int
    sender_id: str
    target_id: Optional[str] = None  # None indicates broadcast

    def __repr__(self) -> str:
        target = f" -> {self.target_id}" if self.target_id else " -> ALL"
        return f"[{self.msg_type} L#{self.timestamp} from {self.sender_id}{target}]"


class LamportMutexNode:
    """A participant in Lamport's distributed mutual exclusion protocol."""

    def __init__(
        self,
        node_id: str,
        all_node_ids: List[str],
        send_callback: Optional[Callable[[MutexMessage], None]] = None,
    ) -> None:
        self.node_id = str(node_id)
        self.all_node_ids = sorted([str(n) for n in all_node_ids])
        self.other_node_ids = [n for n in self.all_node_ids if n != self.node_id]
        self.clock = LamportClock()
        self.send_callback = send_callback

        # Priority request queue: list of (timestamp, process_id)
        self.request_queue: List[Tuple[int, str]] = []
        # Replies received for current request
        self.replies_received: Set[str] = set()
        # Set of dead/offline nodes excluded from quorum requirements
        self.dead_nodes: Set[str] = set()

        self._current_request_ts: Optional[int] = None
        self._in_critical_section = False
        self._waiting = False
        self._lock = threading.RLock()

        # Audit history for inspection and assertions
        self.event_history: List[Dict[str, Any]] = []

    @property
    def in_critical_section(self) -> bool:
        with self._lock:
            return self._in_critical_section

    @property
    def is_waiting(self) -> bool:
        with self._lock:
            return self._waiting

    @property
    def current_request_ts(self) -> Optional[int]:
        with self._lock:
            return self._current_request_ts

    def mark_node_dead(self, dead_id: str) -> None:
        """Explicitly evict an unavailable participant to prevent indefinite deadlock."""
        with self._lock:
            if dead_id in self.other_node_ids:
                self.dead_nodes.add(dead_id)
                # Remove any pending requests from dead node
                self.request_queue = [r for r in self.request_queue if r[1] != dead_id]
                logger.warning(
                    "Node %s marked %s as DEAD; excluded from required reply quorum",
                    self.node_id,
                    dead_id,
                )

    def request_critical_section(self) -> int:
        """Initiate request to enter the critical section.

        Returns the Lamport timestamp assigned to this request.
        """
        with self._lock:
            if self._waiting or self._in_critical_section:
                return self._current_request_ts or self.clock.get_time()

            req_ts = self.clock.send_event()
            self._current_request_ts = req_ts
            self._waiting = True
            self.replies_received = {self.node_id}  # Node trivially replies to itself

            # Insert into own queue
            self._insert_request(req_ts, self.node_id)

            self._log_event("REQUEST_INITIATED", req_ts, {
                "active_queue": list(self.request_queue),
            })

            # Broadcast REQUEST to all other peers
            msg = MutexMessage(
                msg_type="REQUEST",
                timestamp=req_ts,
                sender_id=self.node_id,
                target_id=None,
            )
            if self.send_callback:
                self.send_callback(msg)

            return req_ts

    def can_enter(self) -> bool:
        """Check if conditions to enter the critical section are satisfied."""
        with self._lock:
            if not self._waiting or self._in_critical_section or self._current_request_ts is None:
                return False

            # Condition 1: Own request is at head of queue
            if not self.request_queue:
                return False
            head_req = self.request_queue[0]
            if head_req != (self._current_request_ts, self.node_id):
                return False

            # Condition 2: Received replies from all live participants
            required_replies = set(self.all_node_ids) - self.dead_nodes
            if not required_replies.issubset(self.replies_received):
                return False

            return True

    def enter_critical_section(self) -> bool:
        """Transition into critical section if conditions are met."""
        with self._lock:
            if not self.can_enter():
                return False

            self._in_critical_section = True
            self._waiting = False
            self._log_event("CRITICAL_SECTION_ENTERED", self.clock.get_time(), {
                "request_ts": self._current_request_ts,
                "queue": list(self.request_queue),
                "replies": list(self.replies_received),
            })
            logger.info(
                "CS ENTER: Node %s ENTERED Critical Section (Req L#%d, Total Queue: %s)",
                self.node_id,
                self._current_request_ts,
                self.request_queue,
            )
            return True

    def release_critical_section(self) -> int:
        """Leave critical section, update clock, and broadcast RELEASE to all peers."""
        with self._lock:
            if not self._in_critical_section:
                return self.clock.get_time()

            release_ts = self.clock.send_event()
            self._in_critical_section = False
            req_ts = self._current_request_ts
            self._current_request_ts = None
            self.replies_received.clear()

            # Remove own request from head of queue
            self.request_queue = [
                r for r in self.request_queue if not (r[1] == self.node_id and r[0] == req_ts)
            ]

            self._log_event("CRITICAL_SECTION_RELEASED", release_ts, {
                "remaining_queue": list(self.request_queue),
            })
            logger.info("CS EXIT: Node %s EXITED Critical Section (Release L#%d)", self.node_id, release_ts)

            # Broadcast RELEASE
            msg = MutexMessage(
                msg_type="RELEASE",
                timestamp=release_ts,
                sender_id=self.node_id,
                target_id=None,
            )
            if self.send_callback:
                self.send_callback(msg)

            return release_ts

    def handle_message(self, msg: MutexMessage) -> Optional[MutexMessage]:
        """Process an incoming MutexMessage according to Lamport's rules."""
        with self._lock:
            # Advance local Lamport clock
            self.clock.receive_event(msg.timestamp)

            if msg.msg_type == "REQUEST":
                return self._handle_request(msg)
            elif msg.msg_type == "REPLY":
                self._handle_reply(msg)
                return None
            elif msg.msg_type == "RELEASE":
                self._handle_release(msg)
                return None
            else:
                logger.warning("Node %s received unknown message type: %s", self.node_id, msg.msg_type)
                return None

    def _handle_request(self, msg: MutexMessage) -> MutexMessage:
        """Insert foreign request into ordered queue and generate REPLY."""
        # Insert or deduplicate
        self._insert_request(msg.timestamp, msg.sender_id)

        # Prepare REPLY
        reply_ts = self.clock.send_event()
        reply_msg = MutexMessage(
            msg_type="REPLY",
            timestamp=reply_ts,
            sender_id=self.node_id,
            target_id=msg.sender_id,
        )
        self._log_event("REPLY_SENT", reply_ts, {
            "to": msg.sender_id,
            "for_req_ts": msg.timestamp,
        })
        return reply_msg

    def _handle_reply(self, msg: MutexMessage) -> None:
        """Record received reply from peer."""
        self.replies_received.add(msg.sender_id)
        self._log_event("REPLY_RECEIVED", self.clock.get_time(), {
            "from": msg.sender_id,
            "replies_count": len(self.replies_received),
        })

    def _handle_release(self, msg: MutexMessage) -> None:
        """Remove sender's request from local request queue."""
        # Remove matching entry
        old_queue = list(self.request_queue)
        # In Lamport mutex, a release clears the oldest pending request for sender
        for i, (ts, sender) in enumerate(self.request_queue):
            if sender == msg.sender_id:
                del self.request_queue[i]
                break

        self._log_event("RELEASE_PROCESSED", self.clock.get_time(), {
            "released_sender": msg.sender_id,
            "prev_queue": old_queue,
            "new_queue": list(self.request_queue),
        })

    def _insert_request(self, ts: int, sender_id: str) -> None:
        """Insert (ts, sender_id) into queue maintaining (timestamp, sender_id) total order."""
        item = (ts, sender_id)
        if item not in self.request_queue:
            self.request_queue.append(item)
            # Sort by Lamport timestamp first, then lexicographical process ID
            self.request_queue.sort(key=lambda x: (x[0], x[1]))

    def _log_event(self, event_type: str, ts: int, details: Dict[str, Any]) -> None:
        self.event_history.append({
            "event": event_type,
            "lamport_ts": ts,
            "time_iso": datetime.now(timezone.utc).isoformat(),
            "details": details,
        })


class MutexCluster:
    """Manages an interconnected group of LamportMutexNodes with reliable routing."""

    def __init__(self, node_ids: List[str]) -> None:
        self.node_ids = sorted([str(n) for n in node_ids])
        self.nodes: Dict[str, LamportMutexNode] = {}
        self.delivery_log: List[Dict[str, Any]] = []
        self._lock = threading.Lock()

        for nid in self.node_ids:
            self.nodes[nid] = LamportMutexNode(
                node_id=nid,
                all_node_ids=self.node_ids,
                send_callback=self._route_message,
            )

    def _route_message(self, msg: MutexMessage) -> None:
        """Route message reliably to targets."""
        with self._lock:
            self.delivery_log.append({
                "type": msg.msg_type,
                "sender": msg.sender_id,
                "target": msg.target_id or "ALL",
                "ts": msg.timestamp,
            })

        if msg.target_id:
            # Point-to-point delivery
            target_node = self.nodes.get(msg.target_id)
            if target_node:
                reply = target_node.handle_message(msg)
                if reply and self.nodes.get(reply.target_id):
                    self._route_message(reply)
        else:
            # Broadcast to all peers
            recipients = [nid for nid in self.node_ids if nid != msg.sender_id]
            for r_id in recipients:
                target_node = self.nodes.get(r_id)
                if target_node:
                    reply = target_node.handle_message(msg)
                    if reply and self.nodes.get(reply.target_id):
                        self._route_message(reply)

    def get_node(self, node_id: str) -> LamportMutexNode:
        return self.nodes[node_id]
