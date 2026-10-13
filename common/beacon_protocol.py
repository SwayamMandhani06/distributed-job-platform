"""
Beacon Protocol and Liveness Observation (Unit III — Synchronization).

Theoretical Background (Failure Detection & Beaconing in Distributed Systems):
-------------------------------------------------------------------------------
In an asynchronous distributed system, processes do not share a common clock or memory.
Nodes determine the operational state of remote participants using periodic liveness signals.

Beacon Protocol Concept:
------------------------
A **Beacon** is a lightweight, periodic, one-way message broadcast by a node to advertise:
  * Its identity (node_id)
  * Monotonic sequence number (sequence_number)
  * Emission physical timestamp
  * Optional health telemetry (CPU load, queue depth)

Receivers (Monitors) maintain a sliding observation window of received beacons:
  * If a beacon is received within expected interval T_beacon: node is HEALTHY.
  * If consecutive beacons are missed >= k_suspect: node transitions to SUSPECTED.
  * If beacons resume arriving: node transitions from SUSPECTED back to HEALTHY (Recovery).
  * If silence continues beyond k_dead: node transitions to DEAD.

Distinction from Existing Worker Heartbeat:
-------------------------------------------
1. Worker Heartbeat (Phase 1-3.2 implementation):
   - Database-backed pull/poll pattern: Worker nodes write active rows into SQLite
     (`UPDATE workers SET last_heartbeat = ?`).
   - The scheduler queries persistent storage periodically.
   - Tied directly to job lease ownership and claim tokens.

2. Beacon Protocol (This module):
   - Message-oriented, connectionless liveness dissemination pattern.
   - Operates independent of database transactions and job queues.
   - Models Unreliable Failure Detectors (Chandra-Toueg <>P / <>W hierarchy).

Crucial Distributed Systems Limitation:
---------------------------------------
A missed beacon is NOT mathematical proof that a node has crashed!
In an asynchronous network with unpredictable latency:
  * Network packet loss
  * Temporary network partitioning
  * Operating system thread starvation or Garbage Collection (GC) pauses
can cause beacons to be delayed. The monitor must treat a missed threshold as a
**suspicion**, which can be refuted if the node later resumes transmission.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from common.logging_config import setup_logger

logger = setup_logger("beacon")


@dataclass
class BeaconPacket:
    """A periodic liveness beacon emitted by a node."""
    node_id: str
    sequence_number: int
    timestamp_sent: float
    status: str = "UP"
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __repr__(self) -> str:
        return f"[BEACON #{self.sequence_number} from {self.node_id} @ {self.timestamp_sent:.2f}]"


@dataclass
class NodeLivenessStatus:
    """Evaluated liveness state of a remote node."""
    node_id: str
    state: str  # "HEALTHY", "SUSPECTED", "DEAD"
    last_seen_timestamp: float
    last_sequence_number: int
    missed_beacons: int
    elapsed_seconds: float
    state_changed: bool = False
    previous_state: Optional[str] = None


class BeaconEmitter:
    """Emits periodic beacon packets for a single node."""

    def __init__(self, node_id: str, beacon_interval_seconds: float = 1.0) -> None:
        self.node_id = str(node_id)
        self.beacon_interval_seconds = float(beacon_interval_seconds)
        self.sequence_number = 0
        self.is_active = True

    def emit_beacon(
        self,
        current_time: float,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[BeaconPacket]:
        """Generate the next periodic beacon packet if active."""
        if not self.is_active:
            return None

        self.sequence_number += 1
        packet = BeaconPacket(
            node_id=self.node_id,
            sequence_number=self.sequence_number,
            timestamp_sent=current_time,
            status="UP",
            metadata=metadata or {},
        )
        logger.debug("Emitted %s", packet)
        return packet

    def stop(self) -> None:
        """Simulate node stopping or crashing."""
        self.is_active = False

    def resume(self) -> None:
        """Simulate node recovering from pause or partition."""
        self.is_active = True


class BeaconMonitor:
    """Monitors liveness beacons across multiple nodes and evaluates failure suspicion."""

    def __init__(
        self,
        beacon_interval_seconds: float = 1.0,
        missed_beacon_threshold: int = 3,
        dead_threshold_missed: int = 6,
    ) -> None:
        self.beacon_interval_seconds = float(beacon_interval_seconds)
        self.missed_beacon_threshold = int(missed_beacon_threshold)
        self.dead_threshold_missed = int(dead_threshold_missed)

        # Registered tracking: node_id -> {last_seen, last_seq, state, ...}
        self.nodes: Dict[str, Dict[str, Any]] = {}
        self.event_log: List[str] = []

    def register_node(self, node_id: str, initial_time: float = 0.0) -> None:
        """Register a node for beacon monitoring."""
        nid = str(node_id)
        self.nodes[nid] = {
            "last_seen_timestamp": float(initial_time),
            "last_sequence_number": 0,
            "state": "HEALTHY",
            "missed_beacons": 0,
        }

    def record_beacon(
        self,
        packet: BeaconPacket,
        arrival_time: Optional[float] = None,
    ) -> NodeLivenessStatus:
        """Record receipt of a beacon packet and update node liveness state."""
        nid = packet.node_id
        arrival = float(arrival_time if arrival_time is not None else packet.timestamp_sent)

        if nid not in self.nodes:
            self.nodes[nid] = {
                "last_seen_timestamp": arrival,
                "last_sequence_number": packet.sequence_number,
                "state": "HEALTHY",
                "missed_beacons": 0,
            }

        node_data = self.nodes[nid]
        prev_state = node_data["state"]

        # Update last seen timestamps and sequence
        node_data["last_seen_timestamp"] = arrival
        node_data["last_sequence_number"] = packet.sequence_number
        node_data["missed_beacons"] = 0

        # State transition: if node was SUSPECTED or DEAD, receipt of beacon recovers it to HEALTHY!
        state_changed = False
        if prev_state in ("SUSPECTED", "DEAD"):
            node_data["state"] = "HEALTHY"
            state_changed = True
            msg = f"NODE RECOVERED: {nid} transitioned from {prev_state} -> HEALTHY (Seq #{packet.sequence_number})"
            self.event_log.append(msg)
            logger.info(msg)

        return NodeLivenessStatus(
            node_id=nid,
            state=node_data["state"],
            last_seen_timestamp=arrival,
            last_sequence_number=packet.sequence_number,
            missed_beacons=0,
            elapsed_seconds=0.0,
            state_changed=state_changed,
            previous_state=prev_state if state_changed else None,
        )

    def evaluate_liveness(self, current_time: float) -> Dict[str, NodeLivenessStatus]:
        """Evaluate liveness for all monitored nodes at the specified current_time."""
        now = float(current_time)
        results = {}

        for nid, data in self.nodes.items():
            last_seen = data["last_seen_timestamp"]
            prev_state = data["state"]
            elapsed = max(0.0, now - last_seen)
            missed = int(elapsed // self.beacon_interval_seconds)
            data["missed_beacons"] = missed

            new_state = prev_state
            if missed >= self.dead_threshold_missed:
                new_state = "DEAD"
            elif missed >= self.missed_beacon_threshold:
                new_state = "SUSPECTED"
            else:
                new_state = "HEALTHY"

            state_changed = (new_state != prev_state)
            data["state"] = new_state

            if state_changed:
                msg = f"LIVENESS CHANGE: {nid} transitioned from {prev_state} -> {new_state} (missed={missed}, elapsed={elapsed:.1f}s)"
                self.event_log.append(msg)
                if new_state == "SUSPECTED":
                    logger.warning(msg)
                elif new_state == "DEAD":
                    logger.error(msg)

            results[nid] = NodeLivenessStatus(
                node_id=nid,
                state=new_state,
                last_seen_timestamp=last_seen,
                last_sequence_number=data["last_sequence_number"],
                missed_beacons=missed,
                elapsed_seconds=elapsed,
                state_changed=state_changed,
                previous_state=prev_state if state_changed else None,
            )

        return results
