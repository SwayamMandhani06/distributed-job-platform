"""
Logical Clock Primitives for Distributed Coordination (Unit III — Synchronization).

This module implements:
  1. LamportClock: Scalar logical clock for establishing a causal partial order
     of distributed events (Leslie Lamport, 1978).
  2. VectorClock: Vector clock for capturing full causal relationships and
     detecting concurrent events across named processes (Fidge / Mattern, 1988).
"""

import threading
from typing import Any, Dict, List, Optional


class LamportClock:
    """Thread-safe Lamport Logical Clock.

    Rules:
      1. Increment the local counter before recording a local event or sending a message:
         L = L + 1
      2. On receiving a message with timestamp T, advance local clock:
         L = max(L, T) + 1
    """

    def __init__(self, initial_time: int = 0) -> None:
        self._time: int = int(initial_time)
        self._lock = threading.Lock()

    def get_time(self) -> int:
        """Return the current logical clock timestamp."""
        with self._lock:
            return self._time

    def tick(self) -> int:
        """Rule 1: Increment for an internal local event and return the new timestamp."""
        with self._lock:
            self._time += 1
            return self._time

    def send_event(self) -> int:
        """Rule 1: Increment before sending a message and return the timestamp to attach."""
        with self._lock:
            self._time += 1
            return self._time

    def receive_event(self, received_time: int) -> int:
        """Rule 2: Update clock on message receipt: L = max(local, received) + 1."""
        with self._lock:
            self._time = max(self._time, int(received_time)) + 1
            return self._time

    def __repr__(self) -> str:
        return f"LamportClock(time={self.get_time()})"


class VectorClock:
    """Vector Clock for causal event tracking and concurrency detection.

    In a distributed system with N processes, each process P_i maintains a vector
    V_i where V_i[j] represents P_i's knowledge of the latest event at P_j.

    Rules:
      1. Local event on P_i: V_i[i] += 1
      2. Send event on P_i: V_i[i] += 1; attach V_i to outgoing message
      3. Receive event on P_i with vector V_msg:
         forall k: V_i[k] = max(V_i[k], V_msg[k])
         V_i[i] += 1
    """

    def __init__(
        self,
        process_id: str,
        all_process_ids: Optional[List[str]] = None,
        strict_membership: bool = False,
    ) -> None:
        self.process_id = process_id
        self.strict_membership = strict_membership
        self._lock = threading.Lock()
        known_ids = set(all_process_ids or [])
        known_ids.add(process_id)
        self.clock: Dict[str, int] = {pid: 0 for pid in sorted(known_ids)}

    def get_clock(self) -> Dict[str, int]:
        """Return a copy of the current vector clock state."""
        with self._lock:
            return dict(self.clock)

    def get_members(self) -> List[str]:
        """Return the current registered member process IDs."""
        with self._lock:
            return sorted(list(self.clock.keys()))

    def add_member(self, new_process_id: str) -> None:
        """Explicitly register a new participating process in the vector clock space."""
        with self._lock:
            if new_process_id not in self.clock:
                self.clock[new_process_id] = 0

    def remove_member(self, retired_process_id: str) -> None:
        """Explicitly retire a process from the vector clock space."""
        if retired_process_id == self.process_id:
            raise ValueError(f"Process cannot retire itself ({self.process_id}) from its own vector clock.")
        with self._lock:
            if retired_process_id in self.clock:
                del self.clock[retired_process_id]

    def local_event(self) -> Dict[str, int]:
        """Rule 1: Local event on this process."""
        with self._lock:
            self.clock[self.process_id] = self.clock.get(self.process_id, 0) + 1
            return dict(self.clock)

    def send_event(self) -> Dict[str, int]:
        """Rule 2: Send event on this process (increments local entry and returns vector)."""
        with self._lock:
            self.clock[self.process_id] = self.clock.get(self.process_id, 0) + 1
            return dict(self.clock)

    def receive_event(self, received_vector: Dict[str, int]) -> Dict[str, int]:
        """Rule 3: Receive message with incoming vector."""
        if not isinstance(received_vector, dict):
            raise ValueError(f"Received vector must be a dict, got {type(received_vector)}")

        with self._lock:
            if self.strict_membership:
                unknown = set(received_vector.keys()) - set(self.clock.keys())
                if unknown:
                    raise ValueError(
                        f"Strict membership violation: incoming vector contains unregistered process IDs: {unknown}"
                    )

            for pid, count in received_vector.items():
                self.clock[pid] = max(self.clock.get(pid, 0), int(count))
            self.clock[self.process_id] = self.clock.get(self.process_id, 0) + 1
            return dict(self.clock)

    @staticmethod
    def compare(v1: Dict[str, int], v2: Dict[str, int], strict_membership: bool = False) -> str:
        """Compare two vector clock timestamps.

        Parameters:
        -----------
        v1, v2 : dict
            Vector clocks mapping process_id -> counter.
        strict_membership : bool
            If True, rejects comparisons between vectors with unequal memberships.

        Returns:
        --------
        'EQUAL'      : v1 == v2
        'BEFORE'     : v1 < v2 (v1 causally precedes v2: v1 -> v2)
        'AFTER'      : v1 > v2 (v2 causally precedes v1: v2 -> v1)
        'CONCURRENT' : v1 || v2 (v1 and v2 are concurrent, independent events)
        """
        if not isinstance(v1, dict) or not isinstance(v2, dict):
            raise ValueError("Both arguments must be dictionary vector clocks")

        keys1 = set(v1.keys())
        keys2 = set(v2.keys())

        if strict_membership and keys1 != keys2:
            raise ValueError(
                f"Incompatible vector memberships: {sorted(list(keys1))} vs {sorted(list(keys2))}. "
                "Cannot compare vector clocks with unequal process dimensions under strict membership."
            )

        all_keys = keys1.union(keys2)
        v1_normalized = {k: v1.get(k, 0) for k in all_keys}
        v2_normalized = {k: v2.get(k, 0) for k in all_keys}

        if v1_normalized == v2_normalized:
            return "EQUAL"

        v1_le_v2 = all(v1_normalized[k] <= v2_normalized[k] for k in all_keys)
        v2_le_v1 = all(v2_normalized[k] <= v1_normalized[k] for k in all_keys)

        if v1_le_v2 and not v2_le_v1:
            return "BEFORE"
        elif v2_le_v1 and not v1_le_v2:
            return "AFTER"
        else:
            return "CONCURRENT"

    def __repr__(self) -> str:
        return f"VectorClock(process={self.process_id}, clock={self.get_clock()})"


class LamportTraceRecorder:
    """Thread-safe in-memory recorder for distributed Lamport event causal traces."""

    def __init__(self, max_events: int = 1000) -> None:
        self.max_events = max_events
        self._events: List[Dict[str, Any]] = []
        self._lock = threading.Lock()

    def record_event(
        self,
        process_id: str,
        event_type: str,
        lamport_ts: int,
        job_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Record a single Lamport clock event in the audit trail."""
        from datetime import datetime, timezone
        from uuid import uuid4

        event = {
            "event_id": str(uuid4()),
            "process_id": process_id,
            "event_type": event_type,
            "job_id": job_id,
            "lamport_ts": int(lamport_ts),
            "physical_timestamp": datetime.now(timezone.utc).isoformat(),
            "details": details or {},
        }
        with self._lock:
            self._events.append(event)
            if len(self._events) > self.max_events:
                self._events.pop(0)
        return event

    def get_events(self, job_id: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Retrieve recent Lamport events, optionally filtered by job_id."""
        with self._lock:
            if job_id:
                filtered = [e for e in self._events if e.get("job_id") == job_id]
            else:
                filtered = list(self._events)
            return filtered[-limit:]

    def clear(self) -> None:
        """Clear all recorded Lamport events."""
        with self._lock:
            self._events.clear()


# Global trace recorder singleton
_global_lamport_recorder = LamportTraceRecorder()


def record_lamport_event(
    process_id: str,
    event_type: str,
    lamport_ts: int,
    job_id: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Record a Lamport event in the global trace recorder."""
    return _global_lamport_recorder.record_event(
        process_id=process_id,
        event_type=event_type,
        lamport_ts=lamport_ts,
        job_id=job_id,
        details=details,
    )


def get_lamport_traces(job_id: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
    """Retrieve recorded Lamport events from the global trace recorder."""
    return _global_lamport_recorder.get_events(job_id=job_id, limit=limit)


def clear_lamport_traces() -> None:
    """Clear all recorded Lamport events."""
    _global_lamport_recorder.clear()
