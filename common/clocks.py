"""
Logical Clock Primitives for Distributed Coordination (Unit III — Synchronization).

This module implements:
  1. LamportClock: Scalar logical clock for establishing a causal partial order
     of distributed events (Leslie Lamport, 1978).
  2. VectorClock: Vector clock for capturing full causal relationships and
     detecting concurrent events across named processes (Fidge / Mattern, 1988).
"""

import threading
from typing import Dict, List, Optional


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

    def __init__(self, process_id: str, all_process_ids: Optional[List[str]] = None) -> None:
        self.process_id = process_id
        self._lock = threading.Lock()
        known_ids = set(all_process_ids or [])
        known_ids.add(process_id)
        self.clock: Dict[str, int] = {pid: 0 for pid in sorted(known_ids)}

    def get_clock(self) -> Dict[str, int]:
        """Return a copy of the current vector clock state."""
        with self._lock:
            return dict(self.clock)

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
        with self._lock:
            for pid, count in received_vector.items():
                self.clock[pid] = max(self.clock.get(pid, 0), int(count))
            self.clock[self.process_id] = self.clock.get(self.process_id, 0) + 1
            return dict(self.clock)

    @staticmethod
    def compare(v1: Dict[str, int], v2: Dict[str, int]) -> str:
        """Compare two vector clock timestamps.

        Returns
        -------
        'EQUAL'      : v1 == v2
        'BEFORE'     : v1 < v2 (v1 causally precedes v2: v1 -> v2)
        'AFTER'      : v1 > v2 (v2 causally precedes v1: v2 -> v1)
        'CONCURRENT' : v1 || v2 (v1 and v2 are concurrent, independent events)
        """
        all_keys = set(v1.keys()).union(set(v2.keys()))
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
