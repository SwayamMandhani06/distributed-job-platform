"""
Ring Leader Election Algorithm (Chang & Roberts / Silberschatz) — Unit III Synchronization.

Theoretical Foundation:
-----------------------
In a Ring election algorithm, processes are organized in a logical unidirectional ring:
  P_0 -> P_1 -> P_2 -> ... -> P_{n-1} -> P_0.
Each process knows the structure of the ring and who its successor is.

Protocol Rules:
---------------
1. Initiation:
   When process P_i detects coordinator failure or initiates an election:
   - P_i creates an ELECTION message with an active candidate list: [P_i].
   - P_i forwards this message to its immediate active successor in the ring.
2. Circulation & Candidate Collection:
   When active process P_j receives an ELECTION(candidates) message:
   - If P_j is not yet in candidates, P_j appends itself: candidates.append(P_j).
   - P_j forwards the updated message to its active successor.
3. Completion:
   When the ELECTION message returns to the initiator (completes a full circuit):
   - The initiator inspects the collected candidates.
   - The candidate with the highest priority/ID is elected COORDINATOR.
4. Coordinator Announcement:
   - The initiator creates a COORDINATOR(elected_id) message and circulates it around the ring.
   - Each node records the coordinator and forwards the announcement until it completes the ring.
5. Fault Tolerance (Node Crash / Successor Bypass):
   - If a process's immediate successor is dead/unresponsive, the process skips the dead node
     and forwards the message to the next living node in the ring.
"""

from typing import Dict, List, Optional, Tuple


class RingProcess:
    """Individual process node in a logical unidirectional ring."""

    def __init__(self, process_id: str, priority: int, ring: "RingCluster") -> None:
        self.process_id = process_id
        self.priority = int(priority)
        self.ring = ring
        self.is_alive = True
        self.current_coordinator: Optional[str] = None

    def fail(self) -> None:
        """Simulate process failure."""
        self.is_alive = False
        self.ring.log_trace(f"[{self.process_id}] CRASHED (offline)")

    def recover(self) -> None:
        """Simulate process reboot."""
        self.is_alive = True
        self.ring.log_trace(f"[{self.process_id}] RECOVERED (online)")

    def start_election(self) -> str:
        """Initiate election by creating candidate list and circulating around ring."""
        if not self.is_alive:
            return ""

        self.ring.log_trace(f"[{self.process_id}] (Pri={self.priority}) initiated RING ELECTION")
        candidates: List[Tuple[str, int]] = [(self.process_id, self.priority)]

        # Find first active successor
        successor = self.ring.get_next_active_successor(self.process_id)
        if not successor or successor.process_id == self.process_id:
            # Lone survivor
            self.current_coordinator = self.process_id
            self.ring.log_trace(f"[{self.process_id}] is the ONLY active node in the ring. Self-appointed COORDINATOR.")
            return self.process_id

        # Circulate election message around ring until it returns to initiator
        curr = successor
        while curr.process_id != self.process_id:
            self.ring.log_trace(f"  -> [{curr.process_id}] (Pri={curr.priority}) visited by election token")
            candidates.append((curr.process_id, curr.priority))
            curr = self.ring.get_next_active_successor(curr.process_id)
            if not curr:
                break

        # Completed full circuit!
        self.ring.log_trace(f"Election token completed circuit back to [{self.process_id}].")
        self.ring.log_trace(f"Collected live candidates: {candidates}")

        # Select candidate with highest priority
        elected_id, elected_pri = max(candidates, key=lambda c: c[1])
        self.ring.log_trace(f"*** Node [{elected_id}] with highest priority ({elected_pri}) ELECTED COORDINATOR ***")

        # Announce coordinator around the ring
        self._announce_coordinator(elected_id)
        return elected_id

    def _announce_coordinator(self, elected_id: str) -> None:
        """Circulate COORDINATOR message around the ring."""
        self.current_coordinator = elected_id
        curr = self.ring.get_next_active_successor(self.process_id)
        while curr and curr.process_id != self.process_id:
            curr.current_coordinator = elected_id
            self.ring.log_trace(f"  <- [{curr.process_id}] received COORDINATOR announcement: leader=[{elected_id}]")
            curr = self.ring.get_next_active_successor(curr.process_id)


class RingCluster:
    """Manages a logical ring of RingProcess nodes."""

    def __init__(self) -> None:
        self.ring_order: List[str] = []
        self.processes: Dict[str, RingProcess] = {}
        self.trace_logs: List[str] = []

    def add_process(self, process_id: str, priority: int) -> RingProcess:
        """Add a process to the ring in order."""
        proc = RingProcess(process_id, priority, self)
        self.processes[process_id] = proc
        self.ring_order.append(process_id)
        return proc

    def get_process(self, process_id: str) -> Optional[RingProcess]:
        return self.processes.get(process_id)

    def log_trace(self, message: str) -> None:
        self.trace_logs.append(message)

    def get_trace(self) -> List[str]:
        return list(self.trace_logs)

    def clear_trace(self) -> None:
        self.trace_logs.clear()

    def get_next_active_successor(self, current_id: str) -> Optional[RingProcess]:
        """Find the next living successor in the ring, skipping dead/crashed nodes."""
        if current_id not in self.ring_order:
            return None

        idx = self.ring_order.index(current_id)
        n = len(self.ring_order)

        # Check subsequent nodes up to a full cycle
        for i in range(1, n + 1):
            next_idx = (idx + i) % n
            next_id = self.ring_order[next_idx]
            proc = self.processes[next_id]
            if proc.is_alive:
                if i > 1:
                    # Log failure bypass
                    bypassed = [self.ring_order[(idx + k) % n] for k in range(1, i)]
                    self.log_trace(f"  [Bypass] Node [{current_id}] bypassed dead node(s) {bypassed} -> found live successor [{next_id}]")
                return proc
        return None

    def get_active_coordinator(self) -> Optional[str]:
        """Return coordinator if all active processes have converged."""
        active = [p for p in self.processes.values() if p.is_alive]
        if not active:
            return None
        coordinators = {p.current_coordinator for p in active}
        if len(coordinators) == 1:
            return next(iter(coordinators))
        return None
