"""
Bully Leader Election Algorithm (Garcia-Molina, 1982) — Unit III Synchronization.

Theoretical Foundation:
-----------------------
The Bully algorithm elects the process with the highest priority/ID among all active
processes. The algorithm is called "Bully" because higher-priority processes always
override and take leadership away from lower-priority processes.

Protocol Rules:
---------------
1. When process P notices coordinator has failed or initiates an election:
   - P sends an ELECTION message to all processes with higher priority (P_j where priority_j > priority_P).
2. If no process responds within timeout T:
   - P assumes all higher processes are dead, wins the election, and broadcasts
     COORDINATOR message to all lower-priority processes.
3. If any higher process responds with ANSWER (OK):
   - P's candidacy is halted. P waits for a COORDINATOR message from a higher process.
   - The answering higher process concurrently initiates its own election.
4. When a recovered process with high priority wakes up, it immediately initiates
   an election and "bullies" any existing lower-priority coordinator out of office.

Note on Simulation vs Production:
---------------------------------
This module provides a pure, deterministic in-memory distributed actor cluster
with traceable message queues and simulated failure injection for academic viva defense.
"""

from typing import Callable, Dict, List, Optional, Tuple
import threading
import time


class BullyProcess:
    """Individual node participating in a Bully election cluster."""

    def __init__(self, process_id: str, priority: int, cluster: "BullyCluster") -> None:
        self.process_id = process_id
        self.priority = int(priority)
        self.cluster = cluster
        self.is_alive = True
        self.current_coordinator: Optional[str] = None
        self.election_in_progress = False

    def fail(self) -> None:
        """Simulate abrupt process crash."""
        self.is_alive = False
        self.election_in_progress = False
        self.cluster.log_trace(f"[{self.process_id}] CRASHED (is_alive=False)")

    def recover(self) -> None:
        """Simulate process reboot."""
        self.is_alive = True
        self.election_in_progress = False
        self.cluster.log_trace(f"[{self.process_id}] RECOVERED (priority={self.priority})")

    def start_election(self) -> None:
        """Initiate Bully election from this process."""
        if not self.is_alive:
            return

        self.cluster.log_trace(f"[{self.process_id}] (Pri={self.priority}) initiated ELECTION")
        self.election_in_progress = True

        higher_processes = [
            p for p in self.cluster.get_all_processes()
            if p.priority > self.priority
        ]

        if not higher_processes:
            # No higher process exists; declare victory immediately
            self.declare_coordinator()
            return

        answered = False
        for p in higher_processes:
            self.cluster.log_trace(f"  -> [{self.process_id}] sent ELECTION to [{p.process_id}] (Pri={p.priority})")
            if p.receive_election(sender=self):
                answered = True

        if not answered:
            # None of the higher processes answered; declare victory
            self.cluster.log_trace(f"[{self.process_id}] received NO answers from higher processes. Declaring victory!")
            self.declare_coordinator()
        else:
            self.cluster.log_trace(f"[{self.process_id}] received ANSWER from higher process. Stepping down and waiting.")
            self.election_in_progress = False

    def receive_election(self, sender: "BullyProcess") -> bool:
        """Handle incoming ELECTION message from a lower-priority process."""
        if not self.is_alive:
            return False

        self.cluster.log_trace(f"  <- [{self.process_id}] received ELECTION from [{sender.process_id}]. Responded ANSWER (OK).")

        # Asynchronously or deterministically start our own election
        if not self.election_in_progress:
            self.start_election()
        return True

    def declare_coordinator(self) -> None:
        """Broadcast COORDINATOR message to all other active processes."""
        if not self.is_alive:
            return

        self.current_coordinator = self.process_id
        self.election_in_progress = False
        self.cluster.log_trace(f"*** [{self.process_id}] WON ELECTION and declared itself COORDINATOR ***")

        for p in self.cluster.get_all_processes():
            if p.process_id != self.process_id:
                p.receive_coordinator(coordinator_id=self.process_id)

    def receive_coordinator(self, coordinator_id: str) -> None:
        """Handle incoming COORDINATOR announcement."""
        if not self.is_alive:
            return

        self.current_coordinator = coordinator_id
        self.election_in_progress = False
        self.cluster.log_trace(f"  <- [{self.process_id}] accepted coordinator [{coordinator_id}]")


class BullyCluster:
    """Coordinator cluster managing a group of BullyProcess nodes."""

    def __init__(self) -> None:
        self.processes: Dict[str, BullyProcess] = {}
        self.trace_logs: List[str] = []

    def add_process(self, process_id: str, priority: int) -> BullyProcess:
        """Register a new process into the cluster."""
        proc = BullyProcess(process_id, priority, self)
        self.processes[process_id] = proc
        return proc

    def get_process(self, process_id: str) -> Optional[BullyProcess]:
        return self.processes.get(process_id)

    def get_all_processes(self) -> List[BullyProcess]:
        return list(self.processes.values())

    def log_trace(self, message: str) -> None:
        self.trace_logs.append(message)

    def get_trace(self) -> List[str]:
        return list(self.trace_logs)

    def clear_trace(self) -> None:
        self.trace_logs.clear()

    def get_active_coordinator(self) -> Optional[str]:
        """Return coordinator recognized by active processes (if in consensus)."""
        active = [p for p in self.processes.values() if p.is_alive]
        if not active:
            return None
        coordinators = {p.current_coordinator for p in active}
        if len(coordinators) == 1:
            return next(iter(coordinators))
        return None
