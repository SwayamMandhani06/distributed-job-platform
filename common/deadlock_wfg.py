"""
Wait-For Graph (WFG) Deadlock Detection (Unit III — Synchronization).

Theoretical Background (Distributed Deadlock Detection):
---------------------------------------------------------
In a distributed system where processes coordinate access to shared, non-preemptible
resources, deadlocks occur when a set of processes are blocked waiting for resources
held by other processes in a circular dependency.

Four Coffman Conditions for Deadlock:
1. Mutual Exclusion (resources held exclusively).
2. Hold and Wait (processes holding resources request new ones).
3. No Preemption (resources cannot be forcibly taken).
4. Circular Wait (a closed directed cycle of waiting exists).

Centralized vs. Distributed Deadlock Detection:
----------------------------------------------
1. Centralized Detector (Implemented Here):
   A central coordinator collects resource-allocation and wait-for dependencies from
   participating nodes into a global Wait-For Graph (WFG).
   Vertices represent processes; a directed edge P_i -> P_j indicates P_i is waiting
   for a resource currently held by P_j.
   Deadlock exists if and only if the WFG contains a directed cycle.

2. Distributed Detectors (e.g., Chandy-Misra-Haas Edge-Chasing):
   In large-scale systems without a centralized coordinator, processes detect deadlocks
   by circulating "probe" messages (initiator, sender, receiver) along wait-for edges.
   If an initiator receives its own probe back, a cycle is proven.

Note & Educational Scope:
-------------------------
This module provides an educational, centralized Wait-For Graph (WFG) cycle detector.
It does NOT implement distributed edge-chasing (Chandy-Misra-Haas). In an asynchronous
network, centralized detectors can suffer from "phantom deadlocks" if delay causes the
coordinator to observe transient dependencies that have already been resolved.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

from common.logging_config import setup_logger

logger = setup_logger("deadlock")


@dataclass
class DeadlockReport:
    """Diagnostic report from a Wait-For Graph evaluation."""
    is_deadlocked: bool
    cycles: List[List[str]]
    formatted_cycle_paths: List[str]
    deadlocked_processes: List[str]
    total_processes: int
    timestamp_iso: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class WaitForGraph:
    """Represents a Wait-For Graph (WFG) for detecting circular wait deadlocks."""

    def __init__(self) -> None:
        self.processes: Set[str] = set()
        # adjacency list: waiter -> set of processes being waited on
        self.adjacency: Dict[str, Set[str]] = {}
        # Metadata: (waiter, holder) -> resource_name
        self.edge_resources: Dict[Tuple[str, str], str] = {}
        # Resource allocation: resource_id -> holder_process_id
        self.resource_allocated_to: Dict[str, str] = {}

    def add_process(self, process_id: str) -> None:
        """Register a process in the graph."""
        self.processes.add(str(process_id))
        if str(process_id) not in self.adjacency:
            self.adjacency[str(process_id)] = set()

    def add_wait_dependency(
        self,
        waiter_pid: str,
        holder_pid: str,
        resource_id: Optional[str] = None,
    ) -> None:
        """Add a directed edge: waiter_pid is waiting for holder_pid to release a resource."""
        waiter = str(waiter_pid)
        holder = str(holder_pid)
        self.add_process(waiter)
        self.add_process(holder)
        self.adjacency[waiter].add(holder)
        if resource_id:
            self.edge_resources[(waiter, holder)] = str(resource_id)
        logger.debug("Added WFG edge: %s -> %s (resource=%s)", waiter, holder, resource_id)

    def remove_wait_dependency(self, waiter_pid: str, holder_pid: str) -> bool:
        """Remove a directed dependency edge."""
        waiter = str(waiter_pid)
        holder = str(holder_pid)
        if waiter in self.adjacency and holder in self.adjacency[waiter]:
            self.adjacency[waiter].remove(holder)
            self.edge_resources.pop((waiter, holder), None)
            return True
        return False

    def request_resource(self, process_id: str, resource_id: str) -> bool:
        """Simulate a process requesting a resource.

        If resource is free: process acquires it (returns True).
        If resource is held by another process: adds wait-for edge (returns False).
        """
        pid = str(process_id)
        res = str(resource_id)
        self.add_process(pid)

        holder = self.resource_allocated_to.get(res)
        if holder is None:
            # Resource is available
            self.resource_allocated_to[res] = pid
            logger.info("Process %s ACQUIRED resource %s", pid, res)
            return True
        elif holder == pid:
            # Process already holds this resource
            return True
        else:
            # Resource is locked by holder; pid must wait
            self.add_wait_dependency(pid, holder, resource_id=res)
            logger.warning("Process %s BLOCKED waiting for %s held by %s", pid, res, holder)
            return False

    def release_resource(self, process_id: str, resource_id: str) -> bool:
        """Process releases a resource, clearing wait-for dependencies on this resource."""
        pid = str(process_id)
        res = str(resource_id)
        if self.resource_allocated_to.get(res) != pid:
            return False

        del self.resource_allocated_to[res]
        # Remove wait dependencies from all waiters waiting on this process for this resource
        edges_to_remove = [
            (waiter, holder)
            for (waiter, holder), r in self.edge_resources.items()
            if holder == pid and r == res
        ]
        for w, h in edges_to_remove:
            self.remove_wait_dependency(w, h)

        logger.info("Process %s RELEASED resource %s (cleared %d waiting edges)", pid, res, len(edges_to_remove))
        return True

    def find_all_cycles(self) -> List[List[str]]:
        """Detect all elementary directed cycles in the Wait-For Graph using DFS 3-coloring.

        Color conventions:
          0 = WHITE (unvisited)
          1 = GRAY  (currently in recursion stack / active DFS path)
          2 = BLACK (completely explored)

        Returns
        -------
        list of list of str
            Each list represents a detected cycle sequence, e.g. ['P1', 'P2', 'P3', 'P1'].
        """
        visited: Dict[str, int] = {p: 0 for p in self.processes}
        path: List[str] = []
        cycles: List[List[str]] = []
        seen_cycle_fingerprints: Set[str] = set()

        def dfs(u: str) -> None:
            visited[u] = 1  # GRAY: active in path
            path.append(u)

            for v in sorted(list(self.adjacency.get(u, set()))):
                if v not in visited:
                    continue
                if visited[v] == 1:
                    # Found back-edge! Reconstruct cycle from path stack
                    if v in path:
                        cycle_start_idx = path.index(v)
                        cycle = path[cycle_start_idx:] + [v]
                        # Canonical fingerprint to deduplicate rotated variants of identical cycle
                        nodes_in_cycle = cycle[:-1]
                        min_node_idx = nodes_in_cycle.index(min(nodes_in_cycle))
                        canonical = nodes_in_cycle[min_node_idx:] + nodes_in_cycle[:min_node_idx]
                        fp = "->".join(canonical)
                        if fp not in seen_cycle_fingerprints:
                            seen_cycle_fingerprints.add(fp)
                            cycles.append(cycle)
                elif visited[v] == 0:
                    dfs(v)

            path.pop()
            visited[u] = 2  # BLACK: completed

        for p in sorted(list(self.processes)):
            if visited.get(p) == 0:
                dfs(p)

        return cycles

    def get_formatted_cycle_paths(self) -> List[str]:
        """Return human-readable cycle representations with resource labels where available."""
        cycles = self.find_all_cycles()
        formatted = []
        for cyc in cycles:
            parts = []
            for i in range(len(cyc) - 1):
                u = cyc[i]
                v = cyc[i + 1]
                res = self.edge_resources.get((u, v))
                if res:
                    parts.append(f"{u} -[waits on {res}]->")
                else:
                    parts.append(f"{u} ->")
            parts.append(cyc[-1])
            formatted.append(" ".join(parts))
        return formatted

    def detect_deadlock(self) -> DeadlockReport:
        """Perform deadlock detection and return a comprehensive DeadlockReport."""
        cycles = self.find_all_cycles()
        formatted = self.get_formatted_cycle_paths()
        deadlocked_nodes = sorted(list({node for cycle in cycles for node in cycle[:-1]}))
        return DeadlockReport(
            is_deadlocked=len(cycles) > 0,
            cycles=cycles,
            formatted_cycle_paths=formatted,
            deadlocked_processes=deadlocked_nodes,
            total_processes=len(self.processes),
        )

    def is_deadlocked(self) -> bool:
        """Boolean check whether any cycle exists in the Wait-For Graph."""
        return len(self.find_all_cycles()) > 0

    def resolve_deadlock_by_aborting(self, victim_pid: str) -> bool:
        """Simulate preemption / abort of a victim process to break circular wait."""
        victim = str(victim_pid)
        if victim not in self.processes:
            return False

        # 1. Release all resources held by victim
        held_resources = [r for r, holder in self.resource_allocated_to.items() if holder == victim]
        for res in held_resources:
            self.release_resource(victim, res)

        # 2. Clear all outgoing wait edges from victim
        self.adjacency[victim].clear()

        # 3. Clear all incoming wait edges toward victim
        for p in self.processes:
            self.adjacency[p].discard(victim)

        # 4. Clean up edge_resources
        keys_to_delete = [edge for edge in self.edge_resources if victim in edge]
        for k in keys_to_delete:
            del self.edge_resources[k]

        logger.warning("ABORTED victim process %s to resolve deadlock (freed resources: %s)", victim, held_resources)
        return True


class TaskAbortedException(Exception):
    """Raised when a concurrent task is chosen as a deadlock victim and preempted/aborted."""
    pass


@dataclass
class DeadlockResolution:
    """Diagnostic outcome of resolving a detected deadlock."""
    detected_cycle: List[str]
    formatted_cycle: str
    victim_task_id: str
    freed_resources: List[str]
    remaining_tasks: List[str]
    timestamp_iso: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class ConcurrentResourceManager:
    """Thread-safe resource manager for concurrent tasks with live WFG generation and victim abort.

    Explicit Algorithm Classification:
    ----------------------------------
    This is an authoritative **Centralized Wait-For Graph (WFG) Detector with Victim Abort**.
    It is NOT distributed edge-chasing (Chandy-Misra-Haas). Centralized resource management
    is the standard architecture for lock managers in databases (e.g. Postgres lock manager)
    and distributed cluster schedulers.
    """

    def __init__(self) -> None:
        import threading
        from collections import defaultdict
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)

        # Resource allocation: resource_id -> holder_task_id (or None)
        self.resources: Dict[str, Optional[str]] = {}
        # Waiters: resource_id -> set of waiting task_ids
        self.waiters: Dict[str, Set[str]] = defaultdict(set)
        # Task ownership: task_id -> set of held resource_ids
        self.task_held_resources: Dict[str, Set[str]] = defaultdict(set)
        # Abort signals: task_id -> bool
        self.task_abort_flags: Dict[str, bool] = defaultdict(bool)
        # Task birth/registration: task_id -> float timestamp
        self.task_timestamps: Dict[str, float] = {}

    def register_resource(self, resource_id: str) -> None:
        """Register a shared resource."""
        with self._lock:
            if resource_id not in self.resources:
                self.resources[str(resource_id)] = None

    def register_task(self, task_id: str) -> None:
        """Register a concurrent task."""
        with self._lock:
            tid = str(task_id)
            if tid not in self.task_timestamps:
                import time
                self.task_timestamps[tid] = time.time()
                self.task_abort_flags[tid] = False

    def acquire(self, task_id: str, resource_id: str, timeout_seconds: Optional[float] = None) -> bool:
        """Request exclusive acquisition of resource_id.

        Blocks the calling task/thread until the resource becomes available or the task is aborted.
        Raises TaskAbortedException if the task is selected as a deadlock victim.
        """
        tid = str(task_id)
        rid = str(resource_id)
        import time
        start_time = time.time()

        with self._condition:
            self.register_task(tid)
            self.register_resource(rid)

            if self.task_abort_flags[tid]:
                raise TaskAbortedException(f"Task {tid} has been aborted by deadlock resolver.")

            # If already held by this task, idempotent success
            if self.resources[rid] == tid:
                return True

            # Register as waiter
            self.waiters[rid].add(tid)
            logger.info("Task %s is WAITING for resource %s (currently held by %s)", tid, rid, self.resources.get(rid))

            try:
                while self.resources[rid] is not None and self.resources[rid] != tid:
                    if self.task_abort_flags[tid]:
                        raise TaskAbortedException(f"Task {tid} aborted while waiting for {rid}.")

                    if timeout_seconds is not None:
                        remaining = timeout_seconds - (time.time() - start_time)
                        if remaining <= 0:
                            logger.warning("Task %s timed out waiting for resource %s", tid, rid)
                            return False
                        self._condition.wait(timeout=remaining)
                    else:
                        self._condition.wait()

                    if self.task_abort_flags[tid]:
                        raise TaskAbortedException(f"Task {tid} aborted while waiting for {rid}.")

                # Acquired!
                self.resources[rid] = tid
                self.task_held_resources[tid].add(rid)
                logger.info("Task %s ACQUIRED resource %s", tid, rid)
                return True
            finally:
                self.waiters[rid].discard(tid)

    def release(self, task_id: str, resource_id: str) -> bool:
        """Release a previously acquired resource and wake up waiters."""
        tid = str(task_id)
        rid = str(resource_id)
        with self._condition:
            if self.resources.get(rid) != tid:
                return False

            self.resources[rid] = None
            self.task_held_resources[tid].discard(rid)
            logger.info("Task %s RELEASED resource %s", tid, rid)
            self._condition.notify_all()
            return True

    def build_wfg(self) -> WaitForGraph:
        """Construct a live snapshot of the Wait-For Graph from active allocations and waiters."""
        with self._lock:
            wfg = WaitForGraph()
            # Register all known tasks
            for tid in self.task_timestamps:
                wfg.add_process(tid)

            # Record allocations
            for rid, holder in self.resources.items():
                if holder:
                    wfg.resource_allocated_to[rid] = holder

            # Record wait dependencies
            for rid, waiting_tasks in self.waiters.items():
                holder = self.resources.get(rid)
                if holder:
                    for waiter in waiting_tasks:
                        wfg.add_wait_dependency(waiter, holder, resource_id=rid)

            return wfg

    def detect_deadlock(self) -> DeadlockReport:
        """Generate live WFG and evaluate cycles."""
        wfg = self.build_wfg()
        return wfg.detect_deadlock()

    def abort_task(self, victim_task_id: str) -> List[str]:
        """Forcibly preempt/abort a task, releasing all its held resources and waking waiters."""
        victim = str(victim_task_id)
        with self._condition:
            self.task_abort_flags[victim] = True
            freed = list(self.task_held_resources.get(victim, set()))

            for rid in freed:
                self.resources[rid] = None
                self.task_held_resources[victim].discard(rid)
                logger.warning("FORCIBLY FREED resource %s from aborted task %s", rid, victim)

            # Clear waiter entries for victim
            for rid in self.waiters:
                self.waiters[rid].discard(victim)

            self._condition.notify_all()
            return freed

    def detect_and_resolve(self, victim_policy: str = "lexicographical") -> Optional[DeadlockResolution]:
        """Detect circular wait in live WFG and resolve by aborting a designated victim task."""
        with self._condition:
            report = self.detect_deadlock()
            if not report.is_deadlocked:
                return None

            primary_cycle = report.cycles[0]
            cycle_nodes = primary_cycle[:-1]

            # Victim selection policy
            if victim_policy == "youngest":
                # Pick task with latest registration timestamp
                victim = max(cycle_nodes, key=lambda t: self.task_timestamps.get(t, 0.0))
            else:
                # Lexicographical highest ID
                victim = sorted(cycle_nodes)[-1]

            formatted_path = report.formatted_cycle_paths[0] if report.formatted_cycle_paths else " -> ".join(primary_cycle)
            freed_resources = self.abort_task(victim)
            remaining_tasks = [t for t in self.task_timestamps if not self.task_abort_flags[t]]

            logger.error(
                "DEADLOCK RESOLVED: Cycle '%s' broken by aborting victim '%s' (freed: %s)",
                formatted_path,
                victim,
                freed_resources,
            )

            return DeadlockResolution(
                detected_cycle=primary_cycle,
                formatted_cycle=formatted_path,
                victim_task_id=victim,
                freed_resources=freed_resources,
                remaining_tasks=remaining_tasks,
            )

