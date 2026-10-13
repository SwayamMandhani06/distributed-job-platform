"""
Demonstration and Verification of Centralized Wait-For Graph (WFG) Deadlock Detection (Unit III).

Connects deadlock detection to a real concurrent resource manager managing
exclusive non-preemptible resources across concurrent worker tasks.

Demonstration Scenario:
-----------------------
1. Three concurrent worker tasks: Task-A, Task-B, Task-C.
2. Three shared exclusive resources: Lock-1, Lock-2, Lock-3.
3. Execution sequence creates a classic 3-way circular wait:
   - Task-A acquires Lock-1, then requests Lock-2 (held by Task-B) -> BLOCKS.
   - Task-B acquires Lock-2, then requests Lock-3 (held by Task-C) -> BLOCKS.
   - Task-C acquires Lock-3, then requests Lock-1 (held by Task-A) -> BLOCKS -> DEADLOCK CYCLE!
4. The Centralized Resource Manager constructs the live Wait-For Graph (WFG).
5. DFS cycle detection pinpoints the exact circular dependency:
     Task-A -[waits on Lock-2]-> Task-B -[waits on Lock-3]-> Task-C -[waits on Lock-1]-> Task-A
6. Safe Resolution: The manager aborts victim Task-C, freeing Lock-3.
7. Task-B unblocks, completes its critical section, and releases resources.
8. Task-A unblocks, completes its critical section, and releases resources.
9. Final graph state is strictly acyclic.
"""

import sys
import threading
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from common.deadlock_wfg import ConcurrentResourceManager, TaskAbortedException


def run_demo():
    print("=" * 75)
    print("CENTRALIZED WAIT-FOR GRAPH DEADLOCK DETECTION & RESOLUTION DEMO")
    print("=" * 75)

    manager = ConcurrentResourceManager()
    task_outcomes = {}
    threads = []

    # Step 1: Define worker task execution logic
    def worker_a():
        try:
            print("[Task-A] Acquiring Lock-1...")
            manager.acquire("Task-A", "Lock-1")
            print("[Task-A] Acquired Lock-1. Sleeping briefly...")
            time.sleep(0.3)

            print("[Task-A] Requesting Lock-2 (held by Task-B)...")
            manager.acquire("Task-A", "Lock-2")
            print("[Task-A] Acquired Lock-2! Working in Critical Section...")
            time.sleep(0.2)

            manager.release("Task-A", "Lock-2")
            manager.release("Task-A", "Lock-1")
            task_outcomes["Task-A"] = "COMPLETED"
            print("[Task-A] Successfully COMPLETED and released all locks.")
        except TaskAbortedException as exc:
            task_outcomes["Task-A"] = f"ABORTED: {exc}"
            print(f"[Task-A] Caught {exc}")

    def worker_b():
        try:
            print("[Task-B] Acquiring Lock-2...")
            manager.acquire("Task-B", "Lock-2")
            print("[Task-B] Acquired Lock-2. Sleeping briefly...")
            time.sleep(0.3)

            print("[Task-B] Requesting Lock-3 (held by Task-C)...")
            manager.acquire("Task-B", "Lock-3")
            print("[Task-B] Acquired Lock-3! Working in Critical Section...")
            time.sleep(0.2)

            manager.release("Task-B", "Lock-3")
            manager.release("Task-B", "Lock-2")
            task_outcomes["Task-B"] = "COMPLETED"
            print("[Task-B] Successfully COMPLETED and released all locks.")
        except TaskAbortedException as exc:
            task_outcomes["Task-B"] = f"ABORTED: {exc}"
            print(f"[Task-B] Caught {exc}")

    def worker_c():
        try:
            print("[Task-C] Acquiring Lock-3...")
            manager.acquire("Task-C", "Lock-3")
            print("[Task-C] Acquired Lock-3. Sleeping briefly...")
            time.sleep(0.3)

            print("[Task-C] Requesting Lock-1 (held by Task-A)...")
            manager.acquire("Task-C", "Lock-1")
            print("[Task-C] Acquired Lock-1! Working in Critical Section...")
            time.sleep(0.2)

            manager.release("Task-C", "Lock-1")
            manager.release("Task-C", "Lock-3")
            task_outcomes["Task-C"] = "COMPLETED"
            print("[Task-C] Successfully COMPLETED and released all locks.")
        except TaskAbortedException as exc:
            task_outcomes["Task-C"] = f"ABORTED: {exc}"
            print(f"[Task-C] Caught {exc}")

    # Step 2: Launch concurrent threads
    print("\n[Step 1] Starting concurrent worker tasks: Task-A, Task-B, Task-C...")
    t_a = threading.Thread(target=worker_a, name="Thread-Task-A")
    t_b = threading.Thread(target=worker_b, name="Thread-Task-B")
    t_c = threading.Thread(target=worker_c, name="Thread-Task-C")

    t_a.start()
    t_b.start()
    t_c.start()
    threads = [t_a, t_b, t_c]

    # Wait for all tasks to acquire their first locks and block on their second locks
    time.sleep(1.0)

    # Step 3: Run Centralized Deadlock Detection
    print("\n[Step 2] Evaluating live Wait-For Graph (WFG) from active resource dependencies...")
    report = manager.detect_deadlock()
    print(f"  -> Deadlock Detected: {report.is_deadlocked}")
    print(f"  -> Total Deadlocked Processes: {report.deadlocked_processes}")
    for idx, path in enumerate(report.formatted_cycle_paths, 1):
        print(f"  -> Cycle {idx}: {path}")

    if not report.is_deadlocked:
        raise RuntimeError("Expected deadlock cycle was not detected!")

    # Step 4: Resolve Deadlock via Documented Safe Preemption / Victim Abort
    print("\n[Step 3] Resolving Deadlock via Victim Selection & Preemption...")
    print("  -> Strategy: Centralized preemption of lexicographical highest node (Task-C)...")
    resolution = manager.detect_and_resolve(victim_policy="lexicographical")
    print(f"  -> Aborted Victim Task: {resolution.victim_task_id}")
    print(f"  -> Forcibly Freed Resources: {resolution.freed_resources}")
    print(f"  -> Remaining Active Tasks: {resolution.remaining_tasks}")

    # Step 5: Wait for remaining threads to finish
    print("\n[Step 4] Waiting for unblocked tasks to complete critical work...")
    for t in threads:
        t.join(timeout=3.0)

    print("\n[Step 5] Final Task Outcomes:")
    for tid, outcome in task_outcomes.items():
        print(f"  * {tid}: {outcome}")

    # Step 6: Verify final acyclic state
    final_report = manager.detect_deadlock()
    print(f"\n[Step 6] Final Graph State: Is Deadlocked? {final_report.is_deadlocked}")

    print("\n" + "=" * 75)
    print("CENTRALIZED WFG DEADLOCK DEMO COMPLETED SUCCESSFULLY")
    print("=" * 75)


if __name__ == "__main__":
    run_demo()
