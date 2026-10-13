"""
Deterministic Demonstration: Wait-For Graph (WFG) Deadlock Detection (Unit III — Synchronization).

This script demonstrates:
1. Scenario A (Acyclic Dependency): Safe execution where processes form a Directed Acyclic Graph (DAG).
2. Scenario B (Circular Wait / Deadlock): Classical circular dependency where P1 -> P2 -> P3 -> P1.
3. Cycle Detection Engine: Depth-First Search with 3-Color cycle extraction.
4. Deadlock Recovery: Resolving deadlock by aborting a victim process and re-evaluating the WFG.
5. Educational Analysis: Centralized WFG vs. Distributed Chandy-Misra-Haas edge chasing.
"""

import sys
from common.deadlock_wfg import WaitForGraph


def print_banner(text: str) -> None:
    print("\n" + "=" * 78)
    print(f" {text}")
    print("=" * 78)


def print_step(step_num: int, title: str) -> None:
    print(f"\n--- [STEP {step_num}] {title} ---")


def run_deadlock_demonstration() -> int:
    print_banner("UNIT III: WAIT-FOR GRAPH (WFG) DEADLOCK DETECTION DEMONSTRATION")
    print("Model:       Centralized Wait-For Graph (WFG) Coordinator")
    print("Algorithm:   DFS 3-Color Directed Cycle Detection")
    print("Syllabus:    Unit III -- Synchronization (Distributed Deadlock Detection)")

    # -------------------------------------------------------------------------
    # PART 1: Acyclic Execution (Safe State)
    # -------------------------------------------------------------------------
    print_step(1, "Scenario A: Acyclic Resource Allocation (Safe State)")
    wfg_safe = WaitForGraph()

    # P1 acquires ResA; P2 acquires ResB
    wfg_safe.request_resource("Process-1", "Resource-A")
    wfg_safe.request_resource("Process-2", "Resource-B")

    # P2 needs Resource-A (held by P1) -> P2 waits for P1
    wfg_safe.request_resource("Process-2", "Resource-A")

    # P3 needs Resource-B (held by P2) -> P3 waits for P2
    wfg_safe.request_resource("Process-3", "Resource-B")

    print("Current Wait-For Graph Dependencies:")
    print("  * Process-2 -> Process-1 (waiting on Resource-A)")
    print("  * Process-3 -> Process-2 (waiting on Resource-B)")

    report_safe = wfg_safe.detect_deadlock()
    print(f"\nDeadlock Analysis:")
    print(f"  * Deadlock Detected?    {report_safe.is_deadlocked}")
    print(f"  * Detected Cycles:      {report_safe.cycles}")
    print(f"  * Deadlocked Processes: {report_safe.deadlocked_processes}")
    print("  [OK] Graph is a Directed Acyclic Graph (DAG). System is progressing safely.")

    assert not report_safe.is_deadlocked, "Scenario A must be acyclic!"

    # -------------------------------------------------------------------------
    # PART 2: Circular Wait (Deadlock State)
    # -------------------------------------------------------------------------
    print_step(2, "Scenario B: Circular Wait Creation (Deadlock State)")
    wfg_dead = WaitForGraph()

    # P1 holds R1, P2 holds R2, P3 holds R3
    wfg_dead.request_resource("Process-1", "Resource-1")
    wfg_dead.request_resource("Process-2", "Resource-2")
    wfg_dead.request_resource("Process-3", "Resource-3")
    print("Initial Exclusive Allocations:")
    print("  * Process-1 holds Resource-1")
    print("  * Process-2 holds Resource-2")
    print("  * Process-3 holds Resource-3")

    # Now create the circular dependency:
    # P1 requests Resource-2 (held by P2) -> P1 waits for P2
    wfg_dead.request_resource("Process-1", "Resource-2")
    # P2 requests Resource-3 (held by P3) -> P2 waits for P3
    wfg_dead.request_resource("Process-2", "Resource-3")
    # P3 requests Resource-1 (held by P1) -> P3 waits for P1
    wfg_dead.request_resource("Process-3", "Resource-1")

    print("\nAttempted Competing Allocations:")
    print("  * Process-1 requests Resource-2 -> BLOCKED (held by Process-2)")
    print("  * Process-2 requests Resource-3 -> BLOCKED (held by Process-3)")
    print("  * Process-3 requests Resource-1 -> BLOCKED (held by Process-1)")

    report_dead = wfg_dead.detect_deadlock()
    print("\nDeadlock Detection Engine Output:")
    print(f"  * Deadlock Detected?    {report_dead.is_deadlocked}")
    print(f"  * Deadlocked Processes: {report_dead.deadlocked_processes}")
    print("  * Cycle Paths Found:")
    for path in report_dead.formatted_cycle_paths:
        print(f"    -> {path}")

    assert report_dead.is_deadlocked, "Scenario B must detect the circular deadlock!"

    # -------------------------------------------------------------------------
    # PART 3: Deadlock Recovery via Victim Selection
    # -------------------------------------------------------------------------
    print_step(3, "Scenario C: Deadlock Resolution via Victim Preemption")
    victim = "Process-3"
    print(f"Selecting victim process '{victim}' for preemption / abort...")
    wfg_dead.resolve_deadlock_by_aborting(victim)

    report_resolved = wfg_dead.detect_deadlock()
    print("\nPost-Preemption Graph Evaluation:")
    print(f"  * Deadlock Detected?    {report_resolved.is_deadlocked}")
    print(f"  * Remaining Cycles:     {report_resolved.formatted_cycle_paths}")
    print("  [OK] Circular wait successfully broken! Remaining processes can make progress.")

    assert not report_resolved.is_deadlocked, "Deadlock must be resolved after victim abort!"

    # -------------------------------------------------------------------------
    # PART 4: Architectural Discussion (Syllabus Context)
    # -------------------------------------------------------------------------
    print_step(4, "Theoretical Distributed Systems Context")
    print("1. Centralized WFG (Demonstrated):")
    print("   - Simplicity: O(V + E) cycle check using DFS 3-coloring.")
    print("   - Vulnerability: Single Point of Failure and potential 'Phantom Deadlocks'")
    print("     if network message latency delays dependency release notifications.")
    print("2. Distributed Detection (Chandy-Misra-Haas Edge-Chasing):")
    print("   - Distributed processes pass Probe messages (initiator, sender, receiver)")
    print("     along wait-for edges without maintaining a global graph.")
    print("   - If an initiator receives its own probe, deadlock is confirmed.")

    print_banner("DEADLOCK DETECTION DEMONSTRATION COMPLETE")
    return 0


if __name__ == "__main__":
    sys.exit(run_deadlock_demonstration())
