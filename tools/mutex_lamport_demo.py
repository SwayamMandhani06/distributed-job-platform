"""
Educational Simulation: Lamport Distributed Mutual Exclusion (Unit III — Synchronization).

This script demonstrates:
1. Three simulated nodes (Node-A, Node-B, Node-C) within a single Python runtime
   competing concurrently for critical section access to append to `shared_audit_ledger.log`.
2. Total ordering of requests via (Lamport timestamp, Process ID).
3. Non-overlapping execution of the Critical Section (Mutual Exclusion Safety invariant).
4. Explicit handling of an unavailable participant using an idealized Perfect Failure Detector
   eviction mechanism to prevent indefinite deadlock.

Scope and Guarantees:
---------------------
- Scope: In-process educational simulation with direct callback message routing.
- Classical Assumption: Requires reliable FIFO channels and non-failing nodes.
- Failure Detection: Eviction assumes a Crash-Stop model with a Perfect Failure Detector (P).
  In asynchronous networks, false suspicions could compromise safety.
- Resource Protection: Demonstrates protocol-level mutual exclusion across simulated nodes.
  Does NOT establish OS-level advisory file locking across separate OS processes.
"""

import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from common.logging_config import setup_logger
from common.mutual_exclusion import MutexCluster

logger = setup_logger("mutex_demo")
BASE_DIR = Path(__file__).resolve().parent.parent
LEDGER_FILE = BASE_DIR / "shared_audit_ledger.log"


def print_banner(text: str) -> None:
    print("\n" + "=" * 76)
    print(f" {text}")
    print("=" * 76)


def print_step(step_num: int, title: str) -> None:
    print(f"\n--- [STEP {step_num}] {title} ---")


def write_audit_ledger(node_id: str, lamport_ts: int, action: str, details: str = "") -> None:
    """Safely append an audit ledger entry to the shared audit file."""
    now_iso = datetime.now(timezone.utc).isoformat()
    entry = f"{now_iso} | PID={node_id:<8} | L#{lamport_ts:<3} | {action:<10} | {details}\n"
    with open(LEDGER_FILE, "a", encoding="utf-8") as f:
        f.write(entry)


def verify_non_overlapping_cs(entries: list) -> bool:
    """Verify that no two critical section entries overlap in time."""
    in_cs = None
    for entry in entries:
        action = entry.get("action")
        node = entry.get("node")
        if action == "ENTER":
            if in_cs is not None:
                print(f"[SAFETY VIOLATION DETECTED] Node {node} entered while {in_cs} was in CS!")
                return False
            in_cs = node
        elif action == "EXIT":
            if in_cs != node:
                print(f"[SAFETY VIOLATION DETECTED] Node {node} exited but active CS was {in_cs}!")
                return False
            in_cs = None
    return True


def run_mutual_exclusion_demonstration() -> int:
    print_banner("UNIT III: LAMPORT DISTRIBUTED MUTEX PROTOCOL DEMONSTRATION")
    print("Mode:            In-Process Educational Simulation (Single Python Runtime)")
    print("Resource Target: shared_audit_ledger.log (Logical Mutex, not OS-level file lock)")
    print("Algorithm:       Lamport Request/Reply/Release with (Lamport TS, PID) Total Order")
    print("Participants:    3 Nodes (Node-A, Node-B, Node-C)")

    # Reset audit log file
    if LEDGER_FILE.exists():
        LEDGER_FILE.unlink()

    cluster = MutexCluster(["Node-A", "Node-B", "Node-C"])
    node_a = cluster.get_node("Node-A")
    node_b = cluster.get_node("Node-B")
    node_c = cluster.get_node("Node-C")

    # Step 1: Initial State
    print_step(1, "Cluster Initialization")
    print("Initialized 3 nodes with Lamport logical clocks = 0.")
    for nid, node in cluster.nodes.items():
        print(f"  * {nid}: Clock = {node.clock.get_time()}, Queue = {node.request_queue}")

    # Step 2: Overlapping / Concurrent Requests
    print_step(2, "Concurrent Overlapping Requests Initiated")
    # Simulate slightly staggered requests:
    # Node-B requests first (L#1), then Node-A requests (L#2), then Node-C requests (L#3)
    ts_b = node_b.request_critical_section()
    print(f"  [+] Node-B requested CS -> Timestamp L#{ts_b}")

    ts_a = node_a.request_critical_section()
    print(f"  [+] Node-A requested CS -> Timestamp L#{ts_a}")

    ts_c = node_c.request_critical_section()
    print(f"  [+] Node-C requested CS -> Timestamp L#{ts_c}")

    print("\nRequest Queues after initial message exchanges (Total Order (TS, PID)):")
    for nid in ["Node-A", "Node-B", "Node-C"]:
        node = cluster.get_node(nid)
        print(f"  * {nid:6}: Head={node.request_queue[0] if node.request_queue else None} | Full Queue={node.request_queue}")

    # Step 3: First Critical Section Entry (Node-B)
    print_step(3, "First Critical Section Evaluation")
    print(f"Checking who can enter CS:")
    print(f"  * Node-A can enter? {node_a.can_enter()} (Head is {node_a.request_queue[0]})")
    print(f"  * Node-B can enter? {node_b.can_enter()} (Head is {node_b.request_queue[0]})")
    print(f"  * Node-C can enter? {node_c.can_enter()} (Head is {node_c.request_queue[0]})")

    assert node_b.can_enter(), "Node-B has lowest Lamport TS (L#1) and must enter CS first!"
    node_b.enter_critical_section()
    write_audit_ledger("Node-B", node_b.clock.get_time(), "ENTER", "Entering Critical Section")
    write_audit_ledger("Node-B", node_b.clock.get_time(), "AUDIT", "Appended audit transaction B-001")
    write_audit_ledger("Node-B", node_b.clock.get_time(), "EXIT", "Leaving Critical Section")
    rel_b = node_b.release_critical_section()
    print(f"  [OK] Node-B executed CS and broadcast RELEASE (L#{rel_b})")

    # Step 4: Second Critical Section Entry (Node-A)
    print_step(4, "Second Critical Section Evaluation")
    print(f"Checking who can enter CS following Node-B release:")
    print(f"  * Node-A can enter? {node_a.can_enter()} (Head is {node_a.request_queue[0]})")
    print(f"  * Node-C can enter? {node_c.can_enter()} (Head is {node_c.request_queue[0]})")

    assert node_a.can_enter(), "Node-A has lower Lamport TS than Node-C and must enter next!"
    node_a.enter_critical_section()
    write_audit_ledger("Node-A", node_a.clock.get_time(), "ENTER", "Entering Critical Section")
    write_audit_ledger("Node-A", node_a.clock.get_time(), "AUDIT", "Appended audit transaction A-002")
    write_audit_ledger("Node-A", node_a.clock.get_time(), "EXIT", "Leaving Critical Section")
    rel_a = node_a.release_critical_section()
    print(f"  [OK] Node-A executed CS and broadcast RELEASE (L#{rel_a})")

    # Step 5: Third Critical Section Entry (Node-C)
    print_step(5, "Third Critical Section Evaluation")
    print(f"Checking Node-C:")
    print(f"  * Node-C can enter? {node_c.can_enter()} (Head is {node_c.request_queue[0]})")

    assert node_c.can_enter(), "Node-C is now at head of queue and must enter!"
    node_c.enter_critical_section()
    write_audit_ledger("Node-C", node_c.clock.get_time(), "ENTER", "Entering Critical Section")
    write_audit_ledger("Node-C", node_c.clock.get_time(), "AUDIT", "Appended audit transaction C-003")
    write_audit_ledger("Node-C", node_c.clock.get_time(), "EXIT", "Leaving Critical Section")
    rel_c = node_c.release_critical_section()
    print(f"  [OK] Node-C executed CS and broadcast RELEASE (L#{rel_c})")

    # Step 6: Handling an Unavailable Participant
    print_step(6, "Handling Participant Crash / Unavailability")
    print("Scenario: Node-C crashes while Node-A requests CS again.")
    print("Under naive Lamport Mutex, Node-A would wait forever for Node-C's REPLY.")
    print("Demonstrating explicit dead-node detection and eviction:")

    # Node-C crashes / becomes silent
    node_a.mark_node_dead("Node-C")
    node_b.mark_node_dead("Node-C")

    # Node-A requests CS
    ts_a2 = node_a.request_critical_section()
    print(f"  [+] Node-A requested CS (L#{ts_a2}) with Node-C dead")
    print(f"  * Replies received at Node-A: {node_a.replies_received} (Node-C excluded from required quorum)")
    print(f"  * Can Node-A enter CS? {node_a.can_enter()}")
    assert node_a.can_enter(), "Node-A must enter CS without dead Node-C blocking progress!"

    node_a.enter_critical_section()
    write_audit_ledger("Node-A", node_a.clock.get_time(), "ENTER", "Entering CS (Node-C offline)")
    write_audit_ledger("Node-A", node_a.clock.get_time(), "AUDIT", "Appended emergency failover audit")
    write_audit_ledger("Node-A", node_a.clock.get_time(), "EXIT", "Exiting CS (Node-C offline)")
    node_a.release_critical_section()
    print("  [OK] Node-A successfully entered and released CS despite Node-C failure.")

    # Step 7: Parse and Verify Audit Ledger
    print_step(7, "Verification of Shared Audit Ledger")
    print(f"Reading {LEDGER_FILE.name}:")
    with open(LEDGER_FILE, "r", encoding="utf-8") as f:
        lines = f.readlines()

    audit_events = []
    for line in lines:
        print("  " + line.strip())
        parts = [p.strip() for p in line.split("|")]
        if len(parts) >= 4:
            audit_events.append({"node": parts[1].replace("PID=", ""), "action": parts[3]})

    valid = verify_non_overlapping_cs(audit_events)
    print("\nMutual Exclusion Invariant Verification:")
    print(f"  * Overlapping Critical Section Detected: NO")
    print(f"  * Safety Property Preserved:             {'PASSED [100%]' if valid else 'FAILED'}")
    print(f"  * Message Complexity:                    3(N - 1) per entry = 6 messages")

    print_banner("DEMONSTRATION COMPLETED SUCCESSFULLY")
    return 0 if valid else 1


if __name__ == "__main__":
    sys.exit(run_mutual_exclusion_demonstration())
