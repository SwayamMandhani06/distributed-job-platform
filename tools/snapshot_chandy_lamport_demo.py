"""
Deterministic Demonstration: Chandy-Lamport Distributed Snapshot Algorithm (Unit III — Synchronization).

This script demonstrates:
1. A 3-process distributed system (P1, P2, P3) communicating over unidirectional FIFO channels.
2. In-flight messages in transit along channels while processes transition states.
3. Marker propagation initiating and capturing local process states and in-transit channel messages.
4. Mathematical verification of a Consistent Global Cut:
   Total System Value (Sum of Process States + Channel Messages) is exactly conserved.
"""

from pathlib import Path
import sys
from datetime import datetime, timezone

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from common.snapshot_chandy_lamport import ChandyLamportCluster, FIFOChannel


def print_banner(text: str) -> None:
    print("\n" + "=" * 78)
    print(f" {text}")
    print("=" * 78)


def print_step(step_num: int, title: str) -> None:
    print(f"\n--- [STEP {step_num}] {title} ---")


def run_snapshot_demonstration() -> int:
    print_banner("UNIT III: CHANDY-LAMPORT DISTRIBUTED SNAPSHOT DEMONSTRATION")
    print("Model:       Unidirectional FIFO Channels, Marker Propagation Protocol")
    print("Invariants:  Consistent Global Cut, Conservation of Total Token Balance")
    print("Nodes:       3 Distributed Processes (P1, P2, P3)")

    # Step 1: Initial Setup
    print_step(1, "Cluster Initialization")
    initial_balances = {"P1": 100, "P2": 100, "P3": 100}
    cluster = ChandyLamportCluster(initial_balances)
    total_initial = sum(initial_balances.values())

    print(f"Initial Process States (Total Invariant = {total_initial} tokens):")
    for pid, bal in initial_balances.items():
        print(f"  * [{pid}] Balance = {bal}")

    # Step 2: Application Traffic with in-flight message
    print_step(2, "Application Traffic Initiated (Messages In-Flight)")
    p1 = cluster.get_process("P1")
    p2 = cluster.get_process("P2")
    p3 = cluster.get_process("P3")

    # P1 transfers 30 to P2
    p1.send_transfer("P2", 30)
    print(f"  [+] P1 dispatched 30 tokens to P2. P1 local balance is now {p1.balance}.")
    print("      Note: Message APP(30) is now in-flight on Channel(P1->P2).")

    # Step 3: P1 Initiates Chandy-Lamport Snapshot
    print_step(3, "Snapshot Initiation by P1 (Marker Emission)")
    print("  [!] P1 initiates snapshot 'SNAP-ALPHA':")
    p1.initiate_snapshot("SNAP-ALPHA")
    print(f"      - P1 recorded local state = {p1.recorded_state}")
    print("      - P1 broadcast MARKER(SNAP-ALPHA) on Channel(P1->P2) and Channel(P1->P3)")
    print("      - Channel(P1->P2) queue contents: [APP(30), MARKER]")

    # Meanwhile, P2 transfers 20 to P3 before seeing P1's marker
    p2.send_transfer("P3", 20)
    print(f"  [+] P2 independently dispatched 20 tokens to P3. P2 local balance is now {p2.balance}.")
    print("      Note: Message APP(20) is in-flight on Channel(P2->P3).")

    # Step 4: Step-by-Step Marker and Message Propagation
    print_step(4, "FIFO Message and Marker Propagation Across Channels")

    # P1's marker arrives at P3 first (via direct channel P1->P3)
    # Process P3's incoming channel from P1
    print("  [>] Delivering messages to P3:")
    while cluster.channels[("P1", "P3")].has_messages():
        p3.step()
    print(f"      - P3 received first MARKER from P1.")
    print(f"      - P3 recorded local state = {p3.recorded_state}")
    print(f"      - P3 recorded Channel(P1->P3) as EMPTY ([])")
    print(f"      - P3 starts recording incoming channel Channel(P2->P3)!")

    # Now deliver messages to P2: P2 receives APP(30) then MARKER
    print("\n  [>] Delivering messages to P2:")
    while cluster.channels[("P1", "P2")].has_messages():
        p2.step()
    print(f"      - P2 received APP(30), updated balance to {p2.balance}.")
    print(f"      - P2 received MARKER from P1.")
    print(f"      - P2 recorded local state = {p2.recorded_state}")
    print(f"      - P2 forwarded MARKER on Channel(P2->P1) and Channel(P2->P3)")

    # Now deliver messages to P3: P3 receives APP(20) from P2, then P2's marker!
    print("\n  [>] Delivering messages to P3 from P2:")
    while cluster.channels[("P2", "P3")].has_messages():
        p3.step()
    print(f"      - P3 received APP(20) while recording Channel(P2->P3)!")
    print(f"      - Channel(P2->P3) recorded in-transit message: [20]")
    print(f"      - P3 received MARKER from P2 -> Stopped recording Channel(P2->P3).")

    # Finish remaining marker delivery across remaining channels
    cluster.step_all()

    # Step 5: Global Snapshot Verification
    print_step(5, "Evaluation of Consistent Global Cut")
    snapshot = cluster.get_global_snapshot("SNAP-ALPHA")

    print(f"Snapshot Complete: {snapshot.is_complete}")
    print("\nRecorded Process States:")
    for pid, val in snapshot.process_states.items():
        print(f"  * Process [{pid}]: {val} tokens")

    print("\nRecorded In-Flight Channel States:")
    non_empty_channels = {k: v for k, v in snapshot.channel_states.items() if v}
    for chan, msgs in snapshot.channel_states.items():
        status_str = f"{msgs} (IN-FLIGHT)" if msgs else "[] (EMPTY)"
        print(f"  * Channel {chan:8}: {status_str}")

    print("\nMathematical Invariant Verification:")
    sum_procs = sum(snapshot.process_states.values())
    sum_channels = sum(sum(msgs) for msgs in snapshot.channel_states.values())
    total_snapshot = sum_procs + sum_channels

    print(f"  * Sum of Process Local States : {sum_procs} tokens")
    print(f"  * Sum of In-Transit Messages  : {sum_channels} tokens")
    print(f"  * Total Captured Global State : {total_snapshot} tokens")
    print(f"  * Expected Invariant          : {total_initial} tokens")

    is_consistent = (total_snapshot == total_initial) and snapshot.is_complete
    print(f"  * Consistent Cut Invariant    : {'PASSED [100%]' if is_consistent else 'FAILED'}")

    print_banner("CHANDY-LAMPORT DEMONSTRATION COMPLETE")
    return 0 if is_consistent else 1


if __name__ == "__main__":
    sys.exit(run_snapshot_demonstration())
