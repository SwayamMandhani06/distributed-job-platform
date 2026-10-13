"""
Deterministic Demonstration of Bully Leader Election (Unit III — Synchronization).

Scenario:
  4 Distributed Processes:
    P1 (Priority = 10)
    P2 (Priority = 20)
    P3 (Priority = 30)
    P4 (Priority = 40) — Initial Coordinator

Demonstration Flow:
  1. P4 (Current Coordinator) crashes.
  2. P2 detects failure and initiates Bully Election.
  3. P3 responds to P2, halting P2's candidacy.
  4. P3 sends election message to P4; P4 is dead so no answer arrives.
  5. P3 wins election and broadcasts COORDINATOR announcement to all processes.
  6. Later, P4 recovers, immediately initiates election, bullies P3, and reclaims coordinator!
"""

from common.election_bully import BullyCluster


def run_bully_demo() -> None:
    print("=" * 80)
    print("  BULLY LEADER ELECTION DEMONSTRATION (Unit III -- Synchronization)")
    print("=" * 80)

    cluster = BullyCluster()
    p1 = cluster.add_process("P1", priority=10)
    p2 = cluster.add_process("P2", priority=20)
    p3 = cluster.add_process("P3", priority=30)
    p4 = cluster.add_process("P4", priority=40)

    # Initial stable state: P4 is coordinator
    p4.declare_coordinator()
    print("\n--- PHASE 1: Initial Stable State ---")
    print(f"Active Coordinator across cluster: {cluster.get_active_coordinator()}")
    print("-" * 80)

    # Crash the coordinator
    print("\n--- PHASE 2: Coordinator P4 Crashes ---")
    p4.fail()
    print("-" * 80)

    # Lower-priority process P2 notices failure and initiates election
    cluster.clear_trace()
    print("\n--- PHASE 3: P2 Detects Failure and Initiates Election ---")
    p2.start_election()

    for line in cluster.get_trace():
        print(f"  {line}")

    print(f"\nResulting Coordinator: {cluster.get_active_coordinator()} (Expected P3, since P4 is dead)")
    print("-" * 80)

    # P4 recovers and reclaims leadership
    cluster.clear_trace()
    print("\n--- PHASE 4: P4 (Highest Priority) Recovers and Bullies P3 ---")
    p4.recover()
    p4.start_election()

    for line in cluster.get_trace():
        print(f"  {line}")

    print(f"\nFinal Cluster Coordinator: {cluster.get_active_coordinator()} (P4 successfully bullied back to office)")
    print("=" * 80)


if __name__ == "__main__":
    run_bully_demo()
