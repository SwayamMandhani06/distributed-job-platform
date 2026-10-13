"""
Deterministic Demonstration of Ring Leader Election (Unit III — Synchronization).

Ring Structure:
  P1 (Pri=10) -> P2 (Pri=20) -> P3 (Pri=30) -> P4 (Pri=40) -> P1

Demonstration Flow:
  1. Initial state: P4 is Coordinator.
  2. P4 (Coordinator) and P3 crash.
  3. P1 detects coordinator failure and initiates Ring Election.
  4. P1 passes token to P2.
  5. P2 attempts to forward to P3, discovers P3 is offline, and bypasses P3 directly to P4.
  6. P4 is also offline, so P2 bypasses P4 directly back to P1!
  7. Token completes circuit. Candidate list: [(P1, 10), (P2, 20)].
  8. Highest priority among live candidates is P2 (Pri=20).
  9. P2 is elected Coordinator and announcement circulates the ring.
"""

from common.election_ring import RingCluster


def run_ring_demo() -> None:
    print("=" * 80)
    print("  RING LEADER ELECTION DEMONSTRATION (Unit III -- Synchronization)")
    print("=" * 80)

    ring = RingCluster()
    p1 = ring.add_process("P1", priority=10)
    p2 = ring.add_process("P2", priority=20)
    p3 = ring.add_process("P3", priority=30)
    p4 = ring.add_process("P4", priority=40)

    print("Ring Topology: P1 -> P2 -> P3 -> P4 -> P1")
    print("Initial Coordinator: P4 (Priority 40)")
    p4.start_election()
    print(f"Cluster Consensus Coordinator: {ring.get_active_coordinator()}")
    print("-" * 80)

    # Simulate failure of P4 (Coordinator) and P3
    print("\n--- FAULT INJECTION: P4 (Coordinator) and P3 Crash Abruptly ---")
    p4.fail()
    p3.fail()
    print("-" * 80)

    # P1 notices failure and starts Ring Election
    ring.clear_trace()
    print("\n--- RING ELECTION INITIATION by P1 ---")
    elected = p1.start_election()

    for line in ring.get_trace():
        print(f"  {line}")

    print(f"\nFinal Ring Coordinator: {ring.get_active_coordinator()} (Expected P2)")
    print("=" * 80)


if __name__ == "__main__":
    run_ring_demo()
