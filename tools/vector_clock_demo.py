"""
Vector Clock Demonstration (Unit III — Synchronization).

Demonstrates:
  1. Component-wise merge and local increment rules across 3 named processes (P1, P2, P3).
  2. The distinction between causal precedence (V_a < V_b) and concurrency (V_a || V_b).
  3. A step-by-step trace showing two concurrent events and a later causally dependent event.

Usage:
  python -m tools.vector_clock_demo
"""

from common.clocks import VectorClock


def run_demo() -> None:
    print("=" * 75)
    print("  Vector Clock Synchronization & Concurrency Demonstration (Unit III)")
    print("=" * 75)
    print("System Model: 3 Distributed Processes: P1 (Scheduler), P2 (Worker-A), P3 (Worker-B)")
    print("Vector Structure: [P1, P2, P3]\n")

    processes = ["P1", "P2", "P3"]
    p1 = VectorClock("P1", processes)
    p2 = VectorClock("P2", processes)
    p3 = VectorClock("P3", processes)

    print("Initial State:")
    print(f"  P1 Clock: {p1.clock}")
    print(f"  P2 Clock: {p2.clock}")
    print(f"  P3 Clock: {p3.clock}")
    print("-" * 75)

    # Step 1: P1 performs a local event (e.g. Ingest Job A)
    v_e1 = p1.local_event()
    print(f"Step 1: P1 executes local event e1 (e.g., Job Ingestion)")
    print(f"        P1 Vector: {v_e1}")

    # Step 2: P2 performs an independent local event (e.g. Cache Warmup on Worker-A)
    v_e2 = p2.local_event()
    print(f"\nStep 2: P2 executes independent local event e2 (e.g., Cache Warmup)")
    print(f"        P2 Vector: {v_e2}")

    # Analysis 1: Compare e1 and e2
    rel_1_2 = VectorClock.compare(v_e1, v_e2)
    print(f"\n>>> Causal Analysis of e1 and e2:")
    print(f"    v(e1) = {v_e1}  vs  v(e2) = {v_e2}")
    print(f"    Relationship: {rel_1_2} (e1 || e2)")
    print(f"    Explanation : Neither event observed the other. In a distributed system,")
    print(f"                  physical timestamps might place one before the other, but")
    print(f"                  causally they are CONCURRENT -- neither could influence the other.")
    print("-" * 75)

    # Step 3: P1 sends a message m1 (with Job A) to P3
    v_msg_p1 = p1.send_event()
    print(f"Step 3: P1 sends task message m1 to P3 (attaching vector {v_msg_p1})")
    print(f"        P1 Vector: {p1.clock}")

    # Step 4: P2 sends a message m2 (with telemetry) to P3
    v_msg_p2 = p2.send_event()
    print(f"\nStep 4: P2 sends telemetry message m2 to P3 (attaching vector {v_msg_p2})")
    print(f"        P2 Vector: {p2.clock}")

    # Step 5: P3 receives message m1 from P1
    v_e3 = p3.receive_event(v_msg_p1)
    print(f"\nStep 5: P3 receives message m1 from P1")
    print(f"        P3 merges max(P3, m1) and increments P3 entry.")
    print(f"        P3 Vector: {v_e3}")

    # Step 6: P3 receives message m2 from P2
    v_e4 = p3.receive_event(v_msg_p2)
    print(f"\nStep 6: P3 receives message m2 from P2 (event e4: Aggregated Report)")
    print(f"        P3 merges max(P3, m2) and increments P3 entry.")
    print(f"        P3 Vector: {v_e4}")

    # Analysis 2: Causal relationship of e1, e2 with e4
    rel_1_4 = VectorClock.compare(v_e1, v_e4)
    rel_2_4 = VectorClock.compare(v_e2, v_e4)
    print(f"\n>>> Causal Analysis of final event e4 at P3:")
    print(f"    v(e1) = {v_e1}  vs  v(e4) = {v_e4} -> Relationship: {rel_1_4} (e1 -> e4)")
    print(f"    v(e2) = {v_e2}  vs  v(e4) = {v_e4} -> Relationship: {rel_2_4} (e2 -> e4)")
    print(f"    Conclusion  : Event e4 causally succeeds BOTH e1 and e2.")
    print(f"                  Information from both independent branches has converged at P3!")
    print("=" * 75)


if __name__ == "__main__":
    run_demo()
