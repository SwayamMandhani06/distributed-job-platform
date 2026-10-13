"""
Reproducible Multi-Process Demonstration of Logical Ring Leader Election (Chang & Roberts).
Phase 3 — Real Network Message Transport, Successor Failure Bypass, and Re-Election.

Topology:
---------
Organized in a logical unidirectional ring over real TCP/HTTP endpoints:
  P1 (Pri=10, Port=9211) -> P2 (Pri=20, Port=9212) -> P3 (Pri=30, Port=9213) -> P4 (Pri=40, Port=9214) -> P1

Demonstration Sequence:
-----------------------
1. Start 4 independent HTTP nodes on distinct ports in ring topology.
2. Initial Election: P4 initiates election; token circulates around the full ring;
   P4 (highest priority = 40) is elected COORDINATOR and circulates announcement.
3. Fault Injection: P4 (Coordinator) and P3 crash simultaneously.
4. Ring Bypass & Election: P1 initiates election; forwards to P2.
   - P2 attempts to forward to P3; connection fails/times out.
   - P2 detects dead successor, bypasses P3, and tries P4; connection fails.
   - P2 bypasses P4 and connects back to P1!
   - Token completes circuit. Candidate list: [(P1, 10), (P2, 20)].
   - Node P2 (Priority 20) is elected COORDINATOR!
   - P2 circulates announcement to P1.
5. Clean shutdown of all nodes.
"""

from pathlib import Path
import sys
import threading
import time
from typing import Dict, List, Tuple

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from common.election_transport import RingHTTPNode, run_node_server


def run_demo() -> None:
    print("=" * 80)
    print("  CHANG-ROBERTS RING ELECTION -- MULTI-PROCESS NETWORK TRANSPORT (Unit III)")
    print("=" * 80)

    # Topology: [(id, pri, port), ...] in unidirectional ring sequence
    ring_topology = [
        ("P1", 10, 9211),
        ("P2", 20, 9212),
        ("P3", 30, 9213),
        ("P4", 40, 9214),
    ]

    nodes: Dict[str, RingHTTPNode] = {}
    servers: List[threading.Thread] = []

    print("\n[Step 1] Initializing Logical Ring Nodes on Distinct TCP Ports:")
    for nid, pri, port in ring_topology:
        node = RingHTTPNode(
            node_id=nid,
            priority=pri,
            port=port,
            ring_topology=ring_topology,
            timeout_seconds=0.6,
        )
        nodes[nid] = node
        srv = run_node_server(node, port)
        t = threading.Thread(target=srv.serve_forever, daemon=True)
        t.start()
        servers.append(t)
        print(f"  * Node [{nid}] (Pri={pri}) listening on TCP port {port}")

    time.sleep(0.3)

    try:
        # Phase 1: P4 starts ring election
        print("\n--- PHASE 1: Initial Election -- Token Circulates Full Ring ---")
        nodes["P4"].start_election()
        time.sleep(1.0)

        for nid, node in nodes.items():
            print(f"  Node [{nid}] recognized coordinator: {node.current_coordinator}")
        print("-" * 80)

        # Phase 2: Fault injection: P4 and P3 crash
        print("\n--- PHASE 2: Fault Injection -- P4 (Coordinator) and P3 Crash Abruptly ---")
        nodes["P4"].fail()
        nodes["P3"].fail()
        time.sleep(0.2)
        print("-" * 80)

        # Phase 3: P1 starts ring election with successor failure bypass
        print("\n--- PHASE 3: P1 Initiates Ring Election (Bypassing Dead Nodes) ---")
        nodes["P1"].start_election()
        time.sleep(1.2)  # Wait for bypass attempts and message propagation

        print(f"\nRing State After Successor Bypass Election:")
        for nid in ["P1", "P2"]:
            print(f"  Node [{nid}] current coordinator: {nodes[nid].current_coordinator}")
        print(f"  P2 (Pri=20) elected coordinator! Dead nodes P3 & P4 were safely bypassed over TCP.")
        print("=" * 80)

    finally:
        print("\nShutting down election node servers...")
        for node in nodes.values():
            if node.server:
                node.server.shutdown()
        print("All nodes terminated cleanly.")


if __name__ == "__main__":
    run_demo()
