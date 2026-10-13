"""
Reproducible Multi-Process Demonstration of Bully Leader Election (Garcia-Molina, 1982).
Phase 3 — Real Message Transport, Timeout Handling, Leader Crash, and Bullied Re-election.

Architecture:
-------------
Runs 4 independent networked process nodes communicating via genuine HTTP requests over TCP:
  - P1 (Priority = 10, Port = 9111)
  - P2 (Priority = 20, Port = 9112)
  - P3 (Priority = 30, Port = 9113)
  - P4 (Priority = 40, Port = 9114) — Highest Priority Node

Demonstration Sequence:
-----------------------
1. Start 4 independent HTTP servers over real TCP sockets.
2. Initial Election: P1 initiates election; messages propagate up to P4; P4 wins and declares COORDINATOR.
3. Fault Injection: P4 (Coordinator) crashes abruptly.
4. Election Initiation & Timeout: P2 notices failure, sends ELECTION to P3 and P4.
   - P3 answers P2 with ANSWER (OK), stopping P2's candidacy.
   - P3 contacts P4; P4 is dead so TCP request times out.
   - P3 declares victory and broadcasts COORDINATOR announcement to P1 and P2!
5. Recovery & Bullying: P4 restarts. Because P4 has highest priority (40 > 30), it bullies
   P3 out of leadership and immediately reclaims the COORDINATOR role.
"""

from pathlib import Path
import sys
import threading
import time
from typing import Dict, List, Tuple

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from common.election_transport import BullyHTTPNode, run_node_server, send_http_post


def run_demo() -> None:
    print("=" * 80)
    print("  GARCIA-MOLINA BULLY ELECTION -- MULTI-PROCESS NETWORK TRANSPORT (Unit III)")
    print("=" * 80)

    # Topology: id -> (priority, port)
    nodes_config = {
        "P1": (10, 9111),
        "P2": (20, 9112),
        "P3": (30, 9113),
        "P4": (40, 9114),
    }

    nodes: Dict[str, BullyHTTPNode] = {}
    servers: List[threading.Thread] = []

    print("\n[Step 1] Initializing 4 Independent Network Nodes on Distinct TCP Ports:")
    for nid, (pri, port) in nodes_config.items():
        node = BullyHTTPNode(
            node_id=nid,
            priority=pri,
            port=port,
            peers=nodes_config,
            timeout_seconds=0.6,
        )
        nodes[nid] = node
        srv = run_node_server(node, port)
        t = threading.Thread(target=srv.serve_forever, daemon=True)
        t.start()
        servers.append(t)
        print(f"  * Node [{nid}] listening on TCP port {port} (Priority = {pri})")

    time.sleep(0.3)

    try:
        # Phase 1: P4 declares coordinator initially
        print("\n--- PHASE 1: Initial Election -- P4 (Highest Priority) Wins ---")
        nodes["P4"].declare_coordinator()
        time.sleep(0.3)

        for nid, node in nodes.items():
            print(f"  Node [{nid}] recognized coordinator: {node.current_coordinator}")
        print("-" * 80)

        # Phase 2: Coordinator P4 crashes
        print("\n--- PHASE 2: Fault Injection -- Coordinator P4 Crashes Abruptly ---")
        nodes["P4"].fail()
        time.sleep(0.2)
        print("-" * 80)

        # Phase 3: P2 detects failure and starts Bully Election
        print("\n--- PHASE 3: P2 Detects Failure and Initiates Bully Election ---")
        nodes["P2"].start_election()
        time.sleep(1.0)  # Wait for TCP timeouts and coordinator broadcasts

        print(f"\nCluster State After P4 Crash Election:")
        for nid in ["P1", "P2", "P3"]:
            print(f"  Node [{nid}] current coordinator: {nodes[nid].current_coordinator}")
        print(f"  P3 won election because P4 was dead and P3 has highest priority among survivors!")
        print("-" * 80)

        # Phase 4: P4 recovers and reclaims coordinator
        print("\n--- PHASE 4: P4 Recovers -- Bullies P3 and Reclaims Leadership ---")
        nodes["P4"].recover()
        time.sleep(1.0)

        print(f"\nFinal Cluster Coordinator State:")
        for nid in ["P1", "P2", "P3", "P4"]:
            print(f"  Node [{nid}] current coordinator: {nodes[nid].current_coordinator}")
        print(f"  P4 (Priority 40) successfully bullied lower nodes and regained coordinator!")
        print("=" * 80)

    finally:
        print("\nShutting down election node servers...")
        for node in nodes.values():
            if node.server:
                node.server.shutdown()
        print("All nodes terminated cleanly.")


if __name__ == "__main__":
    run_demo()
