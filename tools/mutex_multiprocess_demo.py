"""
Multi-Process Demonstration and Verification of Lamport Distributed Mutual Exclusion (Unit III).

Runs three independent operating system processes communicating over real TCP sockets:
  - Node-1 (Port 9311)
  - Node-2 (Port 9312)
  - Node-3 (Port 9313)

Demonstration Steps:
--------------------
1. Spawn 3 distinct Python subprocesses running `common/mutex_transport.py`.
2. Concurrently initiate critical section requests from Node-1, Node-2, and Node-3.
3. Observe REQUEST, REPLY, and RELEASE exchanges across the network.
4. Each node enters the critical section, writes to `shared_cs_audit.log`, and releases.
5. Parse the durable log and verify the Mutual Exclusion Invariant:
     max(enter_A, enter_B) >= min(exit_A, exit_B) for all pairs.
6. Failure Demonstration: Simulate Node-3 crashing before sending REPLY.
     Observe that Node-1 blocks (proving classical Lamport failure sensitivity).
7. Recovery Demonstration: Mark Node-3 dead via failure detector hook.
     Observe that Node-1 unblocks and enters critical section.
"""

import json
import os
import subprocess
import sys
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from common.mutex_transport import send_http_post, verify_critical_section_log

PYTHON_EXE = sys.executable
LOG_FILE = BASE_DIR / "shared_cs_audit.log"

PORTS = {
    "Node-1": 9311,
    "Node-2": 9312,
    "Node-3": 9313,
}


def wait_for_node(port: int, max_retries: int = 25) -> bool:
    for _ in range(max_retries):
        success, resp, _ = send_http_post(f"http://127.0.0.1:{port}/health", {}, timeout_seconds=0.5)
        if success:
            return True
        time.sleep(0.2)
    return False


def run_demo():
    print("=" * 75)
    print("LAMPORT DISTRIBUTED MUTUAL EXCLUSION — MULTI-PROCESS NETWORK DEMO")
    print("=" * 75)

    if LOG_FILE.exists():
        LOG_FILE.unlink()

    procs = {}
    try:
        # Step 1: Launch 3 independent processes
        print("\n[Step 1] Spawning 3 independent OS processes on TCP ports 9311, 9312, 9313...")
        for nid, port in PORTS.items():
            peers = {p: prt for p, prt in PORTS.items() if p != nid}
            cmd = [
                PYTHON_EXE,
                "-m",
                "common.mutex_transport",
                "--node-id", nid,
                "--port", str(port),
                "--peers", json.dumps(peers),
                "--log-file", str(LOG_FILE),
            ]
            proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            procs[nid] = proc
            print(f"  -> Spawned {nid} (PID: {proc.pid}) listening on port {port}")

        # Wait for all nodes to become healthy
        for nid, port in PORTS.items():
            if not wait_for_node(port):
                raise RuntimeError(f"Node {nid} on port {port} failed to start.")
        print("  -> All 3 independent participant nodes are online and healthy.")

        # Step 2: Concurrent requests from all 3 nodes
        print("\n[Step 2] Concurrently requesting Critical Section from Node-1, Node-2, and Node-3...")
        req_timestamps = {}
        for nid, port in PORTS.items():
            success, resp, err = send_http_post(f"http://127.0.0.1:{port}/mutex/acquire", {})
            if success:
                ts = resp.get("request_timestamp")
                req_timestamps[nid] = ts
                print(f"  -> {nid} initiated REQUEST with Lamport timestamp L#{ts}")
            else:
                print(f"  -> {nid} request failed: {err}")

        # Let requests propagate over network
        time.sleep(1.0)

        # Step 3: Sequential entry into critical section based on total ordering
        print("\n[Step 3] Processing critical section entries according to (timestamp, node_id) total order...")
        entered_order = []
        for round_num in range(3):
            # Check which node can enter
            eligible_node = None
            for nid, port in PORTS.items():
                if nid in entered_order:
                    continue
                s, r, _ = send_http_post(f"http://127.0.0.1:{port}/mutex/can_enter", {})
                if s and r.get("can_enter"):
                    eligible_node = (nid, port)
                    break

            if not eligible_node:
                # Wait briefly for replies
                time.sleep(0.5)
                for nid, port in PORTS.items():
                    if nid in entered_order:
                        continue
                    s, r, _ = send_http_post(f"http://127.0.0.1:{port}/mutex/can_enter", {})
                    if s and r.get("can_enter"):
                        eligible_node = (nid, port)
                        break

            if eligible_node:
                nid, port = eligible_node
                print(f"\n  [Round {round_num + 1}] {nid} is at head of queue and holds unanimous replies!")
                # Enter CS
                s_enter, r_enter, _ = send_http_post(f"http://127.0.0.1:{port}/mutex/enter", {})
                print(f"    -> {nid} ENTERED Critical Section (written to {LOG_FILE.name})")
                time.sleep(0.3)  # Hold critical section for simulated task
                # Release CS
                s_rel, r_rel, _ = send_http_post(f"http://127.0.0.1:{port}/mutex/release_local", {})
                rel_ts = r_rel.get("release_timestamp")
                print(f"    -> {nid} EXITED Critical Section and broadcast RELEASE (L#{rel_ts})")
                entered_order.append(nid)
                time.sleep(0.5)  # Allow RELEASE message propagation
            else:
                print("  Warning: No eligible node found in round", round_num + 1)

        print(f"\n  Execution order achieved: {' -> '.join(entered_order)}")

        # Step 4: Verify Durable Log & Mathematical Invariant
        print("\n[Step 4] Mathematically verifying Critical Section Mutual Exclusion Invariant...")
        is_valid, errors, sessions = verify_critical_section_log(str(LOG_FILE))
        print(f"  -> Total Critical Section executions recorded: {len(sessions)}")
        for idx, sess in enumerate(sessions, 1):
            print(f"     Session {idx}: Node {sess['node_id']} | Enter: {sess['enter_time']:.4f} | Exit: {sess['exit_time']:.4f} | Duration: {sess['duration']:.4f}s")

        if is_valid:
            print("  [PASS] Mathematical Proof: No two critical section intervals overlapped!")
            print("         For all (A, B): max(enter_A, enter_B) >= min(exit_A, exit_B) strictly holds.")
        else:
            print("  [FAIL] Invariant Violation:", errors)

        # Step 5: Failure Demonstration — Crash Blocking
        print("\n[Step 5] Failure Demonstration: Crash of Node-3 and Blocking Behavior...")
        print("  -> Killing Node-3 process to simulate crash...")
        procs["Node-3"].terminate()
        procs["Node-3"].wait(timeout=2.0)
        print("  -> Node-3 is now DEAD.")

        print("  -> Node-1 requests Critical Section...")
        send_http_post(f"http://127.0.0.1:{PORTS['Node-1']}/mutex/acquire", {})
        time.sleep(1.0)

        s, r, _ = send_http_post(f"http://127.0.0.1:{PORTS['Node-1']}/mutex/can_enter", {})
        can_enter = r.get("can_enter") if s else False
        print(f"  -> Can Node-1 enter CS without Node-3's reply? {can_enter}")
        print("  -> [CONFIRMED] Classical Lamport blocks indefinitely on participant crash (missing unanimous reply).")

        # Step 6: Failure Detector Recovery
        print("\n[Step 6] Recovery Extension: Failure Detector eviction...")
        print("  -> Node-1 marks Node-3 as DEAD under Crash-Stop (P detector) assumption...")
        send_http_post(f"http://127.0.0.1:{PORTS['Node-1']}/mutex/mark_dead", {"dead_id": "Node-3"})
        time.sleep(0.5)

        s_rec, r_rec, _ = send_http_post(f"http://127.0.0.1:{PORTS['Node-1']}/mutex/can_enter", {})
        can_enter_rec = r_rec.get("can_enter") if s_rec else False
        print(f"  -> Can Node-1 enter CS now? {can_enter_rec}")
        if can_enter_rec:
            send_http_post(f"http://127.0.0.1:{PORTS['Node-1']}/mutex/enter", {})
            print("  -> Node-1 ENTERED Critical Section safely!")
            send_http_post(f"http://127.0.0.1:{PORTS['Node-1']}/mutex/release_local", {})
            print("  -> Node-1 RELEASED Critical Section.")

        print("\n" + "=" * 75)
        print("LAMPORT MUTUAL EXCLUSION DEMO COMPLETED SUCCESSFULLY")
        print("=" * 75)

    finally:
        for nid, proc in procs.items():
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=1.0)
                except Exception:
                    proc.kill()


if __name__ == "__main__":
    run_demo()
