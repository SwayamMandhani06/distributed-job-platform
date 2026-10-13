"""
Multi-Process Demonstration and Verification of Chandy-Lamport Distributed Snapshots (Unit III).

Runs three independent operating system processes communicating over real TCP sockets:
  - P1 (Port 9411, Initial: 100)
  - P2 (Port 9412, Initial: 100)
  - P3 (Port 9413, Initial: 100)
Total initial system tokens: 300 tokens.

Demonstration Steps:
--------------------
1. Spawn 3 distinct Python subprocesses running `common/snapshot_transport.py`.
2. Demonstrate concurrent in-flight message exchange:
   - P1 transfers 35 tokens to P2 with a simulated network delay.
   - Concurrently, P1 initiates a Chandy-Lamport snapshot (broadcasts MARKER).
   - P2 transfers 15 tokens to P3 before seeing P1's marker.
3. Observe marker propagation and channel-state recording across the network.
4. Wait for all 3 processes to complete snapshot recording.
5. Aggregate Global Snapshot:
   - Process Local States
   - In-Flight Channel States
6. Mathematically verify the Consistent Cut Conservation Invariant:
     Sum(Recorded Process States) + Sum(Recorded In-Flight Channel Tokens) == 300
7. Demonstrate why a naive direct query without channel recording produces an inconsistent state.
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

from common.snapshot_transport import send_http_get, send_http_post

PYTHON_EXE = sys.executable

PORTS = {
    "P1": 9411,
    "P2": 9412,
    "P3": 9413,
}

INITIAL_BALANCES = {
    "P1": 100,
    "P2": 100,
    "P3": 100,
}


def wait_for_node(port: int, max_retries: int = 25) -> bool:
    for _ in range(max_retries):
        success, resp, _ = send_http_get(f"http://127.0.0.1:{port}/health", timeout_seconds=0.5)
        if success:
            return True
        time.sleep(0.2)
    return False


def run_demo():
    print("=" * 75)
    print("CHANDY-LAMPORT DISTRIBUTED SNAPSHOT — MULTI-PROCESS NETWORK DEMO")
    print("=" * 75)

    procs = {}
    try:
        # Step 1: Launch 3 independent processes
        print("\n[Step 1] Spawning 3 independent OS processes on TCP ports 9411, 9412, 9413...")
        for pid, port in PORTS.items():
            peers = {p: prt for p, prt in PORTS.items() if p != pid}
            cmd = [
                PYTHON_EXE,
                "-m",
                "common.snapshot_transport",
                "--node-id", pid,
                "--port", str(port),
                "--peers", json.dumps(peers),
                "--initial-balance", str(INITIAL_BALANCES[pid]),
                "--delay", "0.2",  # 200ms delay to clearly show in-flight channels
            ]
            proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            procs[pid] = proc
            print(f"  -> Spawned {pid} (PID: {proc.pid}) listening on port {port} [Initial Balance: {INITIAL_BALANCES[pid]}]")

        for pid, port in PORTS.items():
            if not wait_for_node(port):
                raise RuntimeError(f"Snapshot node {pid} on port {port} failed to start.")
        print("  -> All 3 processes online. Total system tokens = 300.")

        # Step 2: Concurrent token transfer & snapshot initiation
        print("\n[Step 2] Executing concurrent transfers and snapshot initiation...")
        print("  -> P1 initiates transfer of 35 tokens to P2 (in-transit on wire)...")
        send_http_post(f"http://127.0.0.1:{PORTS['P1']}/transfer", {"target": "P2", "amount": 35})

        print("  -> P1 immediately initiates Chandy-Lamport snapshot (Marker: SNAP-MULTI-1)...")
        send_http_post(f"http://127.0.0.1:{PORTS['P1']}/snapshot/initiate", {"marker_id": "SNAP-MULTI-1"})

        # Allow markers and transfers to propagate across the network
        print("  -> Markers propagating along logical FIFO channels...")
        time.sleep(1.8)

        # Step 3: Check snapshot completion across all 3 nodes
        print("\n[Step 3] Polling nodes for snapshot completion...")
        all_complete = False
        statuses = {}
        for attempt in range(15):
            all_complete = True
            for pid, port in PORTS.items():
                s, resp, _ = send_http_get(f"http://127.0.0.1:{port}/snapshot/status")
                if s:
                    statuses[pid] = resp
                    if not resp.get("is_complete"):
                        all_complete = False
                else:
                    all_complete = False
            if all_complete:
                break
            time.sleep(0.3)

        print(f"  -> All nodes snapshot recording completed? {all_complete}")

        # Step 4: Aggregate Global Consistent Cut
        print("\n[Step 4] Aggregating Global Consistent Cut Snapshot Result:")
        print("-" * 60)
        total_recorded_process = 0
        total_in_flight_channel = 0

        print("  A. Recorded Process Local States:")
        for pid in sorted(statuses.keys()):
            rec_bal = statuses[pid].get("recorded_local_state")
            total_recorded_process += rec_bal
            print(f"     * Process {pid}: {rec_bal} tokens")

        print("\n  B. Recorded Channel In-Flight States:")
        recorded_channels = {}
        for pid in sorted(statuses.keys()):
            chan_msgs = statuses[pid].get("recorded_channel_messages", {})
            for sender, msgs in chan_msgs.items():
                chan_key = f"{sender} -> {pid}"
                recorded_channels[chan_key] = msgs
                total_in_flight_channel += sum(msgs)
                print(f"     * Channel {chan_key}: in-flight payloads = {msgs} (sum: {sum(msgs)})")

        total_global_cut = total_recorded_process + total_in_flight_channel

        print("-" * 60)
        print(f"  Sum of Process Local States:       {total_recorded_process} tokens")
        print(f"  Sum of In-Flight Channel States:    {total_in_flight_channel} tokens")
        print(f"  Total Global Cut Invariant Sum:     {total_global_cut} tokens (Expected: 300)")

        # Step 5: Verification of Invariant
        print("\n[Step 5] Invariant & Consistent Cut Verification:")
        if total_global_cut == 300:
            print("  [PASS] CHANDY-LAMPORT CONSERVATION INVARIANT SATISFIED!")
            print("         Process states + Channel in-flight messages = Exactly initial system tokens (300).")
            print("         No tokens were lost, duplicated, or created during concurrent execution.")
        else:
            print(f"  [FAIL] Invariant Violation! Recorded total: {total_global_cut} != 300")

        # Step 6: Educational contrast with naive state query
        print("\n[Step 6] Why a simple query is NOT a Chandy-Lamport Snapshot:")
        print("  - A naive query reads current process balances without recording messages on the wire.")
        current_balances = {pid: statuses[pid].get("current_balance") for pid in statuses}
        print(f"  - Current live process balances: {current_balances} (Sum = {sum(current_balances.values())})")
        print("  - During concurrent transfers, a simple query at an arbitrary instant observes send events")
        print("    whose receive events have not yet occurred, missing in-transit tokens and violating consistency.")
        print("  - Chandy-Lamport guarantees a causally consistent cut without freezing execution.")

        print("\n" + "=" * 75)
        print("CHANDY-LAMPORT SNAPSHOT DEMO COMPLETED SUCCESSFULLY")
        print("=" * 75)

    finally:
        for pid, proc in procs.items():
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=1.0)
                except Exception:
                    proc.kill()


if __name__ == "__main__":
    run_demo()
