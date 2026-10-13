"""
Multi-Process Demonstration and Verification of Beacon Protocol & Failure Detection (Unit III).

Runs independent operating system processes communicating over real TCP sockets:
  - Monitor Node (Port 9511)
  - Emitter Node (Independent Python subprocess)

Demonstration Steps:
--------------------
1. Spawn Beacon Monitor process on TCP port 9511.
2. Spawn independent Beacon Emitter process (`Node-Alpha`).
3. Phase 1: Continuous heartbeats -> Monitor evaluates `HEALTHY`.
4. Phase 2: Transient silence / pause (simulating network partition or GC hang):
   - 3 missed beacons -> Monitor transitions `Node-Alpha` to `SUSPECTED`.
5. Phase 3: Recovery demonstration:
   - Beacon resumes -> Monitor immediately refutes suspicion and recovers `Node-Alpha` to `HEALTHY`.
6. Phase 4: Confirmed crash demonstration:
   - Emitter process is killed.
   - 6 missed beacons -> Monitor transitions `Node-Alpha` to confirmed `DEAD`.
7. Limits and False Positives of timeout-based detectors in asynchronous networks.
"""

import os
import subprocess
import sys
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from common.beacon_transport import send_http_get, send_http_post

PYTHON_EXE = sys.executable
MONITOR_PORT = 9511


def wait_for_monitor(port: int, max_retries: int = 25) -> bool:
    for _ in range(max_retries):
        success, resp, _ = send_http_get(f"http://127.0.0.1:{port}/health", timeout_seconds=0.5)
        if success:
            return True
        time.sleep(0.2)
    return False


def run_demo():
    print("=" * 75)
    print("BEACON PROTOCOL & FAILURE DETECTOR — MULTI-PROCESS NETWORK DEMO")
    print("=" * 75)

    monitor_proc = None
    emitter_proc = None

    try:
        # Step 1: Launch Monitor process
        print("\n[Step 1] Spawning Beacon Monitor process on TCP port 9511 (Interval: 0.4s)...")
        monitor_cmd = [
            PYTHON_EXE,
            "-m",
            "common.beacon_transport",
            "--mode", "monitor",
            "--port", str(MONITOR_PORT),
            "--interval", "0.4",
        ]
        monitor_proc = subprocess.Popen(monitor_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print(f"  -> Monitor process running (PID: {monitor_proc.pid})")

        if not wait_for_monitor(MONITOR_PORT):
            raise RuntimeError("Monitor failed to start.")
        print("  -> Monitor is healthy and listening on http://127.0.0.1:9511")

        # Step 2: Launch Emitter process
        print("\n[Step 2] Spawning independent Beacon Emitter process 'Node-Alpha'...")
        emitter_cmd = [
            PYTHON_EXE,
            "-m",
            "common.beacon_transport",
            "--mode", "emitter",
            "--node-id", "Node-Alpha",
            "--monitor-url", f"http://127.0.0.1:{MONITOR_PORT}",
            "--interval", "0.4",
        ]
        emitter_proc = subprocess.Popen(emitter_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print(f"  -> Emitter process running (PID: {emitter_proc.pid})")

        # Step 3: Phase 1 — Healthy state
        print("\n[Step 3] Phase 1: Observing normal beacon exchange...")
        time.sleep(1.5)
        s, liveness, _ = send_http_get(f"http://127.0.0.1:{MONITOR_PORT}/liveness")
        node_status = liveness.get("Node-Alpha", {})
        print(f"  -> Node-Alpha state: {node_status.get('state')} (Missed: {node_status.get('missed_beacons')}, Seq: #{node_status.get('last_sequence_number')})")
        assert node_status.get("state") == "HEALTHY", "Expected HEALTHY state"

        # Step 4: Phase 2 — Transient pause / Suspicion
        print("\n[Step 4] Phase 2: Simulating transient thread pause / network partition (killing emitter temporarily)...")
        emitter_proc.terminate()
        emitter_proc.wait(timeout=2.0)
        print("  -> Emitter process stopped. Waiting for 3 missed beacons (1.4s)...")
        time.sleep(1.4)

        s, liveness, _ = send_http_get(f"http://127.0.0.1:{MONITOR_PORT}/liveness")
        node_status = liveness.get("Node-Alpha", {})
        print(f"  -> Node-Alpha state: {node_status.get('state')} (Missed: {node_status.get('missed_beacons')})")
        assert node_status.get("state") == "SUSPECTED", f"Expected SUSPECTED, got {node_status.get('state')}"
        print("  -> [CONFIRMED] Monitor correctly transitioned node to SUSPECTED!")

        # Step 5: Phase 3 — Node Recovery
        print("\n[Step 5] Phase 3: Node Recovery demonstration (emitter restarts)...")
        emitter_proc = subprocess.Popen(emitter_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print(f"  -> New Emitter process started (PID: {emitter_proc.pid})")
        time.sleep(1.0)

        s, liveness, _ = send_http_get(f"http://127.0.0.1:{MONITOR_PORT}/liveness")
        node_status = liveness.get("Node-Alpha", {})
        print(f"  -> Node-Alpha state: {node_status.get('state')} (Missed: {node_status.get('missed_beacons')}, Seq: #{node_status.get('last_sequence_number')})")
        assert node_status.get("state") == "HEALTHY", "Expected node to RECOVER to HEALTHY"
        print("  -> [CONFIRMED] Monitor refuted suspicion and recovered node to HEALTHY!")

        # Step 6: Phase 4 — Confirmed Crash (DEAD)
        print("\n[Step 6] Phase 4: Simulating permanent node crash...")
        emitter_proc.terminate()
        emitter_proc.wait(timeout=2.0)
        emitter_proc = None
        print("  -> Process terminated. Waiting for 6 missed intervals (2.6s)...")
        time.sleep(2.6)

        s, liveness, _ = send_http_get(f"http://127.0.0.1:{MONITOR_PORT}/liveness")
        node_status = liveness.get("Node-Alpha", {})
        print(f"  -> Node-Alpha state: {node_status.get('state')} (Missed: {node_status.get('missed_beacons')})")
        assert node_status.get("state") == "DEAD", f"Expected DEAD, got {node_status.get('state')}"
        print("  -> [CONFIRMED] Monitor confirmed node failure as DEAD!")

        # Step 7: Transition event log
        print("\n[Step 7] Monitor Transition Event History:")
        s, events_data, _ = send_http_get(f"http://127.0.0.1:{MONITOR_PORT}/events")
        for ev in events_data.get("events", []):
            print(f"  * {ev}")

        # Step 8: Document theoretical limits
        print("\n[Step 8] Theoretical Limits of Timeout-Based Failure Detection:")
        print("  - FLP Impossibility (1985): Perfect consensus/failure detection is impossible")
        print("    in an asynchronous network with even a single crash fault.")
        print("  - False Positives: In asynchronous networks, arbitrary packet delay or OS thread")
        print("    starvation cannot be distinguished from a true crash. A node classified as")
        print("    SUSPECTED or DEAD may simply be slow.")
        print("  - Trade-off: Lower timeouts increase detection speed but drastically increase false positives;")
        print("    higher timeouts reduce false positives but delay fault recovery.")

        print("\n" + "=" * 75)
        print("BEACON PROTOCOL DEMO COMPLETED SUCCESSFULLY")
        print("=" * 75)

    finally:
        if emitter_proc and emitter_proc.poll() is None:
            emitter_proc.terminate()
            try:
                emitter_proc.wait(timeout=1.0)
            except Exception:
                emitter_proc.kill()
        if monitor_proc and monitor_proc.poll() is None:
            monitor_proc.terminate()
            try:
                monitor_proc.wait(timeout=1.0)
            except Exception:
                monitor_proc.kill()


if __name__ == "__main__":
    run_demo()
