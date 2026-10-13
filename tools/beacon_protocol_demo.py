"""
Deterministic Demonstration: Beacon Protocol and Liveness Observation (Unit III — Synchronization).

This script demonstrates:
1. Periodic beacon message emission with configurable interval (1.0s) and thresholds (3 missed -> SUSPECTED, 6 missed -> DEAD).
2. Steady-state healthy beacon reception across cluster nodes.
3. Fault injection: a delayed node misses beacons and transitions to SUSPECTED state.
4. False-suspicion refutation and Recovery: node resumes emitting beacons and transitions back to HEALTHY.
5. Permanent crash detection: node remains silent and transitions to DEAD.
6. Theoretical distinction between the Beacon Protocol and database worker heartbeats.
"""

from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from common.beacon_protocol import BeaconEmitter, BeaconMonitor


def print_banner(text: str) -> None:
    print("\n" + "=" * 78)
    print(f" {text}")
    print("=" * 78)


def print_step(step_num: int, title: str) -> None:
    print(f"\n--- [STEP {step_num}] {title} ---")


def run_beacon_demonstration() -> int:
    print_banner("UNIT III: BEACON PROTOCOL & LIVENESS OBSERVATION DEMONSTRATION")
    print("Protocol:    Periodic Message-Oriented Beaconing")
    print("Interval:    1.0s per beacon")
    print("Thresholds:  Missed >= 3 -> SUSPECTED | Missed >= 6 -> DEAD")
    print("Model:       Chandra-Toueg Unreliable Failure Detector (Eventual Weak Accuracy)")

    monitor = BeaconMonitor(
        beacon_interval_seconds=1.0,
        missed_beacon_threshold=3,
        dead_threshold_missed=6,
    )

    emitter_alpha = BeaconEmitter("Node-Alpha", beacon_interval_seconds=1.0)
    emitter_beta = BeaconEmitter("Node-Beta", beacon_interval_seconds=1.0)

    # -------------------------------------------------------------------------
    # STEP 1: Steady-State Healthy Operation
    # -------------------------------------------------------------------------
    print_step(1, "Steady-State Healthy Beacons (t = 1.0s to 3.0s)")
    for t in [1.0, 2.0, 3.0]:
        pkt_a = emitter_alpha.emit_beacon(current_time=t)
        pkt_b = emitter_beta.emit_beacon(current_time=t)
        monitor.record_beacon(pkt_a)
        monitor.record_beacon(pkt_b)
        print(f"  [t={t:.1f}s] Received {pkt_a} and {pkt_b}")

    status_t3 = monitor.evaluate_liveness(current_time=3.0)
    print("\nLiveness Evaluation at t=3.0s:")
    for nid, s in status_t3.items():
        print(f"  * {nid:<12}: State = {s.state:<10} | Missed = {s.missed_beacons} | Last Seq = #{s.last_sequence_number}")

    assert status_t3["Node-Alpha"].state == "HEALTHY"
    assert status_t3["Node-Beta"].state == "HEALTHY"
    print("  [OK] Both nodes confirmed HEALTHY under regular beaconing.")

    # -------------------------------------------------------------------------
    # STEP 2: Network Delay / Silence -> SUSPECTED Transition
    # -------------------------------------------------------------------------
    print_step(2, "Fault Injection: Node-Beta Experiences Silence (t = 4.0s to 6.5s)")
    print("  [!] Node-Beta paused / network-partitioned. Ceases beacon emission.")
    emitter_beta.stop()

    # Node-Alpha continues emitting normally
    for t in [4.0, 5.0, 6.0]:
        pkt_a = emitter_alpha.emit_beacon(current_time=t)
        monitor.record_beacon(pkt_a)
        print(f"  [t={t:.1f}s] Received {pkt_a} (Node-Beta silent)")

    # Evaluate at t = 6.5s (3.5s elapsed since Node-Beta's last beacon at t=3.0)
    status_t65 = monitor.evaluate_liveness(current_time=6.5)
    print("\nLiveness Evaluation at t=6.5s:")
    for nid, s in status_t65.items():
        print(f"  * {nid:<12}: State = {s.state:<10} | Missed = {s.missed_beacons} | Elapsed = {s.elapsed_seconds:.1f}s")

    assert status_t65["Node-Alpha"].state == "HEALTHY"
    assert status_t65["Node-Beta"].state == "SUSPECTED", "Node-Beta must transition to SUSPECTED after missing 3 beacons!"
    print("  [OK] Node-Beta transitioned from HEALTHY -> SUSPECTED as expected.")

    # -------------------------------------------------------------------------
    # STEP 3: Recovery from False Suspicion
    # -------------------------------------------------------------------------
    print_step(3, "Refutation of Crash: Node-Beta Resumes & Recovers (t = 7.0s)")
    print("  [+] Crucial Concept: In an asynchronous network, suspicion != death.")
    print("  [+] Node-Beta partition clears; Node-Beta resumes beaconing at t=7.0s.")
    emitter_beta.resume()

    pkt_b_resume = emitter_beta.emit_beacon(current_time=7.0)
    rec_status = monitor.record_beacon(pkt_b_resume)
    print(f"  [t=7.0s] Monitor received fresh {pkt_b_resume}")
    print(f"  * Node-Beta Liveness State: {rec_status.state} (Recovered: {rec_status.state_changed})")

    status_t70 = monitor.evaluate_liveness(current_time=7.0)
    assert status_t70["Node-Beta"].state == "HEALTHY", "Node-Beta must recover back to HEALTHY upon receiving beacon!"
    print("  [OK] Node-Beta successfully recovered from SUSPECTED -> HEALTHY.")

    # -------------------------------------------------------------------------
    # STEP 4: Permanent Silence -> DEAD Transition
    # -------------------------------------------------------------------------
    print_step(4, "Permanent Crash Simulation (t = 8.0s to 14.0s)")
    print("  [!] Node-Beta crashes permanently at t=7.5s.")
    emitter_beta.stop()

    for t in range(8, 14):
        pkt_a = emitter_alpha.emit_beacon(current_time=float(t))
        monitor.record_beacon(pkt_a)

    status_t14 = monitor.evaluate_liveness(current_time=14.0)
    print("\nLiveness Evaluation at t=14.0s (7.0s elapsed since Node-Beta last seen):")
    for nid, s in status_t14.items():
        print(f"  * {nid:<12}: State = {s.state:<10} | Missed = {s.missed_beacons} | Elapsed = {s.elapsed_seconds:.1f}s")

    assert status_t14["Node-Beta"].state == "DEAD", "Node-Beta must be marked DEAD after exceeding dead threshold (6)!"
    print("  [OK] Node-Beta transitioned to DEAD after prolonged silence.")

    # -------------------------------------------------------------------------
    # STEP 5: Architectural Comparison
    # -------------------------------------------------------------------------
    print_step(5, "Beacon Protocol vs. Database Worker Heartbeat")
    print("1. Worker Heartbeat (Phase 1-3.2):")
    print("   - Storage-coupled: Workers write to SQLite ('UPDATE workers SET last_heartbeat=NOW').")
    print("   - Direct lease association: Heartbeat staleness directly triggers job reclamation.")
    print("2. Beacon Protocol (This Demonstration):")
    print("   - Message-oriented: Independent periodic liveness packets.")
    print("   - Unreliable Failure Detector: Distinguishes SUSPECTED from DEAD and allows recovery.")

    print_banner("BEACON PROTOCOL DEMONSTRATION COMPLETE")
    return 0


if __name__ == "__main__":
    sys.exit(run_beacon_demonstration())
