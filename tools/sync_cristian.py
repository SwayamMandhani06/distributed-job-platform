"""
Cristian's Physical Clock Synchronization Demonstration (Unit III — Synchronization).

Demonstrates:
  1. Measurement of client send time (T0) and client receive time (T1).
  2. Server physical timestamp (T_server) capture.
  3. Computation of Round-Trip Delay: RTT = T1 - T0.
  4. Calculation of Estimated Server Time at T1: T_estimated = T_server + (RTT / 2).
  5. Calculation of Estimated Clock Offset: theta = T_server - ((T0 + T1) / 2).
  6. Analysis of uncertainty (+/- RTT / 2), symmetric latency assumptions, and distinction from NTP.

Usage:
  # Against live running scheduler:
  python -m tools.sync_cristian --url http://localhost:8000

  # Standalone synthetic simulation (no live server needed):
  python -m tools.sync_cristian --simulate
"""

import argparse
import sys
import time
from common.cristian import compute_cristian_sync, query_cristian_server


def run_synthetic_demo() -> None:
    print("=" * 75)
    print("  Cristian's Clock Synchronization -- Synthetic Simulation (Unit III)")
    print("=" * 75)
    print("Scenario: Client clock has drifted 3.500s behind server clock.")
    print("Network introduces 40ms total round-trip latency (20ms each way).\n")

    # Injected timestamps:
    # Client starts at t=100.000
    # True server time at t=100.000 is 103.500
    # Request reaches server 20ms later: server timestamp = 103.520
    # Response reaches client 20ms later: client timestamp = 100.040
    t0 = 100.000000
    t_server = 103.520000
    t1 = 100.040000

    results = compute_cristian_sync(t0=t0, t_server=t_server, t1=t1)

    print(f"Timestamp Capture:")
    print(f"  [T0] Client Send Time       : {results['t0_client_send']:.6f}s")
    print(f"  [Ts] Server Timestamp       : {results['t_server_timestamp']:.6f}s")
    print(f"  [T1] Client Receive Time    : {results['t1_client_receive']:.6f}s")
    print("-" * 75)
    print(f"Computed Metrics:")
    print(f"  Round-Trip Delay (RTT)      : {results['round_trip_delay_seconds'] * 1000:.2f} ms")
    print(f"  Estimated Server Time at T1 : {results['estimated_server_time_at_t1']:.6f}s")
    print(f"  Calculated Clock Offset (theta) : {results['clock_offset_seconds']:+.6f}s")
    print(f"  Uncertainty Bound (+/- RTT/2)   : +/- {results['uncertainty_seconds'] * 1000:.2f} ms")
    print("-" * 75)
    print("Syllabus & Viva Theoretical Notes:")
    print("  1. Physical Offset Interpretation:")
    print(f"     Adjusting local clock: local_clock := local_clock + ({results['clock_offset_seconds']:+.6f}s).")
    print("  2. Fundamental Assumption:")
    print("     The algorithm assumes network latency is symmetric (T0 -> server == server -> T1).")
    print("     If uplink takes 30ms and downlink takes 10ms, offset error is 10ms.")
    print("  3. Distinction from Network Time Protocol (NTP):")
    print("     - Cristian uses 3 timestamps (T0, Ts, T1); server turnaround time is merged.")
    print("     - NTP uses 4 timestamps (T1 send, T2 receive, T3 transmit, T4 receive),")
    print("       explicitly isolating server processing delay: (T4 - T1) - (T3 - T2).")
    print("=" * 75)


def run_live_demo(url: str) -> None:
    print("=" * 75)
    print(f"  Cristian's Clock Synchronization -- Live Server Query ({url})")
    print("=" * 75)
    try:
        results = query_cristian_server(server_url=url, timeout_seconds=3.0)
        print(f"Success! Time response received from coordinator:\n")
        print(f"  [T0] Client Request Sent    : {results['t0_client_send']:.6f}")
        print(f"  [Ts] Server Time Received   : {results['t_server_timestamp']:.6f}")
        print(f"  [T1] Client Response Received: {results['t1_client_receive']:.6f}")
        print("-" * 75)
        print(f"  Measured Round-Trip Delay   : {results['round_trip_delay_seconds'] * 1000:.3f} ms")
        print(f"  Estimated Clock Offset (theta)  : {results['clock_offset_seconds']:+.6f} seconds")
        print(f"  Accuracy Bound (+/- RTT/2)      : +/- {results['uncertainty_seconds'] * 1000:.3f} ms")
        print("=" * 75)
    except Exception as exc:
        print(f"[Notice] Could not connect to live server at {url}: {exc}")
        print("Falling back to synthetic simulation mode...\n")
        run_synthetic_demo()


def main() -> None:
    parser = argparse.ArgumentParser(description="Cristian's Physical Clock Synchronization Demo")
    parser.add_argument("--url", default="http://localhost:8000", help="Base URL of coordinator service")
    parser.add_argument("--simulate", action="store_true", help="Run deterministic synthetic calculation")
    args = parser.parse_args()

    if args.simulate:
        run_synthetic_demo()
    else:
        run_live_demo(args.url)


if __name__ == "__main__":
    main()
