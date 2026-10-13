"""
Cristian's Physical Clock Synchronization Algorithm (Flaviu Cristian, 1989).

Theoretical Foundation:
-----------------------
Cristian's algorithm synchronizes a client process's physical clock with a centralized
time server across an asynchronous network with no shared physical clock.

Protocol Sequence:
  1. Client captures its local physical clock time: T0 (transmission time).
  2. Client issues request to time server over network.
  3. Server receives request and records its local physical clock time: T_server.
  4. Server replies with T_server.
  5. Client receives the response and records local physical clock time: T1.

Calculations:
  * Round-Trip Time (RTT):
      RTT = T1 - T0
  * Estimated Server Time when response arrived at client (T1):
      T_estimated = T_server + (RTT / 2)
  * Estimated Clock Offset theta (difference between server and client clock):
      theta = T_estimated - T1 = T_server - ((T0 + T1) / 2)
  * Accuracy / Uncertainty:
      Uncertainty = +/- (RTT / 2)

Key Limitations & Assumptions:
  1. Symmetric Delay Assumption: The algorithm assumes network latency is equal
     in both directions (client->server == server->client). Asymmetric routing
     directly introduces error.
  2. Local Clock Drift: If RTT is large relative to clock drift rates, accuracy degrades.
  3. Distinction from NTP: Network Time Protocol (NTP) uses FOUR timestamps
     (T1 client send, T2 server receive, T3 server transmit, T4 client receive) to
     isolate server processing time. Cristian's algorithm uses THREE timestamps (T0, T_server, T1),
     assuming server turnaround time is negligible or incorporated in T_server.
"""

from typing import Any, Callable, Dict, Optional
import time
import requests


def compute_cristian_sync(t0: float, t_server: float, t1: float) -> Dict[str, Any]:
    """Calculate estimated clock offset and uncertainty given Cristian's timestamps.

    Parameters:
    -----------
    t0 : float
        Client physical time at message transmission.
    t_server : float
        Server physical time captured in the response.
    t1 : float
        Client physical time upon response reception.

    Returns:
    --------
    dict with rtt, estimated_server_time, offset, uncertainty, and diagnostic metrics.
    """
    if t1 < t0:
        raise ValueError(f"Receive time t1 ({t1}) cannot be earlier than send time t0 ({t0})")

    rtt = t1 - t0
    half_rtt = rtt / 2.0
    estimated_server_time_at_t1 = t_server + half_rtt
    offset = t_server - ((t0 + t1) / 2.0)

    return {
        "t0_client_send": round(t0, 6),
        "t_server_timestamp": round(t_server, 6),
        "t1_client_receive": round(t1, 6),
        "round_trip_delay_seconds": round(rtt, 6),
        "estimated_server_time_at_t1": round(estimated_server_time_at_t1, 6),
        "clock_offset_seconds": round(offset, 6),
        "uncertainty_seconds": round(half_rtt, 6),
        "synchronization_status": "synchronized",
        "notes": (
            "Offset theta indicates amount to adjust client clock (client + theta = server). "
            "Assumes symmetric network delay."
        ),
    }


def query_cristian_server(
    server_url: str,
    time_source: Optional[Callable[[], float]] = None,
    timeout_seconds: float = 5.0,
) -> Dict[str, Any]:
    """Perform a live Cristian clock synchronization request against a server endpoint.

    Expects server endpoint (e.g. GET {server_url}/api/time) returning JSON:
        {"server_time": float, ...}
    """
    get_time = time_source or time.time
    t0 = get_time()
    endpoint = f"{server_url.rstrip('/')}/api/time"
    response = requests.get(endpoint, timeout=timeout_seconds)
    t1 = get_time()

    response.raise_for_status()
    payload = response.json()
    t_server = float(payload["server_time"])

    return compute_cristian_sync(t0=t0, t_server=t_server, t1=t1)
