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


def compute_cristian_sync(
    t0: float,
    t_server: float,
    t1: float,
    t_server_recv: Optional[float] = None,
    t_server_send: Optional[float] = None,
) -> Dict[str, Any]:
    """Calculate estimated clock offset and uncertainty given Cristian's timestamps.

    Parameters:
    -----------
    t0 : float
        Client physical time at message transmission.
    t_server : float
        Server physical time captured in the response.
    t1 : float
        Client physical time upon response reception.
    t_server_recv : Optional[float]
        Optional server physical time when request arrived at server.
    t_server_send : Optional[float]
        Optional server physical time when response was transmitted by server.

    Returns:
    --------
    dict with rtt, estimated_server_time, offset, uncertainty, and diagnostic metrics.
    """
    if t1 < t0:
        raise ValueError(f"Receive time t1 ({t1}) cannot be earlier than send time t0 ({t0})")

    total_rtt = t1 - t0
    server_processing_time = 0.0
    if t_server_recv is not None and t_server_send is not None:
        if t_server_send >= t_server_recv:
            server_processing_time = t_server_send - t_server_recv

    # Network round trip delay isolating server processing time
    net_rtt = max(0.0, total_rtt - server_processing_time)
    half_net_rtt = net_rtt / 2.0

    # If server_send is present, estimate server time at client receive t1
    reference_server_time = t_server_send if t_server_send is not None else t_server
    estimated_server_time_at_t1 = reference_server_time + half_net_rtt
    offset = reference_server_time + half_net_rtt - t1

    return {
        "t0_client_send": round(t0, 6),
        "t_server_timestamp": round(t_server, 6),
        "t_server_receive": round(t_server_recv, 6) if t_server_recv is not None else None,
        "t_server_send": round(t_server_send, 6) if t_server_send is not None else None,
        "t1_client_receive": round(t1, 6),
        "total_round_trip_seconds": round(total_rtt, 6),
        "server_processing_seconds": round(server_processing_time, 6),
        "network_round_trip_seconds": round(net_rtt, 6),
        "round_trip_delay_seconds": round(total_rtt, 6),
        "estimated_server_time_at_t1": round(estimated_server_time_at_t1, 6),
        "clock_offset_seconds": round(offset, 6),
        "uncertainty_seconds": round(half_net_rtt, 6),
        "actual_os_clock_adjusted": False,
        "simulated_clock_offset": True,
        "synchronization_status": "synchronized",
        "notes": (
            "Calculated software clock offset theta. The operating system physical clock was "
            "NOT modified (modifying physical OS clock requires root/kernel privileges and could "
            "corrupt host timing). Software adjustment formula: synchronized_time = local_time + offset."
        ),
    }


def query_cristian_server(
    server_url: str,
    time_source: Optional[Callable[[], float]] = None,
    timeout_seconds: float = 5.0,
    injected_delay_seconds: float = 0.0,
    injected_jitter_seconds: float = 0.0,
    local_clock_offset_seconds: float = 0.0,
) -> Dict[str, Any]:
    """Perform a live Cristian clock synchronization request against a server endpoint.

    Parameters:
    -----------
    server_url : str
        Base URL of the time coordinator.
    time_source : Optional[Callable[[], float]]
        Callable returning current physical time.
    timeout_seconds : float
        HTTP request timeout.
    injected_delay_seconds : float
        Simulated network delay injected before and after transmission.
    injected_jitter_seconds : float
        Simulated random jitter added to transit latency.
    local_clock_offset_seconds : float
        Simulated local clock drift offset relative to true time.
    """
    raw_time = time_source or time.time
    def get_time() -> float:
        return raw_time() + local_clock_offset_seconds

    t0 = get_time()

    # Simulate outbound network delay/jitter
    if injected_delay_seconds > 0 or injected_jitter_seconds > 0:
        time.sleep(injected_delay_seconds / 2.0 + injected_jitter_seconds)

    endpoint = f"{server_url.rstrip('/')}/api/time"
    try:
        response = requests.get(endpoint, timeout=timeout_seconds)
    except requests.RequestException as req_err:
        raise ConnectionError(f"Failed to reach time server at {endpoint}: {req_err}") from req_err

    # Simulate inbound network delay
    if injected_delay_seconds > 0:
        time.sleep(injected_delay_seconds / 2.0)

    t1 = get_time()

    response.raise_for_status()
    if isinstance(getattr(response, "status_code", None), int) and response.status_code != 200:
        raise ValueError(
            f"Time server returned error HTTP {response.status_code}: {response.text[:200]}"
        )

    try:
        payload = response.json()
    except Exception as parse_err:
        raise ValueError(f"Time server response is not valid JSON: {response.text[:200]}") from parse_err

    if "server_time" not in payload:
        raise ValueError(f"Invalid time server response: missing 'server_time' in {payload}")

    try:
        t_server = float(payload["server_time"])
    except (ValueError, TypeError) as conv_err:
        raise ValueError(f"Invalid 'server_time' value in response: {payload.get('server_time')}") from conv_err

    t_recv = float(payload["server_receive_time"]) if "server_receive_time" in payload else None
    t_send = float(payload["server_send_time"]) if "server_send_time" in payload else None

    return compute_cristian_sync(
        t0=t0,
        t_server=t_server,
        t1=t1,
        t_server_recv=t_recv,
        t_server_send=t_send,
    )
