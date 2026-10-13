"""
Real Network Transport Layer for Lamport Distributed Mutual Exclusion (Unit III).

Theoretical Background (Leslie Lamport, 1978):
----------------------------------------------
"Time, Clocks, and the Ordering of Events in a Distributed System" (CACM 1978).

In a distributed system without shared memory or a global clock, independent processes
coordinate mutually exclusive access to a shared critical section using logical clocks
and inter-process message passing over actual network sockets (TCP/HTTP).

Protocol Message Flow:
----------------------
1. REQUEST(T_i, P_i): Broadcast to all peers when P_i wishes to enter the critical section.
2. REPLY(T_j, P_j): Sent by each peer P_j acknowledging P_i's request.
3. RELEASE(T_i, P_i): Broadcast to all peers when P_i exits the critical section.

Ordering & Entry Rules:
-----------------------
- Total order on requests: (T_a, P_a) < (T_b, P_b) iff T_a < T_b or (T_a == T_b and P_a < P_b).
- A process enters the critical section iff:
  1. Its own request (T_i, P_i) is at the HEAD of its local request queue.
  2. It has received a REPLY (or message with timestamp > T_i) from EVERY other live participant.

Failure Assumptions & Crash Model:
----------------------------------
- Classical Lamport (1978) strictly requires unanimous replies. If ANY participant crashes
  or hangs without replying, the entire protocol BLOCKS INDEFINITELY.
- To safely recover without violating safety, this module implements an explicit failure
  detector hook `mark_node_dead()`, which assumes a Perfect Failure Detector (P) under
  a Crash-Stop model (Chandra & Toueg, 1996) with Strong Accuracy.
"""

import argparse
import json
import logging
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib import request as url_request
from urllib.error import URLError

from common.clocks import LamportClock

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


def send_http_post(
    url: str,
    payload: Dict[str, Any],
    timeout_seconds: float = 1.5,
) -> Tuple[bool, Optional[Dict[str, Any]], str]:
    """Send an HTTP POST request over TCP socket with timeout."""
    data_bytes = json.dumps(payload).encode("utf-8")
    req = url_request.Request(
        url,
        data=data_bytes,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with url_request.urlopen(req, timeout=timeout_seconds) as response:
            body = response.read().decode("utf-8")
            parsed = json.loads(body) if body else {}
            return True, parsed, ""
    except Exception as exc:
        return False, None, str(exc)


def send_http_get(
    url: str,
    timeout_seconds: float = 1.5,
) -> Tuple[bool, Optional[Dict[str, Any]], str]:
    """Send an HTTP GET request over TCP socket with timeout."""
    req = url_request.Request(url, method="GET")
    try:
        with url_request.urlopen(req, timeout=timeout_seconds) as response:
            body = response.read().decode("utf-8")
            parsed = json.loads(body) if body else {}
            return True, parsed, ""
    except Exception as exc:
        return False, None, str(exc)


class NetworkedLamportMutexNode:
    """Independent process node executing Lamport Distributed Mutual Exclusion over HTTP/TCP."""

    def __init__(
        self,
        node_id: str,
        port: int,
        peers: Dict[str, int],  # peer_id -> port
        shared_log_file: Optional[str] = None,
        simulated_delay_seconds: float = 0.0,
    ) -> None:
        self.node_id = str(node_id)
        self.port = int(port)
        self.peers = {str(k): int(v) for k, v in peers.items()}
        self.all_node_ids = sorted([self.node_id] + list(self.peers.keys()))
        self.shared_log_file = shared_log_file
        self.simulated_delay_seconds = float(simulated_delay_seconds)

        self.clock = LamportClock()
        self.request_queue: List[Tuple[int, str]] = []  # List of (timestamp, node_id)
        self.replies_received: Set[str] = set()
        self.dead_nodes: Set[str] = set()

        self._current_request_ts: Optional[int] = None
        self._in_critical_section = False
        self._waiting = False
        self._is_alive = True

        self._lock = threading.RLock()
        self.server: Optional[HTTPServer] = None
        self.logger = logging.getLogger(f"LamportMutex-{node_id}")
        self.event_history: List[Dict[str, Any]] = []

    @property
    def in_critical_section(self) -> bool:
        with self._lock:
            return self._in_critical_section

    @property
    def is_waiting(self) -> bool:
        with self._lock:
            return self._waiting

    @property
    def is_alive(self) -> bool:
        with self._lock:
            return self._is_alive

    def set_alive(self, alive: bool) -> None:
        with self._lock:
            self._is_alive = alive
            self.logger.info("Node %s is_alive set to %s", self.node_id, alive)

    def mark_node_dead(self, dead_id: str) -> None:
        """Evict an unavailable participant to prevent indefinite blocking (P detector assumption)."""
        with self._lock:
            if dead_id in self.peers:
                self.dead_nodes.add(dead_id)
                self.request_queue = [r for r in self.request_queue if r[1] != dead_id]
                self.logger.warning(
                    "Node %s marked %s as DEAD; excluded from required quorum",
                    self.node_id,
                    dead_id,
                )

    def _insert_request(self, ts: int, sender_id: str) -> None:
        """Insert (ts, sender_id) into local queue maintaining (timestamp, node_id) total order."""
        item = (ts, sender_id)
        if item not in self.request_queue:
            self.request_queue.append(item)
            self.request_queue.sort(key=lambda x: (x[0], x[1]))

    def can_enter(self) -> bool:
        """Evaluate Lamport's two conditions for entering the critical section."""
        with self._lock:
            if not self._waiting or self._in_critical_section or self._current_request_ts is None:
                return False

            # Condition 1: Own request is at head of queue
            if not self.request_queue:
                return False
            head = self.request_queue[0]
            if head != (self._current_request_ts, self.node_id):
                return False

            # Condition 2: Received replies from all live participants
            required_replies = set(self.all_node_ids) - self.dead_nodes
            if not required_replies.issubset(self.replies_received):
                return False

            return True

    def request_critical_section(self) -> int:
        """Initiate distributed request to enter critical section."""
        with self._lock:
            if not self._is_alive:
                raise RuntimeError(f"Node {self.node_id} is offline/crashed.")
            if self._waiting or self._in_critical_section:
                return self._current_request_ts or self.clock.get_time()

            req_ts = self.clock.send_event()
            self._current_request_ts = req_ts
            self._waiting = True
            self.replies_received = {self.node_id}  # Local reply
            self._insert_request(req_ts, self.node_id)

            self.logger.info("Node %s INITIATED request for CS at L#%d (queue: %s)", self.node_id, req_ts, self.request_queue)
            self._log_event("REQUEST_INITIATED", req_ts, {"queue": list(self.request_queue)})

        # Broadcast REQUEST to all live peers in background or synchronous dispatch
        self._broadcast_request(req_ts)
        return req_ts

    def _broadcast_request(self, req_ts: int) -> None:
        """Broadcast REQUEST(req_ts, self.node_id) to all peers over TCP/HTTP."""
        for peer_id, peer_port in self.peers.items():
            if peer_id in self.dead_nodes:
                continue

            def _send(p_id=peer_id, p_port=peer_port):
                if self.simulated_delay_seconds > 0:
                    time.sleep(self.simulated_delay_seconds)
                url = f"http://127.0.0.1:{p_port}/mutex/request"
                payload = {"timestamp": req_ts, "sender_id": self.node_id}
                success, resp, err = send_http_post(url, payload, timeout_seconds=2.0)
                if not success:
                    self.logger.warning("Failed to send REQUEST to %s:%d: %s", p_id, p_port, err)

            t = threading.Thread(target=_send, daemon=True)
            t.start()

    def handle_incoming_request(self, req_ts: int, sender_id: str) -> Dict[str, Any]:
        """Handle incoming REQUEST: update clock, insert to queue, and send REPLY."""
        with self._lock:
            if not self._is_alive:
                return {"status": "error", "message": "Node is offline"}

            self.clock.receive_event(req_ts)
            self._insert_request(req_ts, sender_id)
            reply_ts = self.clock.send_event()
            self._log_event("REQUEST_RECEIVED", req_ts, {"sender": sender_id, "reply_ts": reply_ts})
            self.logger.info("Node %s received REQUEST from %s (L#%d). Replying with L#%d", self.node_id, sender_id, req_ts, reply_ts)

        # Asynchronously send REPLY back to sender over HTTP
        peer_port = self.peers.get(sender_id)
        if peer_port:
            def _send_reply():
                if self.simulated_delay_seconds > 0:
                    time.sleep(self.simulated_delay_seconds)
                url = f"http://127.0.0.1:{peer_port}/mutex/reply"
                payload = {"timestamp": reply_ts, "sender_id": self.node_id}
                send_http_post(url, payload, timeout_seconds=2.0)

            threading.Thread(target=_send_reply, daemon=True).start()

        return {"status": "ok", "reply_timestamp": reply_ts}

    def handle_incoming_reply(self, reply_ts: int, sender_id: str) -> Dict[str, Any]:
        """Handle incoming REPLY: update clock, record reply."""
        with self._lock:
            if not self._is_alive:
                return {"status": "error", "message": "Node is offline"}

            self.clock.receive_event(reply_ts)
            self.replies_received.add(sender_id)
            self._log_event("REPLY_RECEIVED", reply_ts, {"sender": sender_id, "total_replies": len(self.replies_received)})
            self.logger.info(
                "Node %s received REPLY from %s (L#%d). Total replies: %d / %d",
                self.node_id,
                sender_id,
                reply_ts,
                len(self.replies_received),
                len(set(self.all_node_ids) - self.dead_nodes),
            )
            return {"status": "ok"}

    def enter_critical_section(self) -> bool:
        """Attempt to transition into critical section if conditions are met."""
        with self._lock:
            if not self.can_enter():
                return False

            self._in_critical_section = True
            self._waiting = False
            enter_wall_time = time.time()
            self._log_event("CS_ENTER", self.clock.get_time(), {
                "request_ts": self._current_request_ts,
                "wall_time": enter_wall_time,
            })
            self.logger.info(">>> CS ENTER: Node %s ENTERED Critical Section (Req L#%d) <<<", self.node_id, self._current_request_ts)

            # Record entry in durable shared log if configured
            if self.shared_log_file:
                self._record_cs_event("ENTER", enter_wall_time, self._current_request_ts)

            return True

    def release_critical_section(self) -> int:
        """Leave critical section and broadcast RELEASE to all live peers."""
        with self._lock:
            if not self._in_critical_section:
                return self.clock.get_time()

            exit_wall_time = time.time()
            release_ts = self.clock.send_event()
            req_ts = self._current_request_ts
            self._in_critical_section = False
            self._current_request_ts = None
            self.replies_received.clear()

            # Remove own request from local queue
            self.request_queue = [
                r for r in self.request_queue if not (r[1] == self.node_id and r[0] == req_ts)
            ]

            self._log_event("CS_EXIT", release_ts, {"req_ts": req_ts, "wall_time": exit_wall_time})
            self.logger.info("<<< CS EXIT: Node %s EXITED Critical Section (Release L#%d) >>>", self.node_id, release_ts)

            if self.shared_log_file:
                self._record_cs_event("EXIT", exit_wall_time, req_ts)

        # Broadcast RELEASE to all live peers over HTTP
        for peer_id, peer_port in self.peers.items():
            if peer_id in self.dead_nodes:
                continue

            def _send_rel(p_id=peer_id, p_port=peer_port):
                if self.simulated_delay_seconds > 0:
                    time.sleep(self.simulated_delay_seconds)
                url = f"http://127.0.0.1:{p_port}/mutex/release"
                payload = {"timestamp": release_ts, "sender_id": self.node_id}
                send_http_post(url, payload, timeout_seconds=2.0)

            threading.Thread(target=_send_rel, daemon=True).start()

        return release_ts

    def handle_incoming_release(self, rel_ts: int, sender_id: str) -> Dict[str, Any]:
        """Handle incoming RELEASE: update clock and remove sender's request from local queue."""
        with self._lock:
            if not self._is_alive:
                return {"status": "error", "message": "Node is offline"}

            self.clock.receive_event(rel_ts)
            for i, (ts, sender) in enumerate(self.request_queue):
                if sender == sender_id:
                    del self.request_queue[i]
                    break

            self._log_event("RELEASE_RECEIVED", rel_ts, {"sender": sender_id, "queue": list(self.request_queue)})
            self.logger.info("Node %s processed RELEASE from %s. Queue now: %s", self.node_id, sender_id, self.request_queue)
            return {"status": "ok"}

    def _record_cs_event(self, event_type: str, wall_time: float, req_ts: Optional[int]) -> None:
        """Atomically append critical section event to the shared test resource ledger file."""
        if not self.shared_log_file:
            return
        log_entry = {
            "event": event_type,
            "node_id": self.node_id,
            "req_ts": req_ts,
            "wall_time": wall_time,
            "time_iso": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(wall_time)) + f".{int((wall_time % 1) * 1000):03d}Z",
        }
        try:
            with open(self.shared_log_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(log_entry) + "\n")
                f.flush()
        except Exception as exc:
            self.logger.error("Failed to write to shared CS log file: %s", exc)

    def _log_event(self, event_type: str, ts: int, details: Dict[str, Any]) -> None:
        self.event_history.append({
            "event": event_type,
            "lamport_ts": ts,
            "wall_time": time.time(),
            "details": details,
        })

    def get_status(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "node_id": self.node_id,
                "port": self.port,
                "is_alive": self._is_alive,
                "clock": self.clock.get_time(),
                "waiting": self._waiting,
                "in_critical_section": self._in_critical_section,
                "current_request_ts": self._current_request_ts,
                "request_queue": list(self.request_queue),
                "replies_received": list(self.replies_received),
                "dead_nodes": list(self.dead_nodes),
            }


class MutexHTTPHandler(BaseHTTPRequestHandler):
    """HTTP request handler for Lamport mutual exclusion nodes."""

    node: Optional[NetworkedLamportMutexNode] = None

    def log_message(self, format: str, *args: Any) -> None:
        pass  # Suppress default HTTP server stderr noise

    def _send_json(self, status_code: int, data: Dict[str, Any]) -> None:
        body = json.dumps(data).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path == "/mutex/status":
            self._send_json(200, self.node.get_status())
        elif self.path == "/health":
            self._send_json(200, {"status": "ok", "alive": self.node.is_alive})
        else:
            self._send_json(404, {"error": "Not found"})

    def do_POST(self) -> None:
        content_length = int(self.headers.get("Content-Length", 0))
        raw_body = self.rfile.read(content_length).decode("utf-8") if content_length > 0 else "{}"
        try:
            data = json.loads(raw_body)
        except Exception:
            data = {}

        if self.path == "/health":
            self._send_json(200, {"status": "ok", "alive": self.node.is_alive})
        elif self.path == "/mutex/request":
            resp = self.node.handle_incoming_request(data.get("timestamp", 0), data.get("sender_id", ""))
            self._send_json(200, resp)
        elif self.path == "/mutex/reply":
            resp = self.node.handle_incoming_reply(data.get("timestamp", 0), data.get("sender_id", ""))
            self._send_json(200, resp)
        elif self.path == "/mutex/release":
            resp = self.node.handle_incoming_release(data.get("timestamp", 0), data.get("sender_id", ""))
            self._send_json(200, resp)
        elif self.path == "/mutex/acquire":
            req_ts = self.node.request_critical_section()
            self._send_json(200, {"status": "requested", "request_timestamp": req_ts})
        elif self.path == "/mutex/can_enter":
            self._send_json(200, {"can_enter": self.node.can_enter()})
        elif self.path == "/mutex/enter":
            entered = self.node.enter_critical_section()
            self._send_json(200, {"entered": entered})
        elif self.path == "/mutex/release_local":
            rel_ts = self.node.release_critical_section()
            self._send_json(200, {"status": "released", "release_timestamp": rel_ts})
        elif self.path == "/mutex/mark_dead":
            dead_id = data.get("dead_id", "")
            self.node.mark_node_dead(dead_id)
            self._send_json(200, {"status": "marked_dead", "dead_id": dead_id})
        elif self.path == "/mutex/set_alive":
            alive = bool(data.get("alive", True))
            self.node.set_alive(alive)
            self._send_json(200, {"status": "updated", "alive": alive})
        else:
            self._send_json(404, {"error": "Unknown endpoint"})


def start_mutex_node_server(node: NetworkedLamportMutexNode) -> HTTPServer:
    """Start an HTTPServer for the given node in a background thread."""
    handler_class = type("BoundMutexHandler", (MutexHTTPHandler,), {"node": node})
    server = HTTPServer(("127.0.0.1", node.port), handler_class)
    node.server = server
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    return server


def verify_critical_section_log(log_path: str) -> Tuple[bool, List[str], List[Dict[str, Any]]]:
    """Parse shared critical section event log and mathematically verify non-overlap.

    Safety Condition:
    For every pair of critical section executions (A, B) where A != B:
        Interval_A [t_enter_A, t_exit_A] and Interval_B [t_enter_B, t_exit_B] MUST be disjoint:
        max(t_enter_A, t_enter_B) >= min(t_exit_A, t_exit_B)
    """
    if not os.path.exists(log_path):
        return False, ["Log file does not exist"], []

    events: List[Dict[str, Any]] = []
    with open(log_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    events.append(json.loads(line))
                except Exception:
                    pass

    # Pair up ENTER and EXIT into sessions
    active_entries: Dict[str, Dict[str, Any]] = {}
    sessions: List[Dict[str, Any]] = []
    errors: List[str] = []

    for ev in events:
        nid = ev["node_id"]
        etype = ev["event"]
        if etype == "ENTER":
            if nid in active_entries:
                errors.append(f"Node {nid} entered CS again before exiting previous execution!")
            active_entries[nid] = ev
        elif etype == "EXIT":
            if nid not in active_entries:
                errors.append(f"Node {nid} exited CS without a preceding ENTER event!")
            else:
                entry_ev = active_entries.pop(nid)
                sessions.append({
                    "node_id": nid,
                    "req_ts": entry_ev.get("req_ts"),
                    "enter_time": entry_ev["wall_time"],
                    "exit_time": ev["wall_time"],
                    "duration": ev["wall_time"] - entry_ev["wall_time"],
                })

    if active_entries:
        errors.append(f"Unmatched ENTER events remaining at end of log: {list(active_entries.keys())}")

    # Verify mutual exclusion invariant: pairwise interval disjointness
    for i in range(len(sessions)):
        for j in range(i + 1, len(sessions)):
            s1 = sessions[i]
            s2 = sessions[j]
            # Two intervals [t1_in, t1_out] and [t2_in, t2_out] overlap if:
            # max(t1_in, t2_in) < min(t1_out, t2_out)
            overlap_start = max(s1["enter_time"], s2["enter_time"])
            overlap_end = min(s1["exit_time"], s2["exit_time"])
            if overlap_start < overlap_end:
                err = (
                    f"CRITICAL SECTION OVERLAP DETECTED! "
                    f"Session {s1['node_id']} [{s1['enter_time']:.4f} - {s1['exit_time']:.4f}] overlaps with "
                    f"Session {s2['node_id']} [{s2['enter_time']:.4f} - {s2['exit_time']:.4f}] by {overlap_end - overlap_start:.4f}s"
                )
                errors.append(err)

    is_valid = len(errors) == 0 and len(sessions) > 0
    return is_valid, errors, sessions


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a standalone Networked Lamport Mutex Node.")
    parser.add_argument("--node-id", required=True, help="Unique node identifier (e.g. Node-1)")
    parser.add_argument("--port", type=int, required=True, help="Port to listen on (e.g. 9311)")
    parser.add_argument("--peers", required=True, help="JSON map of peer_id to port, e.g. '{\"Node-2\": 9312}'")
    parser.add_argument("--log-file", default="shared_cs_audit.log", help="Path to shared audit ledger")
    parser.add_argument("--delay", type=float, default=0.0, help="Simulated message delay in seconds")

    args = parser.parse_args()
    peer_dict = json.loads(args.peers)

    node = NetworkedLamportMutexNode(
        node_id=args.node_id,
        port=args.port,
        peers=peer_dict,
        shared_log_file=args.log_file,
        simulated_delay_seconds=args.delay,
    )
    server = start_mutex_node_server(node)
    node.logger.info("Lamport Mutex Node %s running on port %d with peers %s", args.node_id, args.port, peer_dict)

    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        node.logger.info("Stopping node %s...", args.node_id)
        server.shutdown()


if __name__ == "__main__":
    main()
