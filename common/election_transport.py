"""
Real Network Message Transport for Bully and Ring Leader Elections (Unit III).

This module implements multi-process, networked election nodes communicating
over real TCP sockets using lightweight HTTP REST endpoints:
  - BullyHTTPNode: Garcia-Molina (1982) Bully algorithm over real TCP/HTTP.
  - RingHTTPNode: Chang-Roberts / Silberschatz Logical Ring algorithm over real TCP/HTTP with dead-successor bypass.

Each node runs in an independent operating system process with its own PID,
listening on a distinct TCP port. Messages (ELECTION, ANSWER/OK, COORDINATOR)
are genuine network packets subject to real network timeouts, socket disconnection,
and process termination.
"""

import argparse
import json
import logging
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict, List, Optional, Tuple
from urllib import request as url_request
from urllib.error import URLError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


def send_http_post(
    url: str,
    payload: Dict[str, Any],
    timeout_seconds: float = 1.0,
) -> Tuple[bool, Optional[Dict[str, Any]], str]:
    """Send an HTTP POST request over TCP socket with timeout.

    Returns:
    --------
    (success: bool, response_json: Optional[dict], error_str: str)
    """
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


# ===========================================================================
# 1. Networked Garcia-Molina Bully Node
# ===========================================================================
class BullyHTTPNode:
    """Independent process node executing the Bully Leader Election algorithm over HTTP."""

    def __init__(
        self,
        node_id: str,
        priority: int,
        port: int,
        peers: Dict[str, Tuple[int, int]],  # peer_id -> (priority, port)
        timeout_seconds: float = 1.0,
    ) -> None:
        self.node_id = node_id
        self.priority = int(priority)
        self.port = int(port)
        self.peers = peers  # Maps id -> (priority, port)
        self.timeout_seconds = timeout_seconds
        self.is_alive = True
        self.current_coordinator: Optional[str] = None
        self.election_in_progress = False
        self.trace_logs: List[str] = []
        self._lock = threading.Lock()
        self.server: Optional[HTTPServer] = None
        self.logger = logging.getLogger(f"BullyNode-{node_id}")

    def log_trace(self, msg: str) -> None:
        with self._lock:
            self.trace_logs.append(msg)
            self.logger.info(msg)

    def get_status(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "algorithm": "bully",
                "node_id": self.node_id,
                "priority": self.priority,
                "port": self.port,
                "is_alive": self.is_alive,
                "current_coordinator": self.current_coordinator,
                "election_in_progress": self.election_in_progress,
                "trace": list(self.trace_logs),
            }

    def handle_election_message(self, sender_id: str, sender_priority: int) -> Dict[str, Any]:
        """Handle incoming /election message from a lower-priority node."""
        with self._lock:
            if not self.is_alive:
                return {"status": "DEAD", "node_id": self.node_id}

        self.log_trace(f"<- Received ELECTION from [{sender_id}] (Pri={sender_priority}). Responding ANSWER (OK).")

        # Asynchronously initiate own election to higher nodes if not already doing so
        threading.Thread(target=self.start_election, daemon=True).start()
        return {"status": "ANSWER", "responder_id": self.node_id, "priority": self.priority}

    def handle_coordinator_message(self, coordinator_id: str, priority: int) -> Dict[str, Any]:
        """Handle incoming /coordinator announcement."""
        with self._lock:
            if not self.is_alive:
                return {"status": "DEAD"}
            self.current_coordinator = coordinator_id
            self.election_in_progress = False

        self.log_trace(f"<- Accepted COORDINATOR announcement: leader=[{coordinator_id}] (Pri={priority})")
        return {"status": "ACK", "node_id": self.node_id}

    def start_election(self) -> str:
        """Initiate Bully election by contacting higher priority peers."""
        with self._lock:
            if not self.is_alive:
                return ""
            if self.election_in_progress:
                return self.current_coordinator or ""
            self.election_in_progress = True

        self.log_trace(f"*** [{self.node_id}] (Pri={self.priority}) initiated BULLY ELECTION ***")

        higher_peers = [
            (pid, pri, port)
            for pid, (pri, port) in self.peers.items()
            if pri > self.priority
        ]

        if not higher_peers:
            # No higher priority peer exists; declare victory immediately
            self.log_trace(f"[{self.node_id}] has highest priority among all peers. Declaring victory immediately!")
            self.declare_coordinator()
            return self.node_id

        answered = False
        for pid, pri, port in higher_peers:
            self.log_trace(f"  -> [{self.node_id}] sending ELECTION to [{pid}] (Pri={pri}, Port={port})...")
            url = f"http://127.0.0.1:{port}/election"
            success, resp, err = send_http_post(
                url,
                {"sender_id": self.node_id, "priority": self.priority},
                timeout_seconds=self.timeout_seconds,
            )
            if success and resp and resp.get("status") == "ANSWER":
                self.log_trace(f"  <- Received ANSWER (OK) from higher node [{pid}]. Stepping down candidacy.")
                answered = True

        if not answered:
            self.log_trace(f"[{self.node_id}] received NO answers from higher nodes within timeout. Declaring victory!")
            self.declare_coordinator()
            return self.node_id
        else:
            with self._lock:
                self.election_in_progress = False
            return ""

    def declare_coordinator(self) -> None:
        """Broadcast COORDINATOR message to all peers over HTTP."""
        with self._lock:
            if not self.is_alive:
                return
            self.current_coordinator = self.node_id
            self.election_in_progress = False

        self.log_trace(f"*** [{self.node_id}] WON ELECTION and declared itself COORDINATOR ***")

        for pid, (pri, port) in self.peers.items():
            if pid != self.node_id:
                url = f"http://127.0.0.1:{port}/coordinator"
                send_http_post(
                    url,
                    {"coordinator_id": self.node_id, "priority": self.priority},
                    timeout_seconds=self.timeout_seconds,
                )

    def fail(self) -> None:
        with self._lock:
            self.is_alive = False
            self.election_in_progress = False
        self.log_trace(f"[{self.node_id}] CRASHED (is_alive=False)")

    def recover(self) -> None:
        with self._lock:
            self.is_alive = True
            self.election_in_progress = False
        self.log_trace(f"[{self.node_id}] RECOVERED (Pri={self.priority}). Initiating bully election!")
        threading.Thread(target=self.start_election, daemon=True).start()


# ===========================================================================
# 2. Networked Chang-Roberts Ring Node
# ===========================================================================
class RingHTTPNode:
    """Independent process node executing the Ring Leader Election algorithm over HTTP."""

    def __init__(
        self,
        node_id: str,
        priority: int,
        port: int,
        ring_topology: List[Tuple[str, int, int]],  # [(id, pri, port), ...] in ring order
        timeout_seconds: float = 1.0,
    ) -> None:
        self.node_id = node_id
        self.priority = int(priority)
        self.port = int(port)
        self.ring_topology = ring_topology
        self.timeout_seconds = timeout_seconds
        self.is_alive = True
        self.current_coordinator: Optional[str] = None
        self.trace_logs: List[str] = []
        self._lock = threading.Lock()
        self.server: Optional[HTTPServer] = None
        self.logger = logging.getLogger(f"RingNode-{node_id}")

    def log_trace(self, msg: str) -> None:
        with self._lock:
            self.trace_logs.append(msg)
            self.logger.info(msg)

    def get_status(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "algorithm": "ring",
                "node_id": self.node_id,
                "priority": self.priority,
                "port": self.port,
                "is_alive": self.is_alive,
                "current_coordinator": self.current_coordinator,
                "trace": list(self.trace_logs),
            }

    def start_election(self) -> None:
        """Initiate ring election by creating candidate list and sending to successor."""
        with self._lock:
            if not self.is_alive:
                return

        self.log_trace(f"*** [{self.node_id}] (Pri={self.priority}) initiated RING ELECTION ***")
        candidates = [{"node_id": self.node_id, "priority": self.priority}]
        self._forward_election(initiator_id=self.node_id, candidates=candidates)

    def handle_election_token(self, initiator_id: str, candidates: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Process incoming election token."""
        with self._lock:
            if not self.is_alive:
                return {"status": "DEAD"}

        if initiator_id == self.node_id:
            # Completed full circuit back to initiator!
            self.log_trace(f"Election token completed circuit back to initiator [{self.node_id}].")
            self.log_trace(f"Collected candidates: {[(c['node_id'], c['priority']) for c in candidates]}")
            winner = max(candidates, key=lambda c: c["priority"])
            elected_id = winner["node_id"]
            elected_pri = winner["priority"]
            self.log_trace(f"*** Node [{elected_id}] (Pri={elected_pri}) ELECTED COORDINATOR ***")
            with self._lock:
                self.current_coordinator = elected_id

            # Announce coordinator around the ring
            threading.Thread(
                target=self._forward_coordinator,
                args=(elected_id, self.node_id),
                daemon=True,
            ).start()
            return {"status": "ELECTED", "coordinator": elected_id}

        # Otherwise: append self if not present, and forward to next successor
        self.log_trace(f"  -> [{self.node_id}] (Pri={self.priority}) visited by election token")
        if not any(c["node_id"] == self.node_id for c in candidates):
            candidates.append({"node_id": self.node_id, "priority": self.priority})

        threading.Thread(
            target=self._forward_election,
            args=(initiator_id, candidates),
            daemon=True,
        ).start()
        return {"status": "FORWARDED"}

    def handle_coordinator_announcement(self, coordinator_id: str, initiator_id: str) -> Dict[str, Any]:
        """Process incoming coordinator announcement."""
        with self._lock:
            if not self.is_alive:
                return {"status": "DEAD"}
            self.current_coordinator = coordinator_id

        self.log_trace(f"  <- [{self.node_id}] accepted COORDINATOR: leader=[{coordinator_id}]")

        if initiator_id != self.node_id:
            # Forward announcement around ring until it reaches initiator
            threading.Thread(
                target=self._forward_coordinator,
                args=(coordinator_id, initiator_id),
                daemon=True,
            ).start()
        return {"status": "ACK"}

    def _forward_election(self, initiator_id: str, candidates: List[Dict[str, Any]]) -> bool:
        """Forward election token to the next living successor in the ring, bypassing dead nodes."""
        n = len(self.ring_topology)
        my_idx = next(i for i, (pid, _, _) in enumerate(self.ring_topology) if pid == self.node_id)

        for step in range(1, n + 1):
            next_idx = (my_idx + step) % n
            next_id, next_pri, next_port = self.ring_topology[next_idx]

            if step > 1:
                bypassed = [self.ring_topology[(my_idx + k) % n][0] for k in range(1, step)]
                self.log_trace(f"  [Bypass] Dead successor(s) {bypassed} -> bypassing to [{next_id}] (Port {next_port})")

            url = f"http://127.0.0.1:{next_port}/ring/election"
            success, resp, _ = send_http_post(
                url,
                {"initiator_id": initiator_id, "candidates": candidates},
                timeout_seconds=self.timeout_seconds,
            )
            if success and resp and resp.get("status") != "DEAD":
                return True

        self.log_trace(f"[{self.node_id}] could not reach any successor in ring!")
        return False

    def _forward_coordinator(self, coordinator_id: str, initiator_id: str) -> bool:
        """Forward coordinator announcement to next living successor."""
        n = len(self.ring_topology)
        my_idx = next(i for i, (pid, _, _) in enumerate(self.ring_topology) if pid == self.node_id)

        for step in range(1, n + 1):
            next_idx = (my_idx + step) % n
            next_id, next_pri, next_port = self.ring_topology[next_idx]
            if next_id == initiator_id:
                # Announcement completed ring circuit
                return True

            url = f"http://127.0.0.1:{next_port}/ring/coordinator"
            success, resp, _ = send_http_post(
                url,
                {"coordinator_id": coordinator_id, "initiator_id": initiator_id},
                timeout_seconds=self.timeout_seconds,
            )
            if success and resp and resp.get("status") != "DEAD":
                return True
        return False

    def fail(self) -> None:
        with self._lock:
            self.is_alive = False
        self.log_trace(f"[{self.node_id}] CRASHED (is_alive=False)")

    def recover(self) -> None:
        with self._lock:
            self.is_alive = True
        self.log_trace(f"[{self.node_id}] RECOVERED (is_alive=True)")


# ===========================================================================
# 3. HTTP Server Request Handler Dispatcher
# ===========================================================================
def make_handler(node: Any):
    class ElectionRequestHandler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            # Suppress default noisy access logs to keep traces clean
            pass

        def do_GET(self):
            if self.path == "/status" or self.path == "/health":
                self._send_json(200, node.get_status())
            else:
                self._send_json(404, {"error": "Not found"})

        def do_POST(self):
            content_length = int(self.headers.get("Content-Length", 0))
            body_bytes = self.rfile.read(content_length) if content_length > 0 else b"{}"
            try:
                body = json.loads(body_bytes.decode("utf-8")) if body_bytes else {}
            except Exception:
                body = {}

            if self.path == "/fail":
                node.fail()
                self._send_json(200, {"status": "failed"})
            elif self.path == "/recover":
                node.recover()
                self._send_json(200, {"status": "recovered"})
            elif self.path == "/start_election":
                threading.Thread(target=node.start_election, daemon=True).start()
                self._send_json(200, {"status": "election_initiated"})
            elif self.path == "/election":
                # Bully election message
                resp = node.handle_election_message(body.get("sender_id", ""), body.get("priority", 0))
                self._send_json(200, resp)
            elif self.path == "/coordinator":
                # Bully coordinator announcement
                resp = node.handle_coordinator_message(body.get("coordinator_id", ""), body.get("priority", 0))
                self._send_json(200, resp)
            elif self.path == "/ring/election":
                # Ring election message
                resp = node.handle_election_token(body.get("initiator_id", ""), body.get("candidates", []))
                self._send_json(200, resp)
            elif self.path == "/ring/coordinator":
                # Ring coordinator announcement
                resp = node.handle_coordinator_announcement(body.get("coordinator_id", ""), body.get("initiator_id", ""))
                self._send_json(200, resp)
            elif self.path == "/shutdown":
                self._send_json(200, {"status": "shutting_down"})
                threading.Thread(target=self.server.shutdown, daemon=True).start()
            else:
                self._send_json(404, {"error": "Not found"})

        def _send_json(self, code: int, data: Dict[str, Any]):
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(data).encode("utf-8"))

    return ElectionRequestHandler


def run_node_server(node: Any, port: int) -> HTTPServer:
    """Start an HTTP server on 127.0.0.1:<port> for the given election node."""
    handler = make_handler(node)
    server = HTTPServer(("127.0.0.1", port), handler)
    node.server = server
    return server
