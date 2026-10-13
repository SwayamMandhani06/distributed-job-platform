"""
Real Network Transport Layer for Chandy-Lamport Distributed Snapshots (Unit III).

Theoretical Background (K. Mani Chandy & Leslie Lamport, ACM TOCS 1985):
-------------------------------------------------------------------------
"Distributed Snapshots: Determining Global States of Distributed Systems"

In a distributed system without shared memory or a global clock, determining
a consistent global state (a "consistent cut") is essential for checkpointing,
deadlock detection, stable predicate evaluation, and distributed debugging.

System Model & Assumptions:
---------------------------
1. The distributed system is a directed graph of processes and unidirectional channels.
2. Channels are reliable (no message loss), error-free, and strictly FIFO (First-In, First-Out).
3. The process and channel graph is strongly connected.
4. Snapshot execution does NOT freeze application message processing; transfers continue concurrently.

FIFO Channel Enforcement in Transport:
--------------------------------------
Because HTTP connections over TCP can experience connection interleaving,
this transport layer enforces strict logical FIFO channels by:
  - Assigning a strictly monotonic sequence number (seq) to every message along channel C_ij.
  - Senders serialize messages per outgoing channel.
  - Receivers reorder or queue incoming messages to process them in strict sequence order.

Marker Rules:
-------------
1. Initiator P_i:
   - Records local state S_i.
   - Starts recording on all incoming channels C_ki.
   - Sends MARKER along all outgoing channels C_ik before any subsequent application messages.

2. Marker Receipt by P_i from P_j along C_ji:
   - First time P_i sees marker:
     * Records local state S_i.
     * Records channel C_ji as EMPTY ([]).
     * Starts recording on all other incoming channels C_ki (k != j).
     * Forwards MARKER along all outgoing channels C_ik.
   - Subsequent receipt of marker along C_ji:
     * Stops recording channel C_ji.
     * Channel state is finalized to all messages received on C_ji while recording.
"""

import argparse
import json
import logging
import os
import queue
import sys
import threading
import time
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib import request as url_request
from urllib.error import URLError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


def send_http_post(
    url: str,
    payload: Dict[str, Any],
    timeout_seconds: float = 2.0,
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
    timeout_seconds: float = 2.0,
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


class NetworkedSnapshotNode:
    """Independent process node executing Chandy-Lamport distributed snapshots over TCP/HTTP."""

    def __init__(
        self,
        node_id: str,
        port: int,
        peers: Dict[str, int],  # peer_id -> port
        initial_balance: int = 100,
        simulated_delay_seconds: float = 0.0,
    ) -> None:
        self.node_id = str(node_id)
        self.port = int(port)
        self.peers = {str(k): int(v) for k, v in peers.items()}
        self.all_node_ids = sorted([self.node_id] + list(self.peers.keys()))
        self.balance = int(initial_balance)
        self.simulated_delay_seconds = float(simulated_delay_seconds)

        # Monotonic channel sequence counters (enforces FIFO transport)
        self.outgoing_channel_seq: Dict[str, int] = defaultdict(int)  # target -> next seq
        self.incoming_channel_seq: Dict[str, int] = defaultdict(int)  # sender -> expected seq
        self.incoming_channel_buffers: Dict[str, Dict[int, Dict[str, Any]]] = defaultdict(dict)

        # Snapshot recording structures
        self.active_marker_id: Optional[str] = None
        self.recorded_state: Optional[int] = None
        self.has_recorded_state: bool = False
        self.recording_channels: Set[str] = set()  # senders from which we are actively recording
        self.recorded_channel_messages: Dict[str, List[int]] = defaultdict(list)
        self.markers_received_from: Set[str] = set()

        self._lock = threading.RLock()
        self.server: Optional[HTTPServer] = None
        self.logger = logging.getLogger(f"Snapshot-{node_id}")
        self.event_history: List[str] = []

    def initiate_snapshot(self, marker_id: str = "SNAP-1") -> None:
        """Initiate Chandy-Lamport snapshot as the initiator process."""
        with self._lock:
            self.active_marker_id = marker_id
            # Step 1: Record local state
            self.recorded_state = self.balance
            self.has_recorded_state = True
            msg = f"INITIATED SNAPSHOT({marker_id}): Recorded local balance={self.recorded_state}"
            self.event_history.append(msg)
            self.logger.info("[%s] %s", self.node_id, msg)

            # Step 2: Start recording on all incoming channels
            self.recording_channels = set(self.peers.keys())
            for sender in self.peers:
                self.recorded_channel_messages[sender] = []

        # Step 3: Broadcast MARKER along all outgoing channels
        self._broadcast_marker(marker_id)

    def _broadcast_marker(self, marker_id: str) -> None:
        """Send MARKER token along all outgoing channels respecting FIFO order."""
        for peer_id in self.peers:
            self._send_channel_message(
                target_id=peer_id,
                msg_type="MARKER",
                payload=None,
                marker_id=marker_id,
            )

    def send_transfer(self, target_id: str, amount: int) -> bool:
        """Send application tokens to another process over FIFO logical channel."""
        with self._lock:
            if self.balance < amount or amount <= 0 or target_id not in self.peers:
                return False
            self.balance -= amount
            msg = f"SENT APP({amount}) to {target_id} (New balance: {self.balance})"
            self.event_history.append(msg)
            self.logger.info("[%s] %s", self.node_id, msg)

        self._send_channel_message(
            target_id=target_id,
            msg_type="APP",
            payload=amount,
            marker_id=None,
        )
        return True

    def _send_channel_message(
        self,
        target_id: str,
        msg_type: str,
        payload: Any = None,
        marker_id: Optional[str] = None,
    ) -> None:
        """Enqueue and transmit a message along outgoing channel with sequence number."""
        with self._lock:
            self.outgoing_channel_seq[target_id] += 1
            seq = self.outgoing_channel_seq[target_id]

        target_port = self.peers[target_id]
        body = {
            "msg_type": msg_type,
            "sender": self.node_id,
            "receiver": target_id,
            "payload": payload,
            "marker_id": marker_id,
            "seq": seq,
        }

        def _worker():
            if self.simulated_delay_seconds > 0:
                time.sleep(self.simulated_delay_seconds)
            url = f"http://127.0.0.1:{target_port}/channel/message"
            success, _, err = send_http_post(url, body, timeout_seconds=3.0)
            if not success:
                self.logger.warning("[%s] Failed to send %s to %s: %s", self.node_id, msg_type, target_id, err)

        threading.Thread(target=_worker, daemon=True).start()

    def handle_incoming_channel_message(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Receive message along incoming channel, enforce FIFO sequence order, and apply rules."""
        sender = data.get("sender", "")
        seq = data.get("seq", 0)

        with self._lock:
            expected_seq = self.incoming_channel_seq[sender] + 1
            if seq == expected_seq:
                # In order: process immediately
                self.incoming_channel_seq[sender] = seq
                self._dispatch_message(sender, data)
                # Check buffer for subsequent sequential messages
                buf = self.incoming_channel_buffers[sender]
                next_seq = seq + 1
                while next_seq in buf:
                    buffered_data = buf.pop(next_seq)
                    self.incoming_channel_seq[sender] = next_seq
                    self._dispatch_message(sender, buffered_data)
                    next_seq += 1
            elif seq > expected_seq:
                # Out of order: buffer until predecessor arrives
                self.incoming_channel_buffers[sender][seq] = data
                self.logger.debug("[%s] Buffered out-of-order msg #%d from %s (expected #%d)", self.node_id, seq, sender, expected_seq)
            else:
                # Duplicate or old message
                self.logger.warning("[%s] Ignored duplicate/stale msg #%d from %s", self.node_id, seq, sender)

        return {"status": "ok"}

    def _dispatch_message(self, sender: str, msg: Dict[str, Any]) -> None:
        """Process message according to Chandy-Lamport rules."""
        msg_type = msg.get("msg_type")
        if msg_type == "MARKER":
            self._handle_marker(sender, msg.get("marker_id", "SNAP-DEFAULT"))
        elif msg_type == "APP":
            self._handle_app(sender, int(msg.get("payload", 0)))

    def _handle_marker(self, sender: str, marker_id: str) -> None:
        """Process received MARKER token."""
        self.markers_received_from.add(sender)

        if not self.has_recorded_state:
            # Case 1: First time this node sees this marker!
            self.active_marker_id = marker_id
            # 1. Record local state
            self.recorded_state = self.balance
            self.has_recorded_state = True
            # 2. Record channel from sender as EMPTY
            self.recorded_channel_messages[sender] = []
            # 3. Start recording on all other incoming channels
            self.recording_channels = set(self.peers.keys()) - {sender}
            for other_sender in self.recording_channels:
                self.recorded_channel_messages[other_sender] = []

            msg = f"FIRST MARKER from {sender}: Recorded local balance={self.recorded_state}, Channel({sender}->{self.node_id}) recorded as EMPTY"
            self.event_history.append(msg)
            self.logger.info("[%s] %s", self.node_id, msg)

            # 4. Forward MARKER along all outgoing channels
            self._broadcast_marker(marker_id)
        else:
            # Case 2: Node has already recorded its local state!
            # Stop recording incoming messages on this channel
            if sender in self.recording_channels:
                self.recording_channels.remove(sender)
            msg = f"SUBSEQUENT MARKER from {sender}: Finalized Channel({sender}->{self.node_id}) in-flight: {self.recorded_channel_messages.get(sender, [])}"
            self.event_history.append(msg)
            self.logger.info("[%s] %s", self.node_id, msg)

    def _handle_app(self, sender: str, amount: int) -> None:
        """Process application message and record in-flight token if channel is recording."""
        if sender in self.recording_channels:
            self.recorded_channel_messages[sender].append(amount)
            msg = f"RECORDED IN-FLIGHT MSG APP({amount}) on Channel({sender}->{self.node_id})"
            self.event_history.append(msg)
            self.logger.info("[%s] %s", self.node_id, msg)

        self.balance += amount
        msg = f"PROCESSED APP({amount}) from {sender} (New balance: {self.balance})"
        self.event_history.append(msg)
        self.logger.debug("[%s] %s", self.node_id, msg)

    def is_snapshot_complete(self) -> bool:
        """Completed when local state recorded and markers received on all incoming channels."""
        with self._lock:
            if not self.has_recorded_state:
                return False
            return self.markers_received_from == set(self.peers.keys())

    def get_status(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "node_id": self.node_id,
                "port": self.port,
                "current_balance": self.balance,
                "recorded_local_state": self.recorded_state,
                "has_recorded_state": self.has_recorded_state,
                "is_complete": self.is_snapshot_complete(),
                "active_marker_id": self.active_marker_id,
                "recorded_channel_messages": {k: list(v) for k, v in self.recorded_channel_messages.items()},
                "recording_channels": list(self.recording_channels),
                "markers_received_from": list(self.markers_received_from),
            }


class SnapshotHTTPHandler(BaseHTTPRequestHandler):
    """HTTP request handler for Chandy-Lamport snapshot nodes."""

    node: Optional[NetworkedSnapshotNode] = None

    def log_message(self, format: str, *args: Any) -> None:
        pass

    def _send_json(self, status_code: int, data: Dict[str, Any]) -> None:
        body = json.dumps(data).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path == "/health":
            self._send_json(200, {"status": "ok", "node_id": self.node.node_id})
        elif self.path == "/snapshot/status":
            self._send_json(200, self.node.get_status())
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
            self._send_json(200, {"status": "ok", "node_id": self.node.node_id})
        elif self.path == "/channel/message":
            resp = self.node.handle_incoming_channel_message(data)
            self._send_json(200, resp)
        elif self.path == "/transfer":
            target = data.get("target", "")
            amount = int(data.get("amount", 0))
            ok = self.node.send_transfer(target, amount)
            self._send_json(200, {"success": ok, "balance": self.node.balance})
        elif self.path == "/snapshot/initiate":
            marker_id = data.get("marker_id", "SNAP-1")
            self.node.initiate_snapshot(marker_id)
            self._send_json(200, {"status": "initiated", "marker_id": marker_id})
        else:
            self._send_json(404, {"error": "Unknown endpoint"})


def start_snapshot_node_server(node: NetworkedSnapshotNode) -> HTTPServer:
    """Start HTTPServer for snapshot node in a background thread."""
    handler_class = type("BoundSnapshotHandler", (SnapshotHTTPHandler,), {"node": node})
    server = HTTPServer(("127.0.0.1", node.port), handler_class)
    node.server = server
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    return server


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a standalone Networked Chandy-Lamport Snapshot Node.")
    parser.add_argument("--node-id", required=True, help="Unique node identifier (e.g. P1)")
    parser.add_argument("--port", type=int, required=True, help="Port to listen on (e.g. 9411)")
    parser.add_argument("--peers", required=True, help="JSON map of peer_id to port, e.g. '{\"P2\": 9412}'")
    parser.add_argument("--initial-balance", type=int, default=100, help="Initial token balance")
    parser.add_argument("--delay", type=float, default=0.0, help="Simulated message delay in seconds")

    args = parser.parse_args()
    peer_dict = json.loads(args.peers)

    node = NetworkedSnapshotNode(
        node_id=args.node_id,
        port=args.port,
        peers=peer_dict,
        initial_balance=args.initial_balance,
        simulated_delay_seconds=args.delay,
    )
    server = start_snapshot_node_server(node)
    node.logger.info("Snapshot Node %s running on port %d with initial balance %d", args.node_id, args.port, args.initial_balance)

    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        node.logger.info("Stopping snapshot node %s...", args.node_id)
        server.shutdown()


if __name__ == "__main__":
    main()
