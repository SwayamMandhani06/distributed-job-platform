"""
Real Network Transport Layer for Periodic Beacon Protocol and Failure Detection (Unit III).

Theoretical Background (Failure Detectors in Asynchronous Distributed Systems):
--------------------------------------------------------------------------------
In an asynchronous distributed system (Fischer, Lynch, Paterson, 1985), consensus
and mutual exclusion cannot tolerate unannounced process crashes without a failure detector.
Chandra & Toueg (1996) formalized failure detectors by their completeness and accuracy:
  - Strongly Complete: Every crashed process is eventually suspected.
  - Weakly Accurate: Some live process is never falsely suspected.

The Beacon Protocol implements a heartbeating failure detector:
  - Emitter processes emit periodic, monotonic UDP or HTTP beacon packets.
  - Monitors track arrival times and classify nodes:
      * missed < k_suspect:   HEALTHY
      * k_suspect <= missed < k_dead: SUSPECTED
      * missed >= k_dead:     DEAD
  - Recovery: If beacons resume from a SUSPECTED or DEAD node, the monitor refutes
    the suspicion and transitions the node back to HEALTHY.

Limits & False Positives:
-------------------------
In an asynchronous network with unpredictable latency, thread scheduling delays,
or garbage collection pauses, timeout-based failure detectors CANNOT distinguish
a crashed process from a slow or network-delayed process. This inevitably produces
FALSE POSITIVES.
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
from typing import Any, Dict, List, Optional, Tuple
from urllib import request as url_request
from urllib.error import URLError

from common.beacon_protocol import BeaconMonitor, BeaconPacket, NodeLivenessStatus

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


class NetworkedBeaconMonitorNode:
    """HTTP server node that receives periodic beacons and evaluates cluster node liveness."""

    def __init__(
        self,
        port: int,
        beacon_interval_seconds: float = 0.5,
        missed_beacon_threshold: int = 3,
        dead_threshold_missed: int = 6,
    ) -> None:
        self.port = int(port)
        self.monitor = BeaconMonitor(
            beacon_interval_seconds=beacon_interval_seconds,
            missed_beacon_threshold=missed_beacon_threshold,
            dead_threshold_missed=dead_threshold_missed,
        )
        self._lock = threading.RLock()
        self.server: Optional[HTTPServer] = None
        self.logger = logging.getLogger(f"BeaconMonitor-{port}")

    def record_incoming_beacon(self, data: Dict[str, Any]) -> NodeLivenessStatus:
        with self._lock:
            pkt = BeaconPacket(
                node_id=str(data.get("node_id", "")),
                sequence_number=int(data.get("sequence_number", 0)),
                timestamp_sent=float(data.get("timestamp_sent", time.time())),
                status=str(data.get("status", "UP")),
                metadata=data.get("metadata", {}),
            )
            arrival = time.time()
            status = self.monitor.record_beacon(pkt, arrival_time=arrival)
            return status

    def evaluate_liveness(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            now = time.time()
            results = self.monitor.evaluate_liveness(current_time=now)
            out = {}
            for nid, s in results.items():
                out[nid] = {
                    "node_id": s.node_id,
                    "state": s.state,
                    "last_seen_timestamp": s.last_seen_timestamp,
                    "last_sequence_number": s.last_sequence_number,
                    "missed_beacons": s.missed_beacons,
                    "elapsed_seconds": round(s.elapsed_seconds, 3),
                    "state_changed": s.state_changed,
                    "previous_state": s.previous_state,
                }
            return out


class BeaconMonitorHTTPHandler(BaseHTTPRequestHandler):
    """HTTP handler for Beacon Monitor."""

    node: Optional[NetworkedBeaconMonitorNode] = None

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
            self._send_json(200, {"status": "ok"})
        elif self.path == "/liveness":
            self._send_json(200, self.node.evaluate_liveness())
        elif self.path == "/events":
            with self.node._lock:
                self._send_json(200, {"events": list(self.node.monitor.event_log)})
        else:
            self._send_json(404, {"error": "Not found"})

    def do_POST(self) -> None:
        content_length = int(self.headers.get("Content-Length", 0))
        raw_body = self.rfile.read(content_length).decode("utf-8") if content_length > 0 else "{}"
        try:
            data = json.loads(raw_body)
        except Exception:
            data = {}

        if self.path == "/beacon":
            status = self.node.record_incoming_beacon(data)
            self._send_json(200, {
                "status": "recorded",
                "node_id": status.node_id,
                "current_state": status.state,
                "state_changed": status.state_changed,
            })
        elif self.path == "/health":
            self._send_json(200, {"status": "ok"})
        else:
            self._send_json(404, {"error": "Unknown endpoint"})


def start_beacon_monitor_server(node: NetworkedBeaconMonitorNode) -> HTTPServer:
    """Start Beacon Monitor HTTPServer in a background thread."""
    handler_class = type("BoundBeaconHandler", (BeaconMonitorHTTPHandler,), {"node": node})
    server = HTTPServer(("127.0.0.1", node.port), handler_class)
    node.server = server
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    return server


class NetworkedBeaconEmitterProcess:
    """Independent emitter process emitting periodic beacons to a monitor URL over HTTP."""

    def __init__(
        self,
        node_id: str,
        monitor_url: str,
        interval_seconds: float = 0.5,
    ) -> None:
        self.node_id = str(node_id)
        self.monitor_url = str(monitor_url)
        self.interval_seconds = float(interval_seconds)
        self.sequence_number = 0
        self.is_running = True
        self.pause_event = threading.Event()
        self.logger = logging.getLogger(f"BeaconEmitter-{node_id}")

    def run_loop(self) -> None:
        self.logger.info("Beacon Emitter %s starting periodic emissions (interval=%.2fs)...", self.node_id, self.interval_seconds)
        while self.is_running:
            if not self.pause_event.is_set():
                self.sequence_number += 1
                payload = {
                    "node_id": self.node_id,
                    "sequence_number": self.sequence_number,
                    "timestamp_sent": time.time(),
                    "status": "UP",
                    "metadata": {"pid": os.getpid()},
                }
                success, resp, err = send_http_post(f"{self.monitor_url}/beacon", payload, timeout_seconds=1.0)
                if not success:
                    self.logger.warning("Failed to send beacon #%d: %s", self.sequence_number, err)
            time.sleep(self.interval_seconds)


def main() -> None:
    parser = argparse.ArgumentParser(description="Beacon Protocol Network Component.")
    parser.add_argument("--mode", choices=["monitor", "emitter"], required=True, help="Component mode")
    parser.add_argument("--port", type=int, default=9511, help="Monitor listening port")
    parser.add_argument("--node-id", default="Emitter-1", help="Emitter unique identifier")
    parser.add_argument("--monitor-url", default="http://127.0.0.1:9511", help="Target monitor URL")
    parser.add_argument("--interval", type=float, default=0.5, help="Beacon emission interval in seconds")

    args = parser.parse_args()

    if args.mode == "monitor":
        monitor_node = NetworkedBeaconMonitorNode(
            port=args.port,
            beacon_interval_seconds=args.interval,
            missed_beacon_threshold=3,
            dead_threshold_missed=6,
        )
        server = start_beacon_monitor_server(monitor_node)
        monitor_node.logger.info("Beacon Monitor running on port %d...", args.port)
        try:
            while True:
                time.sleep(1.0)
        except KeyboardInterrupt:
            server.shutdown()
    elif args.mode == "emitter":
        emitter = NetworkedBeaconEmitterProcess(
            node_id=args.node_id,
            monitor_url=args.monitor_url,
            interval_seconds=args.interval,
        )
        try:
            emitter.run_loop()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
