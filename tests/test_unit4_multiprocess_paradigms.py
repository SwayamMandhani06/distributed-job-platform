"""
Automated Integration and Multi-Process Verification Suite for Unit IV Distributed Systems.

Covers:
A. Distributed Object-Based System (Networked HTTP/JSON RPC):
   - Multi-process client/server communication
   - Method registration and allowlist enforcement
   - Private and dunder method rejection
   - Remote exception marshaling
   - Deadline / timeout enforcement
   - Malformed request handling (HTTP 400)
   - Server termination and recovery

B. Networked Distributed File System (DFS Cluster):
   - Decoupled cluster with 1 NameNode and 3 independent DataNodes
   - Block splitting, SHA-256 checksums, and R=2 replication over network
   - Single-node crash tolerance and transparent failover
   - Autonomous self-healing re-replication restoring redundancy
   - Cryptographic bit-rot detection and replica fallback
   - Explicit handling of complete replica loss (no false recovery claims)

C. Serverless Computing Runtime Service:
   - Process-isolated function execution with strict execution timeouts
   - Side-effect prevention via genuine OS process termination
   - Input validation schemas and 400 Bad Request rejection
   - Security allowlist and 404 rejection
   - Durable SQLite idempotency deduplication surviving service restarts
"""

import base64
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from urllib import request as url_request
from urllib.error import HTTPError

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from common.distributed_object import (
    NetworkedRemoteServer,
    RemoteClientProxy,
    RemoteCommunicationChannel,
    RemoteCommunicationError,
    RemoteException,
    RemoteTimeoutError,
)
from common.serverless_runtime import (
    PersistentInvocationStore,
    ServerlessRuntime,
    echo_handler,
    failing_handler,
    idempotent_test_handler,
    slow_timeout_handler,
    validated_math_handler,
)

PYTHON_EXE = sys.executable


def find_free_port() -> int:
    """Acquire an available OS TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def send_http_json(
    url: str,
    method: str = "GET",
    payload: Optional[Dict[str, Any]] = None,
    timeout_seconds: float = 3.0,
) -> Tuple[bool, int, Optional[Dict[str, Any]], str]:
    """Execute HTTP JSON request with timeout and error capture."""
    data_bytes = json.dumps(payload).encode("utf-8") if payload is not None else None
    headers = {"Content-Type": "application/json"} if payload is not None else {}
    req = url_request.Request(url, data=data_bytes, headers=headers, method=method)
    try:
        with url_request.urlopen(req, timeout=timeout_seconds) as resp:
            body = resp.read().decode("utf-8")
            parsed = json.loads(body) if body else {}
            return True, resp.status, parsed, ""
    except HTTPError as err:
        try:
            body = err.read().decode("utf-8")
            parsed = json.loads(body) if body else {}
            return False, err.code, parsed, err.reason
        except Exception:
            return False, err.code, None, err.reason
    except Exception as exc:
        return False, 0, None, str(exc)


def wait_for_service(url: str, max_retries: int = 25) -> bool:
    for _ in range(max_retries):
        ok, code, _, _ = send_http_json(f"{url}/health", timeout_seconds=0.3)
        if ok and code == 200:
            return True
        time.sleep(0.15)
    return False


# ===========================================================================
# A. Distributed Object System (RPC) Integration Tests
# ===========================================================================
class TestNetworkedDistributedObjectSystem(unittest.TestCase):
    """Integration test suite for Networked Distributed Object RPC."""

    def setUp(self) -> None:
        self.port = find_free_port()
        self.server_url = f"http://127.0.0.1:{self.port}"
        self.cmd = [
            PYTHON_EXE,
            "-m",
            "common.distributed_object",
            "--port",
            str(self.port),
        ]
        self.proc = subprocess.Popen(self.cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.assertTrue(wait_for_service(self.server_url), "RPC Server failed to start.")
        self.channel = RemoteCommunicationChannel(server_url=self.server_url, timeout_seconds=2.0)
        self.job_proxy = RemoteClientProxy("JobService", channel=self.channel)

    def tearDown(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=1.0)
            except Exception:
                self.proc.kill()

    def test_rpc_successful_invocation(self) -> None:
        """Verify normal multi-process RPC execution returns computed results."""
        res = self.job_proxy.submit_job(job_type="analytics", payload={"rows": 500})
        self.assertEqual(res["status"], "queued")
        self.assertIn("job_id", res)

    def test_rpc_unregistered_method_rejected(self) -> None:
        """Verify calling public method not in allowlist raises MethodNotAllowedError."""
        with self.assertRaises(RemoteException) as ctx:
            self.job_proxy.unregistered_public_method()
        self.assertIn("MethodNotAllowedError", str(ctx.exception))
        self.assertIn("not in the registered allowlist", str(ctx.exception))

    def test_rpc_private_and_dunder_methods_rejected(self) -> None:
        """Verify calling private or dunder methods is rejected."""
        with self.assertRaises(RemoteException) as ctx:
            self.job_proxy._internal_helper()
        self.assertIn("MethodNotAllowedError", str(ctx.exception))

        with self.assertRaises(RemoteException) as ctx2:
            self.job_proxy.__str__()
        self.assertIn("MethodNotAllowedError", str(ctx2.exception))

    def test_rpc_remote_exception_marshaling(self) -> None:
        """Verify server exceptions (e.g., ValueError) propagate to client proxy."""
        with self.assertRaises(RemoteException) as ctx:
            self.job_proxy.submit_job(job_type="", payload={})
        self.assertIn("ValueError", str(ctx.exception))
        self.assertIn("job_type cannot be empty", str(ctx.exception))

    def test_rpc_timeout_enforcement(self) -> None:
        """Verify strict client timeout enforcement raises RemoteTimeoutError."""
        fast_channel = RemoteCommunicationChannel(server_url=self.server_url, timeout_seconds=0.08)
        fast_proxy = RemoteClientProxy("JobService", channel=fast_channel)
        with self.assertRaises(RemoteTimeoutError):
            fast_proxy.simulate_slow_task(sleep_seconds=0.4)

    def test_rpc_malformed_request_returns_bad_request(self) -> None:
        """Verify server rejects malformed payload with HTTP 400 Bad Request."""
        req = url_request.Request(
            f"{self.server_url}/rpc",
            data=b"invalid-non-json-content",
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with url_request.urlopen(req, timeout=2.0) as resp:
                self.fail("Expected HTTP 400 Bad Request")
        except HTTPError as err:
            self.assertEqual(err.code, 400)

    def test_rpc_server_termination_and_restart(self) -> None:
        """Verify client fails when server terminates and recovers when server restarts."""
        # 1. Terminate server
        self.proc.terminate()
        self.proc.wait(timeout=2.0)

        # 2. Call fails due to unreachable server
        with self.assertRaises(RemoteException):
            self.job_proxy.submit_job(job_type="calc", payload={})

        # 3. Restart server on the same port
        self.proc = subprocess.Popen(self.cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.assertTrue(wait_for_service(self.server_url), "Restarted RPC server failed to start.")

        # 4. Call succeeds again!
        res = self.job_proxy.submit_job(job_type="calc", payload={"val": 42})
        self.assertEqual(res["status"], "queued")


# ===========================================================================
# B. Networked Distributed File System (DFS Cluster) Integration Tests
# ===========================================================================
class TestNetworkedDistributedFileSystemCluster(unittest.TestCase):
    """Integration test suite for multi-process Distributed File System cluster."""

    def setUp(self) -> None:
        self.tmp_dir = tempfile.mkdtemp(prefix="test_dfs_cluster_")
        self.storage_root = Path(self.tmp_dir) / "storage"
        self.storage_root.mkdir(parents=True, exist_ok=True)
        self.metadata_file = Path(self.tmp_dir) / "meta.json"

        self.namenode_port = find_free_port()
        self.namenode_url = f"http://127.0.0.1:{self.namenode_port}"

        self.datanode_ports = {
            "DataNode-1": find_free_port(),
            "DataNode-2": find_free_port(),
            "DataNode-3": find_free_port(),
        }
        self.nodes_map = {nid: f"http://127.0.0.1:{p}" for nid, p in self.datanode_ports.items()}

        self.procs = {}
        # Start DataNodes
        for nid, port in self.datanode_ports.items():
            cmd = [
                PYTHON_EXE,
                "-m",
                "common.dfs_transport",
                "--mode", "datanode",
                "--node-id", nid,
                "--port", str(port),
                "--storage-dir", str(self.storage_root),
            ]
            self.procs[nid] = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        # Start NameNode
        namenode_cmd = [
            PYTHON_EXE,
            "-m",
            "common.dfs_transport",
            "--mode", "namenode",
            "--port", str(self.namenode_port),
            "--metadata-file", str(self.metadata_file),
            "--nodes", json.dumps(self.nodes_map),
        ]
        self.procs["NameNode"] = subprocess.Popen(namenode_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        # Wait for all services
        for nid, port in self.datanode_ports.items():
            self.assertTrue(wait_for_service(f"http://127.0.0.1:{port}"), f"{nid} failed to start.")
        self.assertTrue(wait_for_service(self.namenode_url), "NameNode failed to start.")

    def tearDown(self) -> None:
        for nid, proc in self.procs.items():
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=1.0)
                except Exception:
                    proc.kill()
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_dfs_write_and_read_network_cluster(self) -> None:
        """Verify network write splits blocks with R=2 and reassembles correctly."""
        test_data = b"Hello, Distributed File System! " * 8  # 256 bytes
        b64_content = base64.b64encode(test_data).decode("utf-8")

        ok, code, write_resp, err = send_http_json(
            f"{self.namenode_url}/dfs/write",
            method="POST",
            payload={"filename": "test.txt", "content_b64": b64_content, "block_size": 64, "replication_factor": 2},
        )
        self.assertTrue(ok and code == 200, f"Write failed: {err}")
        self.assertEqual(len(write_resp["file_metadata"]["blocks"]), 4)

        # Read back
        ok, code, read_resp, err = send_http_json(f"{self.namenode_url}/dfs/read/test.txt")
        self.assertTrue(ok and code == 200, f"Read failed: {err}")
        reassembled = base64.b64decode(read_resp["content_b64"].encode("utf-8"))
        self.assertEqual(reassembled, test_data)

    def test_dfs_single_node_failure_and_re_replication(self) -> None:
        """Verify single node crash tolerance and autonomous re-replication."""
        test_data = b"Tolerating hardware failures through redundant block replication!" * 4
        b64_content = base64.b64encode(test_data).decode("utf-8")

        send_http_json(
            f"{self.namenode_url}/dfs/write",
            method="POST",
            payload={"filename": "fault_test.txt", "content_b64": b64_content, "block_size": 64, "replication_factor": 2},
        )

        # Terminate DataNode-1
        self.procs["DataNode-1"].terminate()
        self.procs["DataNode-1"].wait(timeout=2.0)

        # 1. File remains readable via surviving replicas
        ok, code, read_resp, _ = send_http_json(f"{self.namenode_url}/dfs/read/fault_test.txt")
        self.assertTrue(ok and code == 200)
        self.assertEqual(base64.b64decode(read_resp["content_b64"].encode("utf-8")), test_data)

        # 2. Health check reports degraded blocks
        ok, code, health, _ = send_http_json(f"{self.namenode_url}/dfs/health/fault_test.txt")
        self.assertTrue(health["is_readable"])
        self.assertGreater(health["degraded_blocks"], 0)

        # 3. Autonomous re-replication restores R=2 to surviving DataNodes
        ok, code, heal_resp, err = send_http_json(
            f"{self.namenode_url}/dfs/re_replicate",
            method="POST",
            payload={"filename": "fault_test.txt"},
            timeout_seconds=5.0,
        )
        self.assertTrue(ok and code == 200, f"Re-replication failed: {err}")
        self.assertGreater(heal_resp["re_replicated_blocks_count"], 0)

        # 4. Health restored
        ok, code, post_health, _ = send_http_json(f"{self.namenode_url}/dfs/health/fault_test.txt")
        self.assertEqual(post_health["healthy_blocks"], post_health["total_blocks"])
        self.assertEqual(post_health["degraded_blocks"], 0)

    def test_dfs_bitrot_corruption_detection(self) -> None:
        """Verify corrupted block replica is detected via SHA-256 and healthy replica is used."""
        test_data = b"Cryptographic hash checksums prevent silent bit-rot corruption!" * 2
        b64_content = base64.b64encode(test_data).decode("utf-8")

        ok, _, write_resp, _ = send_http_json(
            f"{self.namenode_url}/dfs/write",
            method="POST",
            payload={"filename": "corrupt_test.bin", "content_b64": b64_content, "block_size": 64, "replication_factor": 2},
        )
        first_block = write_resp["file_metadata"]["blocks"][0]
        corrupt_target_nid = first_block["replica_node_ids"][0]
        corrupt_port = self.datanode_ports[corrupt_target_nid]

        # Corrupt the block on the target node
        send_http_json(f"http://127.0.0.1:{corrupt_port}/block/corrupt/{first_block['block_id']}", method="POST")

        # Read back succeeds transparently via fallback replica
        ok, code, read_resp, _ = send_http_json(f"{self.namenode_url}/dfs/read/corrupt_test.bin")
        self.assertTrue(ok and code == 200)
        self.assertEqual(base64.b64decode(read_resp["content_b64"].encode("utf-8")), test_data)

    def test_dfs_complete_replica_loss_reports_unrecoverable(self) -> None:
        """Verify complete replica loss explicitly raises BlockUnavailableError with no false claims."""
        test_data = b"Unrecoverable data if all replicas are permanently destroyed."
        b64_content = base64.b64encode(test_data).decode("utf-8")

        send_http_json(
            f"{self.namenode_url}/dfs/write",
            method="POST",
            payload={"filename": "lost.bin", "content_b64": b64_content, "block_size": 64, "replication_factor": 2},
        )

        # Terminate all DataNodes
        for nid in self.datanode_ports:
            self.procs[nid].terminate()
            self.procs[nid].wait(timeout=1.0)

        # Read must fail with HTTP 500 BlockUnavailableError
        ok, code, resp, _ = send_http_json(f"{self.namenode_url}/dfs/read/lost.bin")
        self.assertEqual(code, 500)
        self.assertEqual(resp.get("error"), "BlockUnavailableError")


# ===========================================================================
# C. Serverless Function Runtime Service Integration Tests
# ===========================================================================
class TestServerlessServiceCluster(unittest.TestCase):
    """Integration test suite for standalone Serverless Computing service."""

    def setUp(self) -> None:
        self.tmp_dir = tempfile.mkdtemp(prefix="test_serverless_suite_")
        self.db_path = Path(self.tmp_dir) / "serverless.db"
        self.port = find_free_port()
        self.service_url = f"http://127.0.0.1:{self.port}"
        self.cmd = [
            PYTHON_EXE,
            "-m",
            "common.serverless_runtime",
            "--port", str(self.port),
            "--db", str(self.db_path),
        ]
        self.proc = subprocess.Popen(self.cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.assertTrue(wait_for_service(self.service_url), "Serverless service failed to start.")

    def tearDown(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=1.0)
            except Exception:
                self.proc.kill()
        time.sleep(0.3)
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_serverless_input_validation(self) -> None:
        """Verify required parameter validation rejects malformed payload with HTTP 400."""
        # Missing required parameter "operation"
        ok, code, res, _ = send_http_json(
            f"{self.service_url}/invoke",
            method="POST",
            payload={"function_name": "math", "event": {"x": 10, "y": 20}},
        )
        self.assertEqual(code, 400)
        self.assertIn("Missing required payload key", res.get("error_message", ""))

        # Valid invocation succeeds
        ok, code, res, _ = send_http_json(
            f"{self.service_url}/invoke",
            method="POST",
            payload={"function_name": "math", "event": {"operation": "multiply", "x": 6, "y": 7}},
        )
        self.assertEqual(code, 200)
        self.assertEqual(res["payload"]["result"], 42.0)

    def test_serverless_strict_timeout_and_process_cleanup(self) -> None:
        """Verify function exceeding timeout limit is killed and returns HTTP 504."""
        ok, code, res, _ = send_http_json(
            f"{self.service_url}/invoke",
            method="POST",
            payload={"function_name": "slow", "event": {"sleep_sec": 0.5}, "timeout_sec": 0.08},
        )
        self.assertEqual(code, 504)
        self.assertIn("timed out", res.get("error_message", ""))
        self.assertIn("process terminated", res.get("error_message", ""))

    def test_serverless_unregistered_function_rejected(self) -> None:
        """Verify calling un-allowlisted function returns HTTP 404."""
        ok, code, res, _ = send_http_json(
            f"{self.service_url}/invoke",
            method="POST",
            payload={"function_name": "nonexistent_func", "event": {}},
        )
        self.assertEqual(code, 404)
        self.assertIn("not registered", res.get("error_message", ""))

    def test_serverless_durable_idempotency_survives_restart(self) -> None:
        """Verify duplicate requests replay cached results across service restarts."""
        counter_file = os.path.join(self.tmp_dir, "counter.txt")
        key = "idemp-key-persistent-42"

        # 1. Initial call
        ok, code, res1, _ = send_http_json(
            f"{self.service_url}/invoke",
            method="POST",
            payload={
                "function_name": "idempotent",
                "event": {"counter_file": counter_file, "token": "val-123"},
                "idempotency_key": key,
            },
        )
        self.assertEqual(code, 200)
        self.assertFalse(res1.get("cached_idempotent"))

        # 2. Restart service instance
        self.proc.terminate()
        self.proc.wait(timeout=2.0)
        self.proc = subprocess.Popen(self.cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.assertTrue(wait_for_service(self.service_url), "Restarted serverless service failed to start.")

        # 3. Replay duplicate call with same key
        ok, code, res2, _ = send_http_json(
            f"{self.service_url}/invoke",
            method="POST",
            payload={
                "function_name": "idempotent",
                "event": {"counter_file": counter_file, "token": "val-123"},
                "idempotency_key": key,
            },
        )
        self.assertEqual(code, 200)
        self.assertTrue(res2.get("cached_idempotent"))
        self.assertEqual(res2["payload"]["token"], "val-123")

        # 4. Handler executed only once
        with open(counter_file, "r", encoding="utf-8") as f:
            lines = f.readlines()
        self.assertEqual(len(lines), 1)


if __name__ == "__main__":
    unittest.main()
