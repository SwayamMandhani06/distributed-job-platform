"""
Multi-Process Demonstration and Verification of Networked Remote Object RPC System (Unit IV).

Runs server and client in independent operating system processes communicating over real TCP sockets:
  - RPC Server (Port 9640) hosting JobManagementServant.
  - Client Process invoking methods via RemoteClientProxy over Networked Transport.

Demonstration Steps:
--------------------
1. Spawn standalone RPC Server process on port 9640.
2. Client connects and executes valid remote procedure calls:
     - submit_job(job_type, payload)
     - get_job_status(job_id)
     - calculate_workload_metrics([0.8, 0.4, 0.9])
3. Security verification:
     - Invoke unregistered method -> raises RemoteException (MethodNotAllowedError).
     - Invoke private method (_internal_helper) -> raises RemoteException (MethodNotAllowedError).
4. Error mapping:
     - Server-side exception (nonexistent job) bubbles up as typed RemoteException.
     - Malformed HTTP request body returns HTTP 400 MalformedRequestError.
5. Deadline/Timeout enforcement:
     - Client configured with 10ms deadline times out when server has latency.
6. Server termination & restart recovery:
     - Server process is terminated -> client detects RemoteCommunicationError.
     - Server process restarts on port 9640 -> client reconnects and executes successfully.
"""

import json
import subprocess
import sys
import time
from pathlib import Path
from urllib import request as url_request

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from common.distributed_object import (
    RemoteClientProxy,
    RemoteCommunicationChannel,
    RemoteCommunicationError,
    RemoteException,
    RemoteTimeoutError,
)

PYTHON_EXE = sys.executable
SERVER_PORT = 9640
SERVER_URL = f"http://127.0.0.1:{SERVER_PORT}"


def wait_for_server(url: str, max_retries: int = 25) -> bool:
    for _ in range(max_retries):
        try:
            req = url_request.Request(f"{url}/health", method="GET")
            with url_request.urlopen(req, timeout=0.5) as resp:
                if resp.status == 200:
                    return True
        except Exception:
            time.sleep(0.2)
    return False


def run_demo():
    print("=" * 75)
    print("DISTRIBUTED OBJECT RPC SYSTEM — MULTI-PROCESS NETWORK DEMO")
    print("=" * 75)

    server_proc = None

    try:
        # Step 1: Launch RPC server in independent process
        print("\n[Step 1] Spawning Networked RPC Server process on port 9640...")
        cmd = [
            PYTHON_EXE,
            "-m",
            "common.distributed_object",
            "--port", str(SERVER_PORT),
        ]
        server_proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print(f"  -> Server process running (PID: {server_proc.pid})")

        if not wait_for_server(SERVER_URL):
            raise RuntimeError(f"Server failed to start at {SERVER_URL}")
        print(f"  -> Server is online and responding at {SERVER_URL}/health")

        # Step 2: Initialize Client Proxy over Real Network Transport
        print("\n[Step 2] Initializing Client Proxy with real HTTP/TCP socket transport...")
        channel = RemoteCommunicationChannel(server_url=SERVER_URL, default_timeout_seconds=2.0)
        proxy = RemoteClientProxy("JobService", channel=channel)

        # Step 3: Execute valid remote procedure calls
        print("\n[Step 3] Executing valid remote method calls across OS process boundary:")
        job_res = proxy.submit_job(
            job_type="math_prime_sieve",
            payload={"limit": 1000},
        )
        print(f"  -> proxy.submit_job() returned: {job_res}")
        job_id = job_res["job_id"]

        status_res = proxy.get_job_status(job_id)
        print(f"  -> proxy.get_job_status('{job_id}') returned: {status_res['status']} (type: {status_res['job_type']})")

        metrics_res = proxy.calculate_workload_metrics([0.75, 0.85, 0.95])
        print(f"  -> proxy.calculate_workload_metrics([0.75, 0.85, 0.95]) returned: {metrics_res:.4f}")

        # Step 4: Security and allowlist verification
        print("\n[Step 4] Enforcing security allowlists and private/dunder rejection:")
        try:
            proxy.unregistered_dangerous_action()
            print("  [FAIL] Expected MethodNotAllowedError for unregistered method")
        except RemoteException as exc:
            print(f"  [PASS] Unregistered method blocked: {exc}")

        try:
            proxy._internal_helper()
            print("  [FAIL] Expected MethodNotAllowedError for private method")
        except RemoteException as exc:
            print(f"  [PASS] Private method blocked: {exc}")

        # Step 5: Remote exception propagation
        print("\n[Step 5] Propagating server-side application exceptions:")
        try:
            proxy.get_job_status("nonexistent-job-uuid-999")
            print("  [FAIL] Expected KeyError exception for missing job")
        except RemoteException as exc:
            print(f"  [PASS] Server-side exception caught by client: {exc}")

        # Step 6: Malformed request handling
        print("\n[Step 6] Testing malformed HTTP request handling:")
        try:
            bad_req = url_request.Request(
                f"{SERVER_URL}/rpc",
                data=b"INVALID_JSON_CORRUPTED",
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with url_request.urlopen(bad_req, timeout=1.0) as resp:
                print("  [FAIL] Server accepted malformed request")
        except url_request.HTTPError as err:
            err_data = json.loads(err.read().decode("utf-8"))
            print(f"  [PASS] Server rejected malformed payload with HTTP {err.code}: {err_data.get('error_type')} - {err_data.get('error_message')}")

        # Step 7: Timeout / Deadline enforcement
        print("\n[Step 7] Enforcing client-side invocation deadline/timeout:")
        strict_channel = RemoteCommunicationChannel(server_url=SERVER_URL, default_timeout_seconds=0.1)
        strict_proxy = RemoteClientProxy("JobService", channel=strict_channel)
        try:
            strict_proxy.simulate_slow_task(sleep_seconds=0.4)
            print("  [FAIL] Expected timeout exception")
        except RemoteTimeoutError as exc:
            print(f"  [PASS] Invocation deadline strictly enforced: {exc}")

        # Step 8: Server termination & Connection handling
        print("\n[Step 8] Testing server termination and connection failure handling:")
        print("  -> Terminating RPC server process...")
        server_proc.terminate()
        server_proc.wait(timeout=2.0)
        print("  -> Server is DOWN.")

        try:
            proxy.submit_job("offline_test", {})
            print("  [FAIL] Expected communication error when server is down")
        except RemoteCommunicationError as exc:
            print(f"  [PASS] Client caught transport disconnection: {exc}")

        # Step 9: Server restart & Reconnection
        print("\n[Step 9] Restarting RPC server process on same port (reconnection test)...")
        server_proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if not wait_for_server(SERVER_URL):
            raise RuntimeError("Server failed to restart.")
        print("  -> Server is back ONLINE.")

        recovered_res = proxy.submit_job("recovered_job", {"priority": "high"})
        print(f"  [PASS] Reconnected successfully! New job submitted: {recovered_res['job_id']}")

        print("\n" + "=" * 75)
        print("DISTRIBUTED OBJECT RPC DEMO COMPLETED SUCCESSFULLY")
        print("=" * 75)

    finally:
        if server_proc and server_proc.poll() is None:
            server_proc.terminate()
            try:
                server_proc.wait(timeout=1.0)
            except Exception:
                server_proc.kill()


if __name__ == "__main__":
    run_demo()
