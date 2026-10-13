"""
Deterministic Demonstration: Distributed Object-Based System (Unit IV — Emerging Paradigms).

This script demonstrates:
1. Architectural roles: Client, Proxy/Stub, Communication Layer, Server Skeleton, Servant.
2. Transparent remote method invocation with JSON serialization and parameter marshalling.
3. Remote exception propagation across address space boundaries.
4. Network timeout simulation and error handling.
5. Contrast between local method calls and remote procedure invocations.
"""

import sys
from common.distributed_object import (
    JobManagementServant,
    RemoteClientProxy,
    RemoteCommunicationChannel,
    RemoteException,
    RemoteServerSkeleton,
    RemoteTimeoutError,
)


def print_banner(text: str) -> None:
    print("\n" + "=" * 78)
    print(f" {text}")
    print("=" * 78)


def print_step(step_num: int, title: str) -> None:
    print(f"\n--- [STEP {step_num}] {title} ---")


def run_remote_object_demonstration() -> int:
    print_banner("UNIT IV: DISTRIBUTED OBJECT & REMOTE PROXY ARCHITECTURE")
    print("Paradigm:    Distributed Object-Based System (RMI / CORBA / RPC Proxy Pattern)")
    print("Roles:       Client -> Client Proxy (Stub) -> Transport -> Skeleton -> Servant")
    print("Syllabus:    Unit IV -- Emerging Paradigms (Distributed Object-Based Systems)")

    # 1. Setup Architecture
    print_step(1, "Setting Up Remote Object Architecture")
    # Server side
    skeleton = RemoteServerSkeleton()
    servant = JobManagementServant()
    skeleton.register_service("JobService", servant)
    print("  [Server] Instantiated JobManagementServant (Server Object / Servant).")
    print("  [Server] Registered 'JobService' with RemoteServerSkeleton (Dispatcher).")

    # Communication layer
    channel = RemoteCommunicationChannel(simulated_latency_seconds=0.01, default_timeout_seconds=2.0)
    channel.bind_server(skeleton)
    print("  [Network] Initialized RemoteCommunicationChannel (simulated transport, latency=10ms).")

    # Client side
    client_proxy = RemoteClientProxy("JobService", channel=channel)
    print("  [Client] Instantiated RemoteClientProxy for 'JobService' (Client Stub).")

    # 2. Transparent Remote Method Invocation
    print_step(2, "Transparent Method Invocation: submit_job()")
    print("  [Client] Calling client_proxy.submit_job('image_thumbnail', {'file': 'photo.jpg'})...")
    res = client_proxy.submit_job("image_thumbnail", {"file": "photo.jpg", "resolution": "300x300"})
    print("  [Client] Method returned seamlessly!")
    print(f"  * Result: {res}")
    job_id = res["job_id"]

    # Inspect packet on the wire
    last_tx = channel.transmitted_packets[-2]
    print(f"  * Network Packet on the wire (Client -> Server): {last_tx['bytes']} bytes")
    print(f"  * Invocation ID: {last_tx['invocation_id']}")

    # 3. Reading Remote Object State
    print_step(3, "Retrieving Remote Object State: get_job_status()")
    status = client_proxy.get_job_status(job_id)
    print(f"  * Result from remote servant: {status}")

    # 4. Complex Parameters
    print_step(4, "Complex Data Marshalling: calculate_workload_metrics()")
    weights = [0.85, 0.42, 0.93, 0.60]
    avg_load = client_proxy.calculate_workload_metrics(weights)
    print(f"  [Client] Average cluster workload calculated remotely: {avg_load:.4f}")

    # 5. Remote Exception Propagation
    print_step(5, "Remote Exception Propagation")
    print("  [Client] Querying nonexistent job 'missing-999' on remote servant...")
    try:
        client_proxy.get_job_status("missing-999")
        print("  [ERROR] Should have raised RemoteException!")
        return 1
    except RemoteException as rex:
        print(f"  [OK] Client caught RemoteException propagated from server object:")
        print(f"       -> {rex}")

    # 6. Network Timeout Simulation
    print_step(6, "Network Timeout & Partial Failure Handling")
    slow_channel = RemoteCommunicationChannel(simulated_latency_seconds=0.5)  # 500ms latency
    slow_channel.bind_server(skeleton)
    strict_proxy = RemoteClientProxy("JobService", channel=slow_channel, timeout_seconds=0.05)  # 50ms timeout

    print("  [Client] Invoking method with 50ms client timeout over 500ms latency channel...")
    try:
        strict_proxy.submit_job("email_task", {"to": "admin@example.com"})
        print("  [ERROR] Should have timed out!")
        return 1
    except RemoteTimeoutError as te:
        print(f"  [OK] Client proxy aborted call and raised RemoteTimeoutError:")
        print(f"       -> {te}")

    # 7. Theoretical Comparison
    print_step(7, "Local Method Call vs. Distributed Object Invocation")
    print("1. Local Method Call:")
    print("   - Single address space; direct instruction pointer jump.")
    print("   - Pass-by-reference semantics (shared pointers on heap).")
    print("   - Fails only if process crashes.")
    print("2. Distributed Object Invocation (Demonstrated):")
    print("   - Across network / address space boundaries.")
    print("   - Pass-by-value semantics (JSON serialization / marshalling).")
    print("   - Partial failures: timeouts, network drops, deserialization errors.")

    print_banner("DISTRIBUTED OBJECT DEMONSTRATION COMPLETE")
    return 0


if __name__ == "__main__":
    sys.exit(run_remote_object_demonstration())
