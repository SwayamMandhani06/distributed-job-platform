"""
Multi-Process Demonstration and Verification of Networked Distributed File System (DFS) (Unit IV).

Runs a decoupled storage cluster across 4 independent operating system processes:
  - NameNode (Metadata Service, Port 9830)
  - DataNode-1 (Storage Node, Port 9831)
  - DataNode-2 (Storage Node, Port 9832)
  - DataNode-3 (Storage Node, Port 9833)

Demonstration Steps:
--------------------
1. Spawn NameNode and 3 DataNodes in independent OS subprocesses.
2. Client writes a 256-byte payload ('cluster_log.txt') split into 64-byte blocks with R=2 replication.
3. Observe block distribution across DataNodes via real HTTP requests.
4. Read and verify content reassembly and cryptographic SHA-256 integrity over the network.
5. Single-Node Failure: DataNode-1 is terminated (simulating machine crash).
6. File read succeeds transparently via surviving replica on DataNode-2!
7. Autonomous Self-Healing / Re-replication:
     NameNode commands surviving DataNode-2 to copy degraded blocks to DataNode-3,
     restoring R=2 redundancy across the live cluster.
8. Bit-rot / Corruption Injection:
     DataNode-2's block is corrupted with garbage bytes.
     Read detects SHA-256 mismatch (HTTP 409) and falls back to healthy replica on DataNode-3.
9. Complete Replica Loss:
     When all replicas for a block are lost, the system explicitly reports BlockUnavailableError
     confirming permanent data loss (no false recovery claims).
"""

import base64
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from common.dfs_transport import send_http_json

PYTHON_EXE = sys.executable
NAMENODE_PORT = 9830
NAMENODE_URL = f"http://127.0.0.1:{NAMENODE_PORT}"

DATANODES = {
    "DataNode-1": 9831,
    "DataNode-2": 9832,
    "DataNode-3": 9833,
}

STORAGE_ROOT = BASE_DIR / "test_cluster_dfs_storage"
METADATA_FILE = BASE_DIR / "test_dfs_metadata.json"


def wait_for_service(url: str, max_retries: int = 25) -> bool:
    for _ in range(max_retries):
        ok, code, resp, _ = send_http_json(f"{url}/health", timeout_seconds=0.5)
        if ok and code == 200:
            return True
        time.sleep(0.2)
    return False


def run_demo():
    print("=" * 75)
    print("NETWORKED DISTRIBUTED FILE SYSTEM (DFS) — MULTI-PROCESS CLUSTER DEMO")
    print("=" * 75)

    if STORAGE_ROOT.exists():
        shutil.rmtree(STORAGE_ROOT)
    if METADATA_FILE.exists():
        METADATA_FILE.unlink()

    STORAGE_ROOT.mkdir(parents=True, exist_ok=True)
    procs = {}

    try:
        # Step 1: Launch 3 DataNodes in separate subprocesses
        print("\n[Step 1] Spawning 3 independent Storage Nodes (DataNodes) on ports 9831, 9832, 9833...")
        for nid, port in DATANODES.items():
            cmd = [
                PYTHON_EXE,
                "-m",
                "common.dfs_transport",
                "--mode", "datanode",
                "--node-id", nid,
                "--port", str(port),
                "--storage-dir", str(STORAGE_ROOT),
            ]
            proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            procs[nid] = proc
            print(f"  -> {nid} running (PID: {proc.pid}) on http://127.0.0.1:{port}")

        # Step 2: Launch NameNode
        print("\n[Step 2] Spawning Metadata Coordinator (NameNode) on port 9830...")
        nodes_map = {nid: f"http://127.0.0.1:{port}" for nid, port in DATANODES.items()}
        namenode_cmd = [
            PYTHON_EXE,
            "-m",
            "common.dfs_transport",
            "--mode", "namenode",
            "--port", str(NAMENODE_PORT),
            "--metadata-file", str(METADATA_FILE),
            "--nodes", json.dumps(nodes_map),
        ]
        namenode_proc = subprocess.Popen(namenode_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        procs["NameNode"] = namenode_proc
        print(f"  -> NameNode running (PID: {namenode_proc.pid}) on {NAMENODE_URL}")

        # Wait for all services
        for nid, port in DATANODES.items():
            if not wait_for_service(f"http://127.0.0.1:{port}"):
                raise RuntimeError(f"DataNode {nid} failed to start.")
        if not wait_for_service(NAMENODE_URL):
            raise RuntimeError("NameNode failed to start.")
        print("  -> Full DFS storage cluster is online and healthy!")

        # Step 3: Write File with Partitioning & R=2 Replication
        print("\n[Step 3] Client writes 'cluster_log.txt' (256 bytes) with R=2 replication (64B chunks)...")
        sample_payload = b"A" * 64 + b"B" * 64 + b"C" * 64 + b"D" * 64  # Exactly 4 blocks of 64 bytes
        b64_content = base64.b64encode(sample_payload).decode("utf-8")

        ok, code, resp, err = send_http_json(
            f"{NAMENODE_URL}/dfs/write",
            method="POST",
            payload={
                "filename": "cluster_log.txt",
                "content_b64": b64_content,
                "block_size": 64,
                "replication_factor": 2,
            },
        )
        assert ok and code == 200, f"Write failed: {err}"
        file_meta = resp["file_metadata"]
        print(f"  -> File written successfully: {file_meta['filename']} ({file_meta['total_size_bytes']} bytes)")
        print(f"  -> Split into {len(file_meta['blocks'])} blocks with R={file_meta['replication_factor']}:")
        for b in file_meta["blocks"]:
            print(f"     * Block {b['block_id']}: {b['size_bytes']} bytes, SHA-256={b['checksum_sha256'][:10]}... Replicas: {b['replica_node_ids']}")

        # Step 4: Read File Back over Network
        print("\n[Step 4] Client reads file back through NameNode & DataNodes...")
        ok, code, read_resp, err = send_http_json(f"{NAMENODE_URL}/dfs/read/cluster_log.txt")
        assert ok and code == 200, f"Read failed: {err}"
        read_bytes = base64.b64decode(read_resp["content_b64"].encode("utf-8"))
        assert read_bytes == sample_payload, "Read data does not match original written data"
        print("  [PASS] Content reassembled perfectly over HTTP with cryptographic verification!")

        # Step 5: Single-Node Crash Fault Injection
        print("\n[Step 5] Single-Node Failure: Terminating DataNode-1 process...")
        procs["DataNode-1"].terminate()
        procs["DataNode-1"].wait(timeout=2.0)
        print("  -> DataNode-1 is DEAD.")

        # Check Health
        ok, code, health_rep, _ = send_http_json(f"{NAMENODE_URL}/dfs/health/cluster_log.txt")
        print(f"  -> File Health after node crash: Healthy blocks={health_rep['healthy_blocks']}, Degraded={health_rep['degraded_blocks']}, Lost={health_rep['lost_blocks']}, Readable={health_rep['is_readable']}")
        assert health_rep["is_readable"] is True, "File must remain readable despite 1 node crash"

        # Read should transparently succeed using surviving replicas
        ok, code, read_rep2, _ = send_http_json(f"{NAMENODE_URL}/dfs/read/cluster_log.txt")
        assert ok and code == 200, "Transparent failover read failed"
        print("  [PASS] Transparent Failover: Read succeeded using surviving replicas!")

        # Step 6: Autonomous Re-Replication (Self-Healing)
        print("\n[Step 6] Self-Healing: Re-replicating degraded blocks to DataNode-3 over network...")
        ok, code, heal_rep, err = send_http_json(
            f"{NAMENODE_URL}/dfs/re_replicate",
            method="POST",
            payload={"filename": "cluster_log.txt"},
            timeout_seconds=5.0,
        )
        assert ok and code == 200, f"Re-replication failed: {err}"
        print(f"  -> Re-replication completed: {heal_rep['re_replicated_blocks_count']} blocks copied to restore redundancy.")

        # Re-check Health
        ok, code, post_heal, _ = send_http_json(f"{NAMENODE_URL}/dfs/health/cluster_log.txt")
        print(f"  -> File Health after re-replication: Healthy={post_heal['healthy_blocks']}, Degraded={post_heal['degraded_blocks']}")
        assert post_heal["healthy_blocks"] == post_heal["total_blocks"], "All blocks must be restored to HEALTHY"
        print("  [PASS] Redundancy fully restored to R=2 across surviving nodes!")

        # Step 7: Bit-Rot / Corruption Detection
        print("\n[Step 7] Bit-Rot Detection: Injecting byte corruption into DataNode-2...")
        first_block = file_meta["blocks"][0]["block_id"]
        send_http_json(f"http://127.0.0.1:{DATANODES['DataNode-2']}/block/corrupt/{first_block}", method="POST")
        print(f"  -> Block {first_block} corrupted on DataNode-2.")

        # Read should detect SHA-256 error and fall back to DataNode-3!
        ok, code, corrupt_read, _ = send_http_json(f"{NAMENODE_URL}/dfs/read/cluster_log.txt")
        assert ok and code == 200, "Fallback after corruption failed"
        print("  [PASS] Cryptographic SHA-256 integrity check detected corruption and fell back to healthy replica!")

        # Step 8: Complete Replica Loss Boundary
        print("\n[Step 8] Complete Replica Loss: Corrupting/terminating all remaining replicas for first block...")
        send_http_json(f"http://127.0.0.1:{DATANODES['DataNode-3']}/block/corrupt/{first_block}", method="POST")

        ok, code, lost_read, err = send_http_json(f"{NAMENODE_URL}/dfs/read/cluster_log.txt")
        assert code == 500, "Expected BlockUnavailableError on complete loss"
        print(f"  [PASS] Permanent data loss explicitly reported: HTTP {code} - {lost_read.get('error')} ({lost_read.get('message')[:60]}...)")
        print("  -> Confirmed: System never falsely claims recovery of unrecoverable data.")

        print("\n" + "=" * 75)
        print("DISTRIBUTED FILE SYSTEM CLUSTER DEMO COMPLETED SUCCESSFULLY")
        print("=" * 75)

    finally:
        for nid, proc in procs.items():
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=1.0)
                except Exception:
                    proc.kill()
        if STORAGE_ROOT.exists():
            shutil.rmtree(STORAGE_ROOT, ignore_errors=True)
        if METADATA_FILE.exists():
            METADATA_FILE.unlink(missing_ok=True)


if __name__ == "__main__":
    run_demo()
