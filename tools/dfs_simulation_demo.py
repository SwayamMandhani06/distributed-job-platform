"""
Deterministic Demonstration: Distributed File System (DFS) Architecture (Unit IV — Emerging Paradigms).

This script demonstrates:
1. Separation of Concerns: Metadata Coordinator (NameNode) vs. Content Storage Nodes (DataNodes).
2. File Partitioning & Block Distribution: Slicing files into fixed chunks with SHA-256 checksums.
3. Replication Strategy: Multi-node replication factor (R=2).
4. Fault Tolerance & Transparent Failover: Surviving a complete DataNode failure using surviving replicas.
5. Autonomous Self-Healing: Re-replicating degraded blocks to healthy nodes.
6. Comparison with Apache HDFS and Amazon S3.
"""

import sys
from common.distributed_filesystem import DistributedFileSystem, BlockUnavailableError


def print_banner(text: str) -> None:
    print("\n" + "=" * 78)
    print(f" {text}")
    print("=" * 78)


def print_step(step_num: int, title: str) -> None:
    print(f"\n--- [STEP {step_num}] {title} ---")


def run_dfs_demonstration() -> int:
    print_banner("UNIT IV: DISTRIBUTED FILE SYSTEM (DFS) ARCHITECTURE DEMONSTRATION")
    print("Model:       HDFS / GFS Master-Worker Pattern (NameNode + DataNodes)")
    print("Nodes:       1 Metadata Coordinator + 3 Storage Nodes (DataNode-1, 2, 3)")
    print("Invariants:  Replication Factor R=2, Cryptographic SHA-256 Block Integrity")
    print("Syllabus:    Unit IV -- Emerging Paradigms (Distributed File Systems)")

    # 1. Setup DFS Cluster
    print_step(1, "Cluster Initialization")
    nodes = ["DataNode-1", "DataNode-2", "DataNode-3"]
    dfs = DistributedFileSystem(node_ids=nodes, default_block_size=64, default_replication_factor=2)
    print("  [NameNode] Initialized Metadata Service (Index & Block Registry).")
    print(f"  [DataNodes] Initialized {len(nodes)} independent disk partitions: {nodes}")

    # 2. Write File
    print_step(2, "Partitioning and Replicating File: 'job_artifact_dataset.csv'")
    raw_content = (
        b"job_id,timestamp,metric_value,worker_node,status\n"
        b"1001,2026-10-12T10:00:00Z,94.2,worker-alpha,COMPLETED\n"
        b"1002,2026-10-12T10:00:01Z,88.7,worker-beta,COMPLETED\n"
        b"1003,2026-10-12T10:00:02Z,12.4,worker-alpha,COMPLETED\n"
    )
    print(f"  * Total File Size: {len(raw_content)} bytes")
    print(f"  * Block Size: 64 bytes per block (Target Replication Factor: R=2)")

    meta = dfs.write_file("job_artifact_dataset.csv", raw_content, block_size=64, replication_factor=2)
    print(f"  [OK] File partitioned into {len(meta.blocks)} blocks:")
    for b in meta.blocks:
        print(f"       -> Block [{b.block_id}] ({b.size_bytes}B, SHA-256={b.checksum_sha256[:12]}...) -> Replicas on: {b.replica_node_ids}")

    # 3. Read File in Healthy State
    print_step(3, "Healthy State Retrieval & Integrity Verification")
    read_back = dfs.read_file("job_artifact_dataset.csv")
    assert read_back == raw_content, "Reassembled content must match original byte-for-byte!"
    print(f"  [OK] Successfully streamed and reassembled all {len(meta.blocks)} blocks.")
    print(f"       Cryptographic integrity verified across all blocks.")

    # 4. Fault Injection: Storage Node Crash
    print_step(4, "Fault Injection: DataNode-1 Crashes Abruptly")
    print("  [!] Marking 'DataNode-1' as OFFLINE (simulating disk failure / network disconnect)...")
    dfs.set_node_online("DataNode-1", False)

    health_before = dfs.check_file_health("job_artifact_dataset.csv")
    print("\nDFS Health Assessment Post-Failure:")
    print(f"  * Total Blocks:    {health_before['total_blocks']}")
    print(f"  * Healthy (R>=2):  {health_before['healthy_blocks']}")
    print(f"  * Degraded (R=1): {health_before['degraded_blocks']}")
    print(f"  * Lost (R=0):      {health_before['lost_blocks']}")
    print(f"  * File Readable?   {health_before['is_readable']}")

    # 5. Transparent Failover Read
    print_step(5, "Transparent Failover Read with DataNode-1 Down")
    print("  [Client] Requesting 'job_artifact_dataset.csv' while DataNode-1 is offline...")
    failover_read = dfs.read_file("job_artifact_dataset.csv")
    assert failover_read == raw_content, "Failover read must return identical content using surviving replicas!"
    print("  [SUCCESS] Client retrieved full file with ZERO data loss in this scenario (F=1 < R=2).")
    print("  [Durability Invariant] File remains readable while >=1 valid replica survives per block.")
    print("  [Limitation Boundary] Simultaneous loss/corruption of all R replicas for any block causes unrecoverable permanent data loss.")

    # 6. Autonomous Self-Healing (Re-Replication)
    print_step(6, "HDFS-Style Autonomous Re-Replication (Self-Healing)")
    print("  [NameNode] Scanning for degraded blocks (R < 2)...")
    re_replicated = dfs.re_replicate_degraded_blocks("job_artifact_dataset.csv")
    print(f"  [NameNode] Successfully copied {re_replicated} degraded blocks to surviving DataNodes.")

    health_after = dfs.check_file_health("job_artifact_dataset.csv")
    print("\nDFS Health Assessment Post-Self-Healing:")
    print(f"  * Healthy Blocks (R=2): {health_after['healthy_blocks']} / {health_after['total_blocks']}")
    print(f"  * Degraded Blocks:      {health_after['degraded_blocks']}")
    print("  [OK] Cluster restored to full target redundancy (R=2) despite permanent DataNode-1 loss.")

    # 7. Theoretical Comparison
    print_step(7, "Comparison with Production Systems")
    print("1. Apache HDFS:")
    print("   - Active/Standby NameNodes (via ZooKeeper + Quorum Journal Manager).")
    print("   - 128MB block default with 3x rack-aware replication.")
    print("   - Append-only write semantics.")
    print("2. Amazon S3:")
    print("   - Object storage (key-value) instead of hierarchical filesystem.")
    print("   - Erasure coding (e.g., 8+4) instead of 3x replication for 99.999999999% durability.")
    print("3. Educational Simulation (Demonstrated):")
    print("   - Local multi-directory isolation demonstrating metadata separation, block hashing,")
    print("     replica fallback, and self-healing without cloud dependencies.")

    dfs.cleanup()
    print_banner("DISTRIBUTED FILE SYSTEM DEMONSTRATION COMPLETE")
    return 0


if __name__ == "__main__":
    sys.exit(run_dfs_demonstration())
