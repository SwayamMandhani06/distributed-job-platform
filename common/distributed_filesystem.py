"""
Distributed File System (DFS) Architecture & Artifact Storage (Unit IV — Emerging Paradigms).

Theoretical Background (HDFS & Google File System Architecture):
-----------------------------------------------------------------
In Distributed File Systems (e.g., GFS, Apache HDFS, Ceph), files too large for a single
machine or requiring fault-tolerant persistence are partitioned into fixed-size blocks
and distributed across a cluster of independent storage nodes.

Key Architectural Separation:
-----------------------------
1. Metadata Service (NameNode in HDFS / Master in GFS):
   - Maintains the filesystem namespace (directory tree, filenames, ownership).
   - Maps each file to an ordered sequence of Block IDs.
   - Tracks which Storage Nodes currently hold replicas of each block.
   - CRITICAL: The metadata coordinator does NOT store or transfer raw file content payload.
     This eliminates the coordinator as a network bottleneck during file reads/writes.

2. Storage Nodes (DataNodes in HDFS / Chunkservers in GFS):
   - Store raw blocks as independent files in their own isolated storage partitions.
   - Validate data integrity using cryptographic block checksums (e.g., SHA-256).
   - Periodically report block health and respond directly to client read/write requests.

3. Replication Strategy & Durability Guarantees:
   - Each block is replicated across R independent nodes (this simulation uses R = 2).
   - Qualified Durability: A file remains readable if at least one valid replica survives
     for every constituent block (i.e. concurrent node failures F < R).
   - Data Loss Boundary: Simultaneous loss or corruption of all R replicas for any block
     causes permanent unrecoverable data loss (raising BlockUnavailableError).
   - Self-Healing: The coordinator scans for degraded blocks (active replicas < target R)
     and commands surviving nodes to copy blocks to online nodes, preserving SHA-256 integrity.

Consistency & Durability Trade-offs:
------------------------------------
- Write-Once-Read-Many (WORM): Files are immutable or append-only (simplifies consistency).
- Network Locality: Clients read from the nearest available replica node.
- Educational Simulation vs. Production HDFS:
  This module simulates metadata/block decoupling, SHA-256 block hashing, and re-replication
  across isolated local directories. It does NOT claim to provide production HDFS capabilities
  (such as ZooKeeper active/standby failover, Quorum Journal Manager, or physical rack-aware network topology).
"""

import hashlib
import os
import shutil
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from common.logging_config import setup_logger

logger = setup_logger("dfs")


class BlockCorruptError(Exception):
    """Raised when a retrieved block fails cryptographic checksum verification."""
    pass


class BlockUnavailableError(Exception):
    """Raised when all replicas of a block are offline or unavailable."""
    pass


class FileNotFoundDFS(Exception):
    """Raised when requested file does not exist in DFS metadata."""
    pass


@dataclass
class BlockMetadata:
    """Metadata for an individual file chunk block."""
    block_id: str
    chunk_index: int
    size_bytes: int
    checksum_sha256: str
    replica_node_ids: List[str] = field(default_factory=list)


@dataclass
class FileMetadata:
    """Filesystem namespace metadata for a stored file."""
    filename: str
    total_size_bytes: int
    block_size_bytes: int
    replication_factor: int
    blocks: List[BlockMetadata] = field(default_factory=list)
    created_at_iso: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class StorageNode:
    """Simulates an independent DataNode storage machine with its own disk partition."""

    def __init__(self, node_id: str, storage_root_dir: Any) -> None:
        self.node_id = str(node_id)
        self.storage_dir = Path(storage_root_dir) / self.node_id
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.is_online = True
        self.stored_blocks: Set[str] = set()

    def store_block(self, block_id: str, data: bytes, checksum_sha256: str) -> None:
        """Store a block file and checksum in this node's private directory."""
        if not self.is_online:
            raise IOError(f"Storage node {self.node_id} is offline.")

        block_path = self.storage_dir / f"{block_id}.block"
        meta_path = self.storage_dir / f"{block_id}.meta"

        with open(block_path, "wb") as f:
            f.write(data)
        with open(meta_path, "w", encoding="utf-8") as f:
            f.write(checksum_sha256)

        self.stored_blocks.add(block_id)
        logger.debug("[%s] Stored block %s (%d bytes)", self.node_id, block_id, len(data))

    def read_block(self, block_id: str) -> bytes:
        """Read block data and verify cryptographic checksum integrity."""
        if not self.is_online:
            raise IOError(f"Storage node {self.node_id} is offline.")

        block_path = self.storage_dir / f"{block_id}.block"
        meta_path = self.storage_dir / f"{block_id}.meta"

        if not block_path.exists():
            raise FileNotFoundError(f"Block {block_id} not found on node {self.node_id}.")

        with open(block_path, "rb") as f:
            data = f.read()

        expected_checksum = ""
        if meta_path.exists():
            with open(meta_path, "r", encoding="utf-8") as f:
                expected_checksum = f.read().strip()

        # Cryptographic integrity verification
        actual_checksum = hashlib.sha256(data).hexdigest()
        if expected_checksum and actual_checksum != expected_checksum:
            raise BlockCorruptError(
                f"Block corruption detected on {self.node_id} for {block_id}! "
                f"Expected {expected_checksum[:10]}..., got {actual_checksum[:10]}..."
            )

        return data

    def corrupt_block(self, block_id: str) -> None:
        """Simulate bit rot or disk block corruption for testing."""
        block_path = self.storage_dir / f"{block_id}.block"
        if block_path.exists():
            with open(block_path, "wb") as f:
                f.write(b"CORRUPTED_BIT_ROT_DATA")

    def has_block(self, block_id: str) -> bool:
        return self.is_online and (self.storage_dir / f"{block_id}.block").exists()


class DistributedFileSystem:
    """Metadata coordinator (NameNode) managing file namespace and block replication."""

    def __init__(
        self,
        node_ids: List[str],
        base_storage_dir: Optional[Path] = None,
        default_block_size: int = 64,  # Small chunk size for demonstration
        default_replication_factor: int = 2,
    ) -> None:
        self.default_block_size = default_block_size
        self.default_replication = default_replication_factor
        self.temp_dir: Optional[tempfile.TemporaryDirectory] = None

        if base_storage_dir is None:
            self.temp_dir = tempfile.TemporaryDirectory()
            self.base_dir = Path(self.temp_dir.name)
        else:
            self.base_dir = Path(base_storage_dir)

        # Storage nodes (DataNodes)
        self.nodes: Dict[str, StorageNode] = {}
        for nid in node_ids:
            self.nodes[nid] = StorageNode(node_id=nid, storage_root_dir=self.base_dir)

        # Namespace metadata: filename -> FileMetadata
        self.namespace: Dict[str, FileMetadata] = {}

    def cleanup(self) -> None:
        if self.temp_dir:
            self.temp_dir.cleanup()

    def get_node(self, node_id: str) -> StorageNode:
        return self.nodes[node_id]

    def set_node_online(self, node_id: str, online: bool) -> None:
        if node_id in self.nodes:
            self.nodes[node_id].is_online = online
            logger.info("Storage node %s state changed: online=%s", node_id, online)

    def write_file(
        self,
        filename: str,
        content: bytes,
        block_size: Optional[int] = None,
        replication_factor: Optional[int] = None,
    ) -> FileMetadata:
        """Partition file into chunks, calculate SHA-256 checksums, and replicate across nodes."""
        b_size = block_size or self.default_block_size
        r_factor = min(replication_factor or self.default_replication, len(self.nodes))
        total_size = len(content)

        file_meta = FileMetadata(
            filename=filename,
            total_size_bytes=total_size,
            block_size_bytes=b_size,
            replication_factor=r_factor,
        )

        # Split content into blocks
        available_nodes = sorted(list(self.nodes.keys()))
        chunk_idx = 0
        offset = 0

        while offset < total_size or (total_size == 0 and chunk_idx == 0):
            chunk_data = content[offset : offset + b_size]
            block_id = f"blk-{hashlib.md5(f'{filename}:{chunk_idx}'.encode()).hexdigest()[:12]}"
            checksum = hashlib.sha256(chunk_data).hexdigest()

            # Select R distinct storage nodes using round-robin placement
            target_nodes: List[str] = []
            for r in range(r_factor):
                node_idx = (chunk_idx + r) % len(available_nodes)
                target_nodes.append(available_nodes[node_idx])

            # Store replicas on target DataNodes
            for nid in target_nodes:
                self.nodes[nid].store_block(block_id, chunk_data, checksum)

            block_meta = BlockMetadata(
                block_id=block_id,
                chunk_index=chunk_idx,
                size_bytes=len(chunk_data),
                checksum_sha256=checksum,
                replica_node_ids=target_nodes,
            )
            file_meta.blocks.append(block_meta)

            chunk_idx += 1
            offset += b_size
            if total_size == 0:
                break

        self.namespace[filename] = file_meta
        logger.info(
            "DFS WRITE: File '%s' (%d bytes) partitioned into %d blocks (R=%d)",
            filename,
            total_size,
            len(file_meta.blocks),
            r_factor,
        )
        return file_meta

    def read_file(self, filename: str) -> bytes:
        """Stream blocks in order, falling back to replicas if primary node is offline."""
        meta = self.namespace.get(filename)
        if not meta:
            raise FileNotFoundDFS(f"File '{filename}' not found in DFS metadata.")

        reassembled = bytearray()

        for b_meta in meta.blocks:
            block_bytes = None
            last_err = None

            # Attempt to read from replicas in order
            for nid in b_meta.replica_node_ids:
                node = self.nodes.get(nid)
                if node and node.is_online:
                    try:
                        block_bytes = node.read_block(b_meta.block_id)
                        logger.debug("Successfully read block %s from node %s", b_meta.block_id, nid)
                        break
                    except (IOError, BlockCorruptError, FileNotFoundError) as err:
                        last_err = err
                        logger.warning("Failed to read block %s from node %s: %s (Trying replica)", b_meta.block_id, nid, err)

            if block_bytes is None:
                raise BlockUnavailableError(
                    f"CRITICAL DATA LOSS: All replicas for block {b_meta.block_id} "
                    f"of file '{filename}' are offline or corrupted! Last error: {last_err}"
                )

            reassembled.extend(block_bytes)

        return bytes(reassembled)

    def check_file_health(self, filename: str) -> Dict[str, Any]:
        """Inspect health of all block replicas across cluster storage nodes."""
        meta = self.namespace.get(filename)
        if not meta:
            raise FileNotFoundDFS(f"File '{filename}' not found.")

        total_blocks = len(meta.blocks)
        healthy_blocks = 0
        degraded_blocks = 0
        lost_blocks = 0

        block_details = []
        for b in meta.blocks:
            online_replicas = [
                nid for nid in b.replica_node_ids
                if self.nodes[nid].has_block(b.block_id)
            ]
            count = len(online_replicas)
            if count >= meta.replication_factor:
                healthy_blocks += 1
                status = "HEALTHY"
            elif count > 0:
                degraded_blocks += 1
                status = "DEGRADED"
            else:
                lost_blocks += 1
                status = "LOST"

            block_details.append({
                "block_id": b.block_id,
                "online_replicas": online_replicas,
                "target_replication": meta.replication_factor,
                "status": status,
            })

        return {
            "filename": filename,
            "total_blocks": total_blocks,
            "healthy_blocks": healthy_blocks,
            "degraded_blocks": degraded_blocks,
            "lost_blocks": lost_blocks,
            "is_readable": lost_blocks == 0,
            "blocks": block_details,
        }

    def re_replicate_degraded_blocks(self, filename: str) -> int:
        """HDFS-style self-healing: copy degraded blocks to new online healthy nodes."""
        meta = self.namespace.get(filename)
        if not meta:
            return 0

        re_replicated_count = 0
        online_nodes = [nid for nid, node in self.nodes.items() if node.is_online]

        for b in meta.blocks:
            current_online_replicas = [
                nid for nid in b.replica_node_ids
                if self.nodes[nid].has_block(b.block_id)
            ]
            needed = meta.replication_factor - len(current_online_replicas)

            if needed > 0 and current_online_replicas:
                # Read valid data from one surviving replica
                source_node_id = current_online_replicas[0]
                data = self.nodes[source_node_id].read_block(b.block_id)

                # Find candidate nodes that do not already store this block
                candidates = [nid for nid in online_nodes if nid not in b.replica_node_ids]
                for new_nid in candidates[:needed]:
                    self.nodes[new_nid].store_block(b.block_id, data, b.checksum_sha256)
                    b.replica_node_ids.append(new_nid)
                    re_replicated_count += 1
                    logger.info("DFS RE-REPLICATION: Replicated block %s to node %s", b.block_id, new_nid)

        return re_replicated_count
