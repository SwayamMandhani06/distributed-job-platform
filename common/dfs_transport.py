"""
Real Networked Distributed File System (DFS) Storage Cluster (Unit IV).

Theoretical Background (Decoupled Metadata & Storage Architecture):
-------------------------------------------------------------------
In Distributed File Systems (e.g. HDFS, Google File System, Ceph):
1. Metadata Service (NameNode):
   - Maintains namespace hierarchy, filename-to-block mapping, and replica locations.
   - Persists metadata durably on disk.
   - Does NOT handle raw file payloads, eliminating network bottlenecks.
2. Storage Nodes (DataNodes):
   - Independent server processes managing dedicated disk partitions.
   - Serve block read and write requests over HTTP/TCP sockets.
   - Enforce cryptographic SHA-256 data integrity on every block access.
3. Fault Tolerance & Self-Healing:
   - Configurable replication factor R (default R=2 or R=3).
   - Tolerates up to R - 1 concurrent node failures per block.
   - If a storage node crashes or suffers bit rot, the metadata service transparently
     falls back to surviving replicas and re-replicates degraded blocks to healthy nodes.
   - Complete Replica Loss: If all R replicas for any block are lost, permanent data loss
     is explicitly raised (BlockUnavailableError); recovery of non-existent data is never claimed.
"""

import argparse
import base64
import hashlib
import json
import logging
import os
import shutil
import socket
import sys
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib import request as url_request
from urllib.error import HTTPError, URLError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


def send_http_json(
    url: str,
    method: str = "GET",
    payload: Optional[Dict[str, Any]] = None,
    timeout_seconds: float = 2.0,
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


# ===========================================================================
# 1. Independent Networked Storage Node (DataNode)
# ===========================================================================
class NetworkedStorageNode:
    """Independent storage server hosting block partitions on disk."""

    def __init__(self, node_id: str, port: int, storage_dir: Path) -> None:
        self.node_id = str(node_id)
        self.port = int(port)
        self.storage_dir = Path(storage_dir) / self.node_id
        self.blocks_dir = self.storage_dir / "blocks"
        self.blocks_dir.mkdir(parents=True, exist_ok=True)
        self.is_online = True
        self.server: Optional[HTTPServer] = None
        self.logger = logging.getLogger(f"DataNode-{node_id}")

    def store_block(self, block_id: str, data: bytes, checksum_sha256: str) -> None:
        """Atomically persist block bytes and checksum metadata."""
        if not self.is_online:
            raise IOError(f"Storage node {self.node_id} is offline.")

        block_path = self.blocks_dir / f"{block_id}.block"
        meta_path = self.blocks_dir / f"{block_id}.meta"

        # Verify incoming checksum before writing
        actual_checksum = hashlib.sha256(data).hexdigest()
        if actual_checksum != checksum_sha256:
            raise ValueError(f"Checksum mismatch: expected {checksum_sha256}, got {actual_checksum}")

        temp_block = self.blocks_dir / f"{block_id}.tmp"
        with open(temp_block, "wb") as f:
            f.write(data)
        os.replace(temp_block, block_path)

        with open(meta_path, "w", encoding="utf-8") as f:
            f.write(checksum_sha256)

        self.logger.info("Stored block %s (%d bytes, sha256=%s...)", block_id, len(data), checksum_sha256[:10])

    def read_block(self, block_id: str) -> bytes:
        """Read block and enforce cryptographic SHA-256 integrity."""
        if not self.is_online:
            raise IOError(f"Storage node {self.node_id} is offline.")

        block_path = self.blocks_dir / f"{block_id}.block"
        meta_path = self.blocks_dir / f"{block_id}.meta"

        if not block_path.exists():
            raise FileNotFoundError(f"Block {block_id} not found on node {self.node_id}.")

        with open(block_path, "rb") as f:
            data = f.read()

        expected_checksum = ""
        if meta_path.exists():
            with open(meta_path, "r", encoding="utf-8") as f:
                expected_checksum = f.read().strip()

        actual_checksum = hashlib.sha256(data).hexdigest()
        if expected_checksum and actual_checksum != expected_checksum:
            raise IOError(
                f"CORRUPTION DETECTED on {self.node_id} for block {block_id}: "
                f"expected {expected_checksum[:10]}..., got {actual_checksum[:10]}..."
            )

        return data

    def corrupt_block(self, block_id: str) -> None:
        """Fault injection: simulate disk bit-rot / sector corruption."""
        block_path = self.blocks_dir / f"{block_id}.block"
        if block_path.exists():
            with open(block_path, "wb") as f:
                f.write(b"CORRUPTED_BIT_ROT_PAYLOAD")
            self.logger.warning("INJECTED BIT ROT into block %s", block_id)

    def has_block(self, block_id: str) -> bool:
        return self.is_online and (self.blocks_dir / f"{block_id}.block").exists()


class StorageNodeHTTPHandler(BaseHTTPRequestHandler):
    """HTTP endpoint handler for DataNodes."""

    node: Optional[NetworkedStorageNode] = None

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
            self._send_json(200, {"status": "ok", "node_id": self.node.node_id, "online": self.node.is_online})
        elif self.path.startswith("/block/has/"):
            block_id = self.path[len("/block/has/") :]
            has = self.node.has_block(block_id)
            self._send_json(200, {"block_id": block_id, "has_block": has})
        elif self.path.startswith("/block/read/"):
            block_id = self.path[len("/block/read/") :]
            try:
                data = self.node.read_block(block_id)
                b64 = base64.b64encode(data).decode("utf-8")
                checksum = hashlib.sha256(data).hexdigest()
                self._send_json(200, {"status": "ok", "block_id": block_id, "data_b64": b64, "checksum": checksum})
            except FileNotFoundError as fnf:
                self._send_json(404, {"error": "BlockNotFound", "message": str(fnf)})
            except IOError as ioe:
                self._send_json(409, {"error": "BlockCorruptError", "message": str(ioe)})
        else:
            self._send_json(404, {"error": "Not found"})

    def do_POST(self) -> None:
        content_length = int(self.headers.get("Content-Length", 0))
        raw_body = self.rfile.read(content_length).decode("utf-8") if content_length > 0 else "{}"
        try:
            d = json.loads(raw_body)
        except Exception:
            d = {}

        if self.path == "/block/store":
            block_id = d.get("block_id", "")
            data_b64 = d.get("data_b64", "")
            checksum = d.get("checksum", "")
            try:
                raw_bytes = base64.b64decode(data_b64.encode("utf-8"))
                self.node.store_block(block_id, raw_bytes, checksum)
                self._send_json(200, {"status": "stored", "block_id": block_id, "bytes": len(raw_bytes)})
            except Exception as exc:
                self._send_json(400, {"error": "StoreFailed", "message": str(exc)})
        elif self.path.startswith("/block/corrupt/"):
            block_id = self.path[len("/block/corrupt/") :]
            self.node.corrupt_block(block_id)
            self._send_json(200, {"status": "corrupted", "block_id": block_id})
        elif self.path == "/set_online":
            online = bool(d.get("online", True))
            self.node.is_online = online
            self._send_json(200, {"status": "updated", "online": online})
        else:
            self._send_json(404, {"error": "Unknown endpoint"})


def start_storage_node_server(node: NetworkedStorageNode) -> HTTPServer:
    handler = type("BoundStorageHandler", (StorageNodeHTTPHandler,), {"node": node})
    server = HTTPServer(("127.0.0.1", node.port), handler)
    node.server = server
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    return server


# ===========================================================================
# 2. Networked Metadata Coordinator (NameNode)
# ===========================================================================
@dataclass
class NetworkedBlockMeta:
    block_id: str
    chunk_index: int
    size_bytes: int
    checksum_sha256: str
    replica_node_ids: List[str] = field(default_factory=list)


@dataclass
class NetworkedFileMeta:
    filename: str
    total_size_bytes: int
    block_size_bytes: int
    replication_factor: int
    blocks: List[NetworkedBlockMeta] = field(default_factory=list)
    created_at_iso: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class NetworkedMetadataService:
    """Central metadata coordinator managing namespace and block locations with persistent storage."""

    def __init__(
        self,
        port: int,
        storage_nodes: Dict[str, str],  # node_id -> "http://127.0.0.1:port"
        metadata_file: Path,
        default_block_size: int = 64,
        default_replication_factor: int = 2,
    ) -> None:
        self.port = int(port)
        self.storage_nodes = dict(storage_nodes)
        self.metadata_file = Path(metadata_file)
        self.default_block_size = int(default_block_size)
        self.default_replication = int(default_replication_factor)

        self.namespace: Dict[str, NetworkedFileMeta] = {}
        self._lock = threading.RLock()
        self.server: Optional[HTTPServer] = None
        self.logger = logging.getLogger("NameNode")

        # Load persisted metadata if available
        self._load_metadata()

    def _load_metadata(self) -> None:
        if self.metadata_file.exists():
            try:
                with open(self.metadata_file, "r", encoding="utf-8") as f:
                    raw = json.load(f)
                    for fname, fmeta in raw.items():
                        blocks = [NetworkedBlockMeta(**b) for b in fmeta.get("blocks", [])]
                        self.namespace[fname] = NetworkedFileMeta(
                            filename=fname,
                            total_size_bytes=fmeta["total_size_bytes"],
                            block_size_bytes=fmeta["block_size_bytes"],
                            replication_factor=fmeta["replication_factor"],
                            blocks=blocks,
                            created_at_iso=fmeta.get("created_at_iso", ""),
                        )
                self.logger.info("Loaded metadata for %d files from %s", len(self.namespace), self.metadata_file)
            except Exception as exc:
                self.logger.error("Failed to load metadata file: %s", exc)

    def _persist_metadata(self) -> None:
        try:
            dump_dict = {}
            for fname, meta in self.namespace.items():
                dump_dict[fname] = asdict(meta)
            temp_file = self.metadata_file.with_suffix(".tmp")
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(dump_dict, f, indent=2)
            os.replace(temp_file, self.metadata_file)
        except Exception as exc:
            self.logger.error("Failed to persist metadata: %s", exc)

    def write_file(
        self,
        filename: str,
        content: bytes,
        block_size: Optional[int] = None,
        replication_factor: Optional[int] = None,
    ) -> NetworkedFileMeta:
        """Split file into blocks, store replicas across independent nodes via HTTP, and persist metadata."""
        with self._lock:
            b_size = block_size or self.default_block_size
            r_factor = min(replication_factor or self.default_replication, len(self.storage_nodes))
            total_size = len(content)

            file_meta = NetworkedFileMeta(
                filename=filename,
                total_size_bytes=total_size,
                block_size_bytes=b_size,
                replication_factor=r_factor,
            )

            available_node_ids = sorted(list(self.storage_nodes.keys()))
            chunk_idx = 0
            offset = 0

            while offset < total_size or (total_size == 0 and chunk_idx == 0):
                chunk_data = content[offset : offset + b_size]
                block_id = f"blk-{hashlib.md5(f'{filename}:{chunk_idx}'.encode()).hexdigest()[:12]}"
                checksum = hashlib.sha256(chunk_data).hexdigest()
                b64_data = base64.b64encode(chunk_data).decode("utf-8")

                # Choose R distinct nodes
                target_nodes: List[str] = []
                for r in range(r_factor):
                    node_idx = (chunk_idx + r) % len(available_node_ids)
                    target_nodes.append(available_node_ids[node_idx])

                # Transmit over real HTTP network to each storage node
                successful_replicas: List[str] = []
                for nid in target_nodes:
                    node_url = self.storage_nodes[nid]
                    ok, code, resp, err = send_http_json(
                        f"{node_url}/block/store",
                        method="POST",
                        payload={"block_id": block_id, "data_b64": b64_data, "checksum": checksum},
                    )
                    if ok and code == 200:
                        successful_replicas.append(nid)
                    else:
                        self.logger.warning("Failed to store block %s on %s: %s", block_id, nid, err)

                if len(successful_replicas) == 0:
                    raise IOError(f"Write failed: unable to store any replicas of block {block_id} across storage cluster.")

                b_meta = NetworkedBlockMeta(
                    block_id=block_id,
                    chunk_index=chunk_idx,
                    size_bytes=len(chunk_data),
                    checksum_sha256=checksum,
                    replica_node_ids=successful_replicas,
                )
                file_meta.blocks.append(b_meta)

                chunk_idx += 1
                offset += b_size
                if total_size == 0:
                    break

            self.namespace[filename] = file_meta
            self._persist_metadata()
            self.logger.info("Wrote file '%s' (%d bytes, %d blocks, R=%d)", filename, total_size, len(file_meta.blocks), r_factor)
            return file_meta

    def read_file(self, filename: str) -> bytes:
        """Stream blocks in order from storage nodes, falling back transparently on error/corruption."""
        with self._lock:
            meta = self.namespace.get(filename)
            if not meta:
                raise FileNotFoundError(f"File '{filename}' not found in DFS metadata.")

        reassembled = bytearray()

        for b in meta.blocks:
            block_data = None
            last_err = None

            # Try each replica node in order over network
            for nid in b.replica_node_ids:
                node_url = self.storage_nodes.get(nid)
                if not node_url:
                    continue

                ok, code, resp, err = send_http_json(f"{node_url}/block/read/{b.block_id}", timeout_seconds=0.5)
                if ok and code == 200 and resp:
                    # Valid block read over network!
                    raw_bytes = base64.b64decode(resp["data_b64"].encode("utf-8"))
                    actual_checksum = hashlib.sha256(raw_bytes).hexdigest()
                    if actual_checksum == b.checksum_sha256:
                        block_data = raw_bytes
                        self.logger.debug("Successfully read block %s from %s", b.block_id, nid)
                        break
                    else:
                        last_err = f"Checksum mismatch from {nid}"
                        self.logger.warning("Corrupted checksum for %s from %s, trying replica...", b.block_id, nid)
                else:
                    last_err = f"Node {nid} returned error {code}: {err}"
                    self.logger.warning("Node %s unavailable for block %s, trying replica...", nid, b.block_id)

            if block_data is None:
                raise IOError(
                    f"CRITICAL DATA LOSS: All replicas for block {b.block_id} of file '{filename}' "
                    f"are offline or corrupted! Last error: {last_err}"
                )

            reassembled.extend(block_data)

        return bytes(reassembled)

    def check_file_health(self, filename: str) -> Dict[str, Any]:
        """Verify block replicas across cluster storage nodes over network."""
        with self._lock:
            meta = self.namespace.get(filename)
            if not meta:
                raise FileNotFoundError(f"File '{filename}' not found.")

        healthy_blocks = 0
        degraded_blocks = 0
        lost_blocks = 0
        block_details = []

        for b in meta.blocks:
            online_replicas = []
            for nid in b.replica_node_ids:
                node_url = self.storage_nodes.get(nid)
                if node_url:
                    ok, code, resp, _ = send_http_json(f"{node_url}/block/has/{b.block_id}", timeout_seconds=0.3)
                    if ok and code == 200 and resp and resp.get("has_block"):
                        online_replicas.append(nid)

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
            "total_blocks": len(meta.blocks),
            "healthy_blocks": healthy_blocks,
            "degraded_blocks": degraded_blocks,
            "lost_blocks": lost_blocks,
            "is_readable": lost_blocks == 0,
            "blocks": block_details,
        }

    def re_replicate_file(self, filename: str) -> int:
        """Restore target replication redundancy from surviving valid replicas over network."""
        with self._lock:
            meta = self.namespace.get(filename)
            if not meta:
                return 0

        re_replicated_count = 0
        # Determine live storage nodes via heartbeat/health
        live_nodes = []
        for nid, url in sorted(self.storage_nodes.items()):
            ok, code, _, _ = send_http_json(f"{url}/health", timeout_seconds=0.3)
            if ok and code == 200:
                live_nodes.append(nid)

        for b in meta.blocks:
            # Check surviving replicas over network
            surviving_nodes = []
            valid_data_bytes = None

            for nid in list(b.replica_node_ids):
                node_url = self.storage_nodes.get(nid)
                if node_url and nid in live_nodes:
                    ok, code, resp, _ = send_http_json(f"{node_url}/block/read/{b.block_id}", timeout_seconds=0.4)
                    if ok and code == 200 and resp:
                        raw = base64.b64decode(resp["data_b64"].encode("utf-8"))
                        if hashlib.sha256(raw).hexdigest() == b.checksum_sha256:
                            surviving_nodes.append(nid)
                            if valid_data_bytes is None:
                                valid_data_bytes = raw

            needed = meta.replication_factor - len(surviving_nodes)
            if needed > 0 and valid_data_bytes is not None:
                # Find candidate live nodes that do not have this block
                candidates = [nid for nid in live_nodes if nid not in surviving_nodes]
                b64_payload = base64.b64encode(valid_data_bytes).decode("utf-8")

                stored_count = 0
                for target_nid in candidates:
                    if stored_count >= needed:
                        break
                    target_url = self.storage_nodes[target_nid]
                    ok, code, resp, err = send_http_json(
                        f"{target_url}/block/store",
                        method="POST",
                        payload={"block_id": b.block_id, "data_b64": b64_payload, "checksum": b.checksum_sha256},
                        timeout_seconds=1.0,
                    )
                    if ok and code == 200:
                        with self._lock:
                            if target_nid not in b.replica_node_ids:
                                b.replica_node_ids.append(target_nid)
                            # Prune dead replicas from block metadata
                            b.replica_node_ids = [n for n in b.replica_node_ids if n in live_nodes or n == target_nid]
                        stored_count += 1
                        re_replicated_count += 1
                        self.logger.info("RE-REPLICATED block %s to %s over network", b.block_id, target_nid)

        with self._lock:
            self._persist_metadata()
        return re_replicated_count


class MetadataServiceHTTPHandler(BaseHTTPRequestHandler):
    """HTTP endpoint handler for NameNode."""

    service: Optional[NetworkedMetadataService] = None

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
            self._send_json(200, {"status": "ok", "tracked_files": len(self.service.namespace)})
        elif self.path.startswith("/dfs/health/"):
            fname = self.path[len("/dfs/health/") :]
            try:
                report = self.service.check_file_health(fname)
                self._send_json(200, report)
            except FileNotFoundError:
                self._send_json(404, {"error": "FileNotFound", "filename": fname})
        elif self.path.startswith("/dfs/read/"):
            fname = self.path[len("/dfs/read/") :]
            try:
                data = self.service.read_file(fname)
                b64 = base64.b64encode(data).decode("utf-8")
                self._send_json(200, {"status": "ok", "filename": fname, "content_b64": b64, "bytes": len(data)})
            except FileNotFoundError:
                self._send_json(404, {"error": "FileNotFound", "filename": fname})
            except IOError as ioe:
                self._send_json(500, {"error": "BlockUnavailableError", "message": str(ioe)})
        else:
            self._send_json(404, {"error": "Not found"})

    def do_POST(self) -> None:
        content_length = int(self.headers.get("Content-Length", 0))
        raw_body = self.rfile.read(content_length).decode("utf-8") if content_length > 0 else "{}"
        try:
            d = json.loads(raw_body)
        except Exception:
            d = {}

        if self.path == "/dfs/write":
            filename = d.get("filename", "")
            content_b64 = d.get("content_b64", "")
            raw_bytes = base64.b64decode(content_b64.encode("utf-8"))
            b_size = d.get("block_size")
            r_factor = d.get("replication_factor")
            try:
                meta = self.service.write_file(filename, raw_bytes, block_size=b_size, replication_factor=r_factor)
                self._send_json(200, {"status": "written", "file_metadata": asdict(meta)})
            except Exception as exc:
                self._send_json(500, {"error": "WriteFailed", "message": str(exc)})
        elif self.path == "/dfs/re_replicate":
            filename = d.get("filename", "")
            count = self.service.re_replicate_file(filename)
            self._send_json(200, {"status": "completed", "re_replicated_blocks_count": count})
        else:
            self._send_json(404, {"error": "Unknown endpoint"})


def start_metadata_service_server(service: NetworkedMetadataService) -> HTTPServer:
    handler = type("BoundMetadataHandler", (MetadataServiceHTTPHandler,), {"service": service})
    server = HTTPServer(("127.0.0.1", service.port), handler)
    service.server = server
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    return server


def main() -> None:
    parser = argparse.ArgumentParser(description="Distributed File System Network Components.")
    parser.add_argument("--mode", choices=["datanode", "namenode"], required=True, help="Component mode")
    parser.add_argument("--node-id", default="DataNode-1", help="Storage node identifier")
    parser.add_argument("--port", type=int, required=True, help="Port to listen on")
    parser.add_argument("--storage-dir", default="dfs_storage", help="Root partition directory")
    parser.add_argument("--metadata-file", default="dfs_metadata.json", help="Persistent metadata JSON file")
    parser.add_argument("--nodes", default="{}", help="JSON mapping of node_id to URL for NameNode")

    args = parser.parse_args()

    if args.mode == "datanode":
        node = NetworkedStorageNode(node_id=args.node_id, port=args.port, storage_dir=Path(args.storage_dir))
        server = start_storage_node_server(node)
        print(f"DataNode {args.node_id} listening on port {args.port}...")
        try:
            while True:
                time.sleep(1.0)
        except KeyboardInterrupt:
            server.shutdown()
    elif args.mode == "namenode":
        nodes_map = json.loads(args.nodes)
        service = NetworkedMetadataService(
            port=args.port,
            storage_nodes=nodes_map,
            metadata_file=Path(args.metadata_file),
        )
        server = start_metadata_service_server(service)
        print(f"NameNode listening on port {args.port}...")
        try:
            while True:
                time.sleep(1.0)
        except KeyboardInterrupt:
            server.shutdown()


if __name__ == "__main__":
    main()
