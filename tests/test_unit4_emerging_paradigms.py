"""
Unit IV: Emerging Paradigms Automated Test Suite.

Tests cover:
1. Distributed Object-Based Systems (common/distributed_object.py):
   - Transparent client proxy invocation & parameter marshalling.
   - Remote exception serialization and client-side bubbling.
   - Network timeout enforcement.
   - Missing service / method handling.
2. Distributed File Systems (common/distributed_filesystem.py):
   - Metadata vs. storage separation & multi-block partitioning.
   - Replica placement invariant (R=2).
   - Transparent replica failover when a storage node crashes.
   - Cryptographic SHA-256 corruption detection and healthy replica fallback.
   - Autonomous re-replication of degraded blocks to restore redundancy.
3. Serverless Computing (common/serverless_runtime.py):
   - Stateless event invocation & context passing.
   - Cold start latency vs. warm container reuse lifecycle.
   - Strict execution limit (timeout) cutoff returning HTTP 504.
   - Idempotency key caching & duplicate event replay prevention.
   - Structured error handling.
"""

import os
import shutil
import tempfile
import time
import unittest
from typing import Any, Dict

from common.distributed_filesystem import (
    BlockUnavailableError,
    DistributedFileSystem,
    FileNotFoundDFS,
    StorageNode,
)
from common.distributed_object import (
    JobManagementServant,
    RemoteClientProxy,
    RemoteCommunicationChannel,
    RemoteException,
    RemoteServerSkeleton,
    RemoteTimeoutError,
)
from common.serverless_runtime import (
    FunctionContext,
    ServerlessRuntime,
    UnsupportedHandlerError,
    delayed_side_effect_handler,
    echo_handler,
    failing_handler,
    idempotent_test_handler,
    slow_timeout_handler,
)


class TestDistributedObjectSystem(unittest.TestCase):
    """Test suite for Distributed Object-Based Systems (Remote Proxy / Stub pattern)."""

    def setUp(self) -> None:
        self.channel = RemoteCommunicationChannel(simulated_latency_seconds=0.005)
        self.skeleton = RemoteServerSkeleton()
        self.servant = JobManagementServant()
        self.skeleton.register_service("JobService", self.servant)
        self.channel.bind_server(self.skeleton)
        self.proxy = RemoteClientProxy(
            service_name="JobService",
            channel=self.channel,
            timeout_seconds=0.5,
        )

    def test_remote_object_successful_invocation(self) -> None:
        """Verify client proxy transparently forwards calls to remote servant."""
        result = self.proxy.submit_job(
            job_type="math_prime_sieve",
            payload={"limit": 500},
        )
        self.assertIn("job_id", result)
        self.assertEqual(result["status"], "queued")
        job_id = result["job_id"]

        # Read back status via proxy
        status_info = self.proxy.get_job_status(job_id)
        self.assertEqual(status_info["job_id"], job_id)
        self.assertEqual(status_info["job_type"], "math_prime_sieve")
        self.assertEqual(status_info["payload"], {"limit": 500})

    def test_remote_object_exception_propagation(self) -> None:
        """Verify server-side exceptions serialize and bubble up as RemoteException."""
        with self.assertRaises(RemoteException) as ctx:
            self.proxy.get_job_status("nonexistent-job-uuid-123")
        self.assertIn("KeyError", str(ctx.exception))
        self.assertIn("does not exist", str(ctx.exception))

    def test_remote_object_timeout_enforcement(self) -> None:
        """Verify that channel latency exceeding client timeout raises RemoteTimeoutError."""
        slow_channel = RemoteCommunicationChannel(simulated_latency_seconds=0.1)
        slow_channel.bind_server(self.skeleton)
        strict_proxy = RemoteClientProxy(
            service_name="JobService",
            channel=slow_channel,
            timeout_seconds=0.03,  # 30ms timeout < 100ms channel latency
        )

        with self.assertRaises(RemoteTimeoutError):
            strict_proxy.submit_job("test_job", {})

    def test_remote_object_unregistered_service(self) -> None:
        """Verify calling an unregistered service raises a RemoteException."""
        orphan_proxy = RemoteClientProxy(
            service_name="NonExistentService",
            channel=self.channel,
            timeout_seconds=0.5,
        )
        with self.assertRaises(RemoteException) as ctx:
            orphan_proxy.some_method()
        self.assertIn("ServiceNotFoundError", str(ctx.exception))

    def test_remote_object_rejects_private_method(self) -> None:
        """Verify calling private method starting with underscore raises MethodNotAllowedError."""
        with self.assertRaises(RemoteException) as ctx:
            self.proxy._internal_helper()
        self.assertIn("MethodNotAllowedError", str(ctx.exception))
        self.assertIn("private or dunder", str(ctx.exception))

    def test_remote_object_rejects_dunder_method(self) -> None:
        """Verify calling dunder method __str__ raises MethodNotAllowedError."""
        with self.assertRaises(RemoteException) as ctx:
            self.proxy.__str__()
        self.assertIn("MethodNotAllowedError", str(ctx.exception))
        self.assertIn("private or dunder", str(ctx.exception))

    def test_remote_object_rejects_unregistered_public_method(self) -> None:
        """Verify calling a public method not present in the allowed list raises MethodNotAllowedError."""
        class MockServiceServant:
            def public_permitted(self) -> str:
                return "permitted"

            def public_unregistered(self) -> str:
                return "should_be_blocked"

        skeleton = RemoteServerSkeleton()
        # Explicitly allow ONLY 'public_permitted'
        skeleton.register_service(
            "MockService",
            MockServiceServant(),
            allowed_methods={"public_permitted"},
        )
        channel = RemoteCommunicationChannel(simulated_latency_seconds=0.001)
        channel.bind_server(skeleton)
        proxy = RemoteClientProxy("MockService", channel=channel)

        # Permitted method succeeds
        self.assertEqual(proxy.public_permitted(), "permitted")

        # Unregistered public method is blocked
        with self.assertRaises(RemoteException) as ctx:
            proxy.public_unregistered()
        self.assertIn("MethodNotAllowedError", str(ctx.exception))
        self.assertIn("not in the registered allowlist", str(ctx.exception))

    def test_remote_object_omitted_allowlist_fails_safely(self) -> None:
        """Verify that omitting an allowlist defaults to an empty set and fails safely without exposing public methods."""
        class UnconfiguredServant:
            def some_public_method(self) -> str:
                return "unsafe_exposure"

        skeleton = RemoteServerSkeleton()
        # Register without explicit allowed_methods and without __remote_allowed_methods__
        skeleton.register_service("UnconfiguredService", UnconfiguredServant())
        channel = RemoteCommunicationChannel(simulated_latency_seconds=0.001)
        channel.bind_server(skeleton)
        proxy = RemoteClientProxy("UnconfiguredService", channel=channel)

        # Call must fail safely with MethodNotAllowedError
        with self.assertRaises(RemoteException) as ctx:
            proxy.some_public_method()
        self.assertIn("MethodNotAllowedError", str(ctx.exception))
        self.assertIn("not in the registered allowlist", str(ctx.exception))


class TestDistributedFileSystem(unittest.TestCase):
    """Test suite for Distributed File System (Metadata Coordinator & Storage Nodes)."""

    def setUp(self) -> None:
        self.dfs = DistributedFileSystem(
            node_ids=["DataNode-1", "DataNode-2", "DataNode-3"],
            default_block_size=32,
            default_replication_factor=2,
        )

    def tearDown(self) -> None:
        self.dfs.cleanup()

    def test_dfs_write_and_read_healthy(self) -> None:
        """Verify normal partitioning, replication (R=2), and reassembly."""
        content = b"Distributed filesystems partition large files into redundant block replicas."
        file_meta = self.dfs.write_file("data.txt", content)

        self.assertEqual(file_meta.filename, "data.txt")
        self.assertEqual(file_meta.total_size_bytes, len(content))
        self.assertGreater(len(file_meta.blocks), 1)

        # Verify each block has target replication factor 2
        for blk in file_meta.blocks:
            self.assertEqual(len(blk.replica_node_ids), 2)

        # Read back content
        retrieved = self.dfs.read_file("data.txt")
        self.assertEqual(retrieved, content)

    def test_dfs_node_failure_transparent_failover(self) -> None:
        """Verify file remains readable when one storage node fails abruptly."""
        content = b"Critical job datasets must survive storage node crashes seamlessly."
        self.dfs.write_file("critical_dataset.csv", content)

        # Crash DataNode-1
        self.dfs.set_node_online("DataNode-1", False)

        # Status check
        health = self.dfs.check_file_health("critical_dataset.csv")
        self.assertTrue(health["is_readable"])
        self.assertGreater(health["degraded_blocks"], 0)

        # Read should succeed via replica on DataNode-2 or DataNode-3
        recovered_content = self.dfs.read_file("critical_dataset.csv")
        self.assertEqual(recovered_content, content)

    def test_dfs_block_corruption_detection(self) -> None:
        """Verify corrupt block fails checksum and falls back to healthy replica."""
        content = b"Integrity verification protects against silent bitrot corruption."
        self.dfs.write_file("integrity_check.bin", content)

        file_meta = self.dfs.namespace["integrity_check.bin"]
        first_block = file_meta.blocks[0]
        corrupted_node_id = first_block.replica_node_ids[0]

        # Use helper corrupt_block on node
        self.dfs.get_node(corrupted_node_id).corrupt_block(first_block.block_id)

        # Read should detect corrupted checksum on tampered_node and retrieve from second replica
        retrieved = self.dfs.read_file("integrity_check.bin")
        self.assertEqual(retrieved, content)

    def test_dfs_autonomous_re_replication(self) -> None:
        """Verify degraded blocks are re-replicated to restore target redundancy."""
        content = b"Self-healing clusters autonomously restore redundancy following hardware loss."
        self.dfs.write_file("healing_test.txt", content)

        # Take node 1 offline
        self.dfs.set_node_online("DataNode-1", False)
        health_pre = self.dfs.check_file_health("healing_test.txt")
        self.assertGreater(health_pre["degraded_blocks"], 0)

        # Trigger autonomous re-replication
        re_replicated_count = self.dfs.re_replicate_degraded_blocks("healing_test.txt")
        self.assertGreater(re_replicated_count, 0)

        health_post = self.dfs.check_file_health("healing_test.txt")
        self.assertEqual(health_post["degraded_blocks"], 0)
        self.assertEqual(health_post["lost_blocks"], 0)

    def test_dfs_all_replicas_loss_causes_permanent_data_loss(self) -> None:
        """Verify loss of all replicas for a block causes permanent unrecoverable data loss."""
        content = b"Unrecoverable data if all replicas fail simultaneously."
        file_meta = self.dfs.write_file("doomed_file.bin", content)

        # Take all DataNodes offline
        for nid in ["DataNode-1", "DataNode-2", "DataNode-3"]:
            self.dfs.set_node_online(nid, False)

        # 1. Health check reports unreadable and lost blocks
        health = self.dfs.check_file_health("doomed_file.bin")
        self.assertFalse(health["is_readable"])
        self.assertEqual(health["lost_blocks"], len(file_meta.blocks))

        # 2. Reading raises BlockUnavailableError
        with self.assertRaises(BlockUnavailableError) as ctx:
            self.dfs.read_file("doomed_file.bin")
        self.assertIn("CRITICAL DATA LOSS", str(ctx.exception))

        # 3. Autonomous re-replication cannot recover when zero replicas survive
        re_replicated = self.dfs.re_replicate_degraded_blocks("doomed_file.bin")
        self.assertEqual(re_replicated, 0)

    def test_dfs_re_replication_preserves_content_checksums_metadata(self) -> None:
        """Verify autonomous re-replication preserves exact byte content, checksums, and metadata."""
        content = b"Durability verification: re-replication must preserve SHA-256 checksums and bytes."
        file_meta = self.dfs.write_file("preserve_test.bin", content)
        original_checksums = [b.checksum_sha256 for b in file_meta.blocks]

        # Take DataNode-1 offline
        self.dfs.set_node_online("DataNode-1", False)
        health_pre = self.dfs.check_file_health("preserve_test.bin")
        self.assertGreater(health_pre["degraded_blocks"], 0)

        # Re-replicate degraded blocks to DataNode-3
        re_replicated = self.dfs.re_replicate_degraded_blocks("preserve_test.bin")
        self.assertGreater(re_replicated, 0)

        # 1. Read-back matches exact original bytes
        recovered = self.dfs.read_file("preserve_test.bin")
        self.assertEqual(recovered, content)

        # 2. Checksums match original block checksums exactly
        updated_meta = self.dfs.namespace["preserve_test.bin"]
        new_checksums = [b.checksum_sha256 for b in updated_meta.blocks]
        self.assertEqual(original_checksums, new_checksums)

        # 3. Target replication count is restored
        health_post = self.dfs.check_file_health("preserve_test.bin")
        self.assertEqual(health_post["degraded_blocks"], 0)
        self.assertEqual(health_post["lost_blocks"], 0)
        self.assertTrue(health_post["is_readable"])

    def test_dfs_missing_file_raises_error(self) -> None:
        """Verify querying nonexistent file raises FileNotFoundDFS exception."""
        from common.distributed_filesystem import FileNotFoundDFS
        with self.assertRaises(FileNotFoundDFS):
            self.dfs.read_file("ghost_file.txt")


class TestServerlessComputingRuntime(unittest.TestCase):
    """Test suite for Serverless Computing Runtime (FaaS event execution & lifecycle)."""

    def setUp(self) -> None:
        self.runtime = ServerlessRuntime(
            simulated_cold_start_delay_sec=0.02,
            container_idle_ttl_sec=2.0,
        )

    def test_serverless_unsupported_handler_rejected(self) -> None:
        """Verify that non-pickleable local closures are rejected with UnsupportedHandlerError."""
        captured_value = 100

        def local_closure(event: Dict[str, Any], context: FunctionContext) -> Dict[str, Any]:
            return {"val": captured_value}

        with self.assertRaises(UnsupportedHandlerError) as ctx:
            self.runtime.register_function("unpickleable_fn", local_closure)
        self.assertIn("unsupported", str(ctx.exception))
        self.assertIn("cannot be pickled across OS process boundaries", str(ctx.exception))

    def test_serverless_explicit_thread_isolation_opt_in(self) -> None:
        """Verify that caller can explicitly opt into thread isolation without false process termination claims."""
        captured_value = 42

        def local_closure(event: Dict[str, Any], context: FunctionContext) -> Dict[str, Any]:
            return {"val": captured_value}

        # Explicitly opt out of process isolation
        self.runtime.register_function("thread_func", local_closure, require_process_isolation=False)
        res = self.runtime.invoke("thread_func", {})
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.payload["val"], 42)

    def test_serverless_cold_start_vs_warm_reuse(self) -> None:
        """Verify first invocation incurs cold start and subsequent invocation reuses warm container."""
        self.runtime.register_function("echo_func", echo_handler)

        # 1. Cold start
        res1 = self.runtime.invoke("echo_func", {"msg": "hello"})
        self.assertEqual(res1.status_code, 200)
        self.assertTrue(res1.cold_start)
        self.assertEqual(res1.payload["echo"], "hello")

        # 2. Warm reuse
        res2 = self.runtime.invoke("echo_func", {"msg": "world"})
        self.assertEqual(res2.status_code, 200)
        self.assertFalse(res2.cold_start)
        self.assertEqual(res2.payload["echo"], "world")

    def test_serverless_execution_timeout(self) -> None:
        """Verify function exceeding execution timeout is terminated and returns HTTP 504."""
        self.runtime.register_function("slow_func", slow_timeout_handler)
        res = self.runtime.invoke("slow_func", {"sleep_sec": 0.25}, timeout_sec=0.04)

        self.assertEqual(res.status_code, 504)
        self.assertIn("timed out", res.error_message)
        self.assertIn("process terminated", res.error_message)

    def test_serverless_child_process_cleanup_on_timeout(self) -> None:
        """Verify that timed-out process is killed, evicted, and leaves no zombie container."""
        self.runtime.register_function("slow_clean_func", slow_timeout_handler)
        res = self.runtime.invoke("slow_clean_func", {"sleep_sec": 0.3}, timeout_sec=0.04)
        self.assertEqual(res.status_code, 504)

        # Degraded container must be evicted from active pool
        stats = self.runtime.get_container_stats("slow_clean_func")
        self.assertEqual(stats["warm_container_count"], 0)

    def test_serverless_idempotency_deduplication(self) -> None:
        """Verify identical idempotency key returns cached result on retry replay without re-running handler."""
        with tempfile.TemporaryDirectory() as tmpdir:
            counter_file = os.path.join(tmpdir, "idemp_calls.txt")
            self.runtime.register_function("idemp_func", idempotent_test_handler)

            key = "idemp-key-xyz-100"
            # First call
            res1 = self.runtime.invoke(
                "idemp_func",
                {"counter_file": counter_file, "token": "abc"},
                idempotency_key=key,
            )
            self.assertEqual(res1.status_code, 200)
            self.assertEqual(res1.payload["token"], "abc")
            self.assertFalse(res1.cached_idempotent)

            # Duplicate replay call
            res2 = self.runtime.invoke(
                "idemp_func",
                {"counter_file": counter_file, "token": "abc"},
                idempotency_key=key,
            )
            self.assertEqual(res2.status_code, 200)
            self.assertEqual(res2.payload["token"], "abc")
            self.assertTrue(res2.cached_idempotent)

            # Handler executed only once (file has exactly one entry)
            with open(counter_file, "r", encoding="utf-8") as f:
                lines = f.readlines()
            self.assertEqual(len(lines), 1)

    def test_serverless_handler_exception_handling(self) -> None:
        """Verify exception raised inside handler across process boundaries returns structured HTTP 500."""
        self.runtime.register_function("faulty_func", failing_handler)
        res = self.runtime.invoke("faulty_func", {})

        self.assertEqual(res.status_code, 500)
        self.assertIn("ValueError", res.error_message)

    def test_serverless_unregistered_function_returns_404(self) -> None:
        """Verify invoking unregistered function returns HTTP 404."""
        res = self.runtime.invoke("nonexistent_func", {})
        self.assertEqual(res.status_code, 404)
        self.assertIn("not registered", res.error_message)

    def test_serverless_timeout_terminates_child_process_preventing_side_effects(self) -> None:
        """Verify timed-out execution is terminated at the OS process level and side effects never execute."""
        with tempfile.TemporaryDirectory() as tmpdir:
            marker_file = os.path.join(tmpdir, "isolated_side_effect.txt")
            self.runtime.register_function("delayed_side_effect", delayed_side_effect_handler)

            # Handler sleeps for 0.4s before writing marker_file; timeout is 0.08s
            res = self.runtime.invoke(
                "delayed_side_effect",
                {"marker_file": marker_file, "sleep_sec": 0.4},
                timeout_sec=0.08,
            )

            self.assertEqual(res.status_code, 504)
            self.assertIn("timed out", res.error_message)

            # Allow enough time (0.5s) for a surviving background thread or process to have completed
            time.sleep(0.5)

            # Assert that the side effect file was NEVER written because the child process was terminated
            self.assertFalse(
                os.path.exists(marker_file),
                "Timed-out handler continued executing in the background and wrote marker file!",
            )


if __name__ == "__main__":
    unittest.main()
