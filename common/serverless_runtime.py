"""
Serverless Computing Runtime Simulation (Unit IV - Emerging Paradigms).

This module implements an educational event-driven serverless function execution
environment (FaaS - Function-as-a-Service) simulating:
1. Event invocation model: accepts structured event payloads with an invocation context.
2. Cold Start vs. Warm Container lifecycle: simulates runtime container spinning
   and reuse across repeated invocations.
3. Strict execution timeout enforcement: uses process-level isolation (`multiprocessing.Process`)
   to genuinely terminate timed-out executions, preventing background zombie execution on Windows.
4. Stateless execution invariant: functions do not retain in-memory state;
   external state stores are required for durability.
5. Idempotent event processing: deduplication cache handling at-least-once retry storms.

Contrast with Continuous Background Workers:
- Continuous Worker (this repository's worker/consumer.py):
    * Long-lived operating system process.
    * Persistent Redis/DB connections.
    * Always-on infrastructure costs regardless of workload volume.
    * Suitable for arbitrary-length batch jobs (hours).
- Serverless Function (this module):
    * Scale-to-zero when idle (zero idle cost).
    * Ephemeral micro-VM / container execution context.
    * Cold-start overhead on burst or initialization.
    * Strict execution timeout cap (e.g., 15 minutes in AWS Lambda).
    * Stateless handler requiring external stores (Redis/DynamoDB/S3).
    * Terminated immediately on timeout cutoff via process termination.
"""

from __future__ import annotations

import logging
import multiprocessing as mp
import pickle
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger("serverless_runtime")


@dataclass
class FunctionContext:
    """
    Invocation context passed to serverless handler functions.
    Mirrors AWS Lambda Context / Google Cloud Functions Context.
    """
    function_name: str
    invocation_id: str
    execution_timeout_sec: float
    cold_start: bool
    memory_mb: int = 128
    created_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "function_name": self.function_name,
            "invocation_id": self.invocation_id,
            "execution_timeout_sec": self.execution_timeout_sec,
            "cold_start": self.cold_start,
            "memory_mb": self.memory_mb,
            "created_at": self.created_at,
        }


@dataclass
class FunctionResult:
    """
    Structured outcome of a serverless invocation.
    """
    status_code: int
    payload: Dict[str, Any]
    execution_duration_ms: float
    cold_start: bool
    invocation_id: str
    error_message: Optional[str] = None
    cached_idempotent: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status_code": self.status_code,
            "payload": self.payload,
            "execution_duration_ms": round(self.execution_duration_ms, 2),
            "cold_start": self.cold_start,
            "invocation_id": self.invocation_id,
            "error_message": self.error_message,
            "cached_idempotent": self.cached_idempotent,
        }


class UnsupportedHandlerError(ValueError):
    """
    Raised when a serverless handler cannot be safely executed with
    terminable process isolation (e.g., non-pickleable local closure on Windows).
    """
    pass


def _is_pickleable(fn: Any) -> bool:
    """Test whether a callable can be serialized across OS process boundaries."""
    try:
        pickle.dumps(fn)
        return True
    except Exception:
        return False


# Type alias for serverless function handlers: handler(event, context) -> dict
ServerlessHandler = Callable[[Dict[str, Any], FunctionContext], Dict[str, Any]]


def _serverless_process_runner(
    handler_fn: Any,
    event: Dict[str, Any],
    context_dict: Dict[str, Any],
    out_queue: Any,
) -> None:
    """
    Module-level entrypoint executed inside an isolated child process.
    Enables true OS process termination on timeout cutoff.
    """
    try:
        context = FunctionContext(**context_dict)
        res = handler_fn(event, context)
        out_queue.put({"success": True, "result": res, "error_type": None, "error_msg": None})
    except Exception as exc:
        out_queue.put({
            "success": False,
            "result": None,
            "error_type": type(exc).__name__,
            "error_msg": str(exc),
        })


def echo_handler(event: Dict[str, Any], context: FunctionContext) -> Dict[str, Any]:
    """Stateless echo handler returning payload and cold-start metadata."""
    return {"echo": event.get("msg"), "cold": context.cold_start}


def slow_timeout_handler(event: Dict[str, Any], context: FunctionContext) -> Dict[str, Any]:
    """Slow handler that sleeps beyond timeout limits."""
    sleep_sec = float(event.get("sleep_sec", 0.3))
    time.sleep(sleep_sec)
    return {"status": "ok", "slept": sleep_sec}


def failing_handler(event: Dict[str, Any], context: FunctionContext) -> Dict[str, Any]:
    """Faulty handler that raises ValueError."""
    raise ValueError("Invalid numerical argument")


def idempotent_test_handler(event: Dict[str, Any], context: FunctionContext) -> Dict[str, Any]:
    """Idempotency test handler tracking execution counts via marker file if provided."""
    counter_file = event.get("counter_file")
    if counter_file:
        with open(counter_file, "a", encoding="utf-8") as f:
            f.write("invoked\n")
    return {"token": event.get("token", "default_val")}


def delayed_side_effect_handler(event: Dict[str, Any], context: FunctionContext) -> Dict[str, Any]:
    """
    Test helper handler: sleeps before performing a disk side effect.
    Used to prove that timed-out process termination genuinely halts side effects.
    """
    marker_file = event.get("marker_file")
    sleep_sec = float(event.get("sleep_sec", 0.4))
    time.sleep(sleep_sec)
    if marker_file:
        with open(marker_file, "w", encoding="utf-8") as f:
            f.write("EXECUTED_SIDE_EFFECT")
    return {"done": True}


class ServerlessContainer:
    """
    Simulates an isolated execution container / micro-VM (e.g., Firecracker / gVisor).
    """

    def __init__(self, function_name: str, memory_mb: int = 128) -> None:
        self.container_id = f"ctr-{uuid.uuid4().hex[:8]}"
        self.function_name = function_name
        self.memory_mb = memory_mb
        self.created_at = time.time()
        self.last_used_at = time.time()
        self.invocation_count = 0
        self.is_active = True

    def mark_used(self) -> None:
        self.last_used_at = time.time()
        self.invocation_count += 1


class ServerlessRuntime:
    """
    Local educational runtime orchestrator for serverless functions.
    
    Provides:
    - Function registration and invocation.
    - Cold-start container provisioning (with configurable latency simulation).
    - Warm-container reuse.
    - Container reclamation / idle pruning.
    - Process-level execution timeout enforcement with genuine termination.
    - Idempotency key tracking for safe retries.
    """

    def __init__(
        self,
        simulated_cold_start_delay_sec: float = 0.05,
        container_idle_ttl_sec: float = 5.0,
    ) -> None:
        self.simulated_cold_start_delay_sec = simulated_cold_start_delay_sec
        self.container_idle_ttl_sec = container_idle_ttl_sec
        self._registry: Dict[str, ServerlessHandler] = {}
        self._isolation_modes: Dict[str, bool] = {}
        # Warm container pool: function_name -> list of ServerlessContainer
        self._warm_containers: Dict[str, list[ServerlessContainer]] = {}
        # Idempotency storage: idempotency_key -> FunctionResult
        self._idempotency_store: Dict[str, FunctionResult] = {}
        self._lock = threading.Lock()

    def register_function(
        self,
        name: str,
        handler: ServerlessHandler,
        require_process_isolation: bool = True,
    ) -> None:
        """
        Register a stateless handler function with the runtime.

        Args:
            name: Identifier for the serverless function.
            handler: Callable taking (event: Dict[str, Any], context: FunctionContext) -> Dict[str, Any].
            require_process_isolation: If True (default), validates that the handler is pickleable
                across OS process boundaries on Windows so execution timeouts can be strictly enforced
                by terminating the child process. If False, allows running in a thread with explicit
                acknowledgement that timeouts cannot terminate the thread.
        Raises:
            UnsupportedHandlerError: If require_process_isolation is True and the handler cannot be
                pickled for Windows multiprocessing execution.
        """
        if require_process_isolation and not _is_pickleable(handler):
            raise UnsupportedHandlerError(
                f"Handler '{name}' is unsupported: function '{getattr(handler, '__qualname__', str(handler))}' "
                f"cannot be pickled across OS process boundaries on Windows. "
                f"To guarantee strict timeout enforcement and prevent background zombie execution, "
                f"handlers must be module-level pickleable callables. (Pass "
                f"require_process_isolation=False to explicitly opt into non-terminable thread execution)."
            )

        with self._lock:
            self._registry[name] = handler
            self._isolation_modes[name] = require_process_isolation
            if name not in self._warm_containers:
                self._warm_containers[name] = []
        logger.info(
            "Registered serverless function '%s' (process_isolation=%s)",
            name,
            require_process_isolation,
        )

    def invoke(
        self,
        function_name: str,
        event: Dict[str, Any],
        timeout_sec: float = 1.0,
        idempotency_key: Optional[str] = None,
        force_cold_start: bool = False,
    ) -> FunctionResult:
        """
        Invoke a registered serverless function synchronously.
        
        Args:
            function_name: Target registered function.
            event: Event payload dictionary.
            timeout_sec: Strict execution timeout limit.
            idempotency_key: Optional key to deduplicate retries.
            force_cold_start: If True, forces container eviction to trigger cold start.
        """
        # 1. Idempotency Check
        if idempotency_key:
            with self._lock:
                if idempotency_key in self._idempotency_store:
                    cached = self._idempotency_store[idempotency_key]
                    logger.info(
                        f"Idempotency hit for key '{idempotency_key}': returning cached result"
                    )
                    return FunctionResult(
                        status_code=cached.status_code,
                        payload=cached.payload,
                        execution_duration_ms=0.0,
                        cold_start=False,
                        invocation_id=cached.invocation_id,
                        error_message=cached.error_message,
                        cached_idempotent=True,
                    )

        # 2. Handler lookup
        with self._lock:
            handler = self._registry.get(function_name)
            if not handler:
                return FunctionResult(
                    status_code=404,
                    payload={},
                    execution_duration_ms=0.0,
                    cold_start=False,
                    invocation_id=f"inv-{uuid.uuid4().hex[:8]}",
                    error_message=f"Function '{function_name}' not registered in serverless runtime.",
                )

        invocation_id = f"inv-{uuid.uuid4().hex[:8]}"

        # 3. Container allocation (Cold vs. Warm start)
        is_cold = False
        container: Optional[ServerlessContainer] = None

        with self._lock:
            # Prune expired idle containers first
            now = time.time()
            active_pool = [
                c for c in self._warm_containers.get(function_name, [])
                if (now - c.last_used_at) <= self.container_idle_ttl_sec
            ]
            self._warm_containers[function_name] = active_pool

            if force_cold_start or not active_pool:
                is_cold = True
                container = ServerlessContainer(function_name=function_name)
                self._warm_containers[function_name].append(container)
            else:
                is_cold = False
                container = active_pool[0]
                container.mark_used()

        # Simulate cold-start container initialization overhead
        if is_cold and self.simulated_cold_start_delay_sec > 0:
            time.sleep(self.simulated_cold_start_delay_sec)

        context = FunctionContext(
            function_name=function_name,
            invocation_id=invocation_id,
            execution_timeout_sec=timeout_sec,
            cold_start=is_cold,
        )

        # 4. Determine execution isolation model
        use_process_isolation = self._isolation_modes.get(function_name, True)
        start_time = time.perf_counter()

        if use_process_isolation:
            # TRUE PROCESS ISOLATION: child process can be terminated on timeout
            out_queue = mp.Queue()
            proc = mp.Process(
                target=_serverless_process_runner,
                args=(handler, event, context.to_dict(), out_queue),
                name=f"faas-worker-{invocation_id}",
            )
            proc.start()
            proc.join(timeout=timeout_sec)
            elapsed_ms = (time.perf_counter() - start_time) * 1000.0

            if proc.is_alive():
                # TIMEOUT ENFORCEMENT: Terminate and kill child process to guarantee no zombie code runs
                error_msg = (
                    f"Task timed out after {timeout_sec:.2f} seconds "
                    f"(execution limit exceeded, process terminated)"
                )
                logger.error(f"[{invocation_id}] {error_msg}")
                try:
                    proc.terminate()
                    proc.join(timeout=0.2)
                    if proc.is_alive():
                        proc.kill()
                        proc.join(timeout=0.2)
                except Exception as term_err:
                    logger.warning(f"Error terminating worker process: {term_err}")
                finally:
                    out_queue.close()
                    out_queue.cancel_join_thread()

                # Evict degraded container
                with self._lock:
                    if container and container in self._warm_containers.get(function_name, []):
                        self._warm_containers[function_name].remove(container)

                result = FunctionResult(
                    status_code=504,
                    payload={},
                    execution_duration_ms=elapsed_ms,
                    cold_start=is_cold,
                    invocation_id=invocation_id,
                    error_message=error_msg,
                )
            else:
                # Process completed normally within timeout
                try:
                    if not out_queue.empty():
                        payload_data = out_queue.get(timeout=0.2)
                        out_queue.close()
                        out_queue.join_thread()
                        if payload_data.get("success"):
                            result = FunctionResult(
                                status_code=200,
                                payload=payload_data.get("result") or {},
                                execution_duration_ms=elapsed_ms,
                                cold_start=is_cold,
                                invocation_id=invocation_id,
                                error_message=None,
                            )
                        else:
                            err_type = payload_data.get("error_type", "RuntimeError")
                            err_msg = payload_data.get("error_msg", "Unknown error")
                            logger.warning(f"[{invocation_id}] Handler raised exception: {err_type}: {err_msg}")
                            result = FunctionResult(
                                status_code=500,
                                payload={},
                                execution_duration_ms=elapsed_ms,
                                cold_start=is_cold,
                                invocation_id=invocation_id,
                                error_message=f"{err_type}: {err_msg}",
                            )
                    else:
                        out_queue.close()
                        out_queue.cancel_join_thread()
                        result = FunctionResult(
                            status_code=500,
                            payload={},
                            execution_duration_ms=elapsed_ms,
                            cold_start=is_cold,
                            invocation_id=invocation_id,
                            error_message="Process terminated prematurely without returning output.",
                        )
                except Exception as read_exc:
                    result = FunctionResult(
                        status_code=500,
                        payload={},
                        execution_duration_ms=elapsed_ms,
                        cold_start=is_cold,
                        invocation_id=invocation_id,
                        error_message=f"Failed to read process output: {read_exc}",
                    )
        else:
            # EXPLICIT OPT-IN THREAD ISOLATION:
            # Documented limitation: threads cannot be preempted or killed on timeout in Python
            logger.warning(
                f"[{invocation_id}] Function '{function_name}' executing with explicit opt-in thread isolation; "
                f"timeouts cannot preempt or terminate the underlying thread."
            )
            execution_container: Dict[str, Any] = {
                "result": None,
                "error": None,
                "completed": False,
            }

            def worker_target() -> None:
                try:
                    res = handler(event, context)
                    execution_container["result"] = res
                    execution_container["completed"] = True
                except Exception as exc:
                    execution_container["error"] = exc
                    execution_container["completed"] = True

            exec_thread = threading.Thread(target=worker_target, daemon=True)
            exec_thread.start()
            exec_thread.join(timeout=timeout_sec)
            elapsed_ms = (time.perf_counter() - start_time) * 1000.0

            if exec_thread.is_alive() and not execution_container["completed"]:
                error_msg = (
                    f"Task timed out after {timeout_sec:.2f} seconds "
                    f"(execution limit exceeded, thread execution timed out without process termination guarantee)"
                )
                logger.error(f"[{invocation_id}] {error_msg}")
                with self._lock:
                    if container and container in self._warm_containers.get(function_name, []):
                        self._warm_containers[function_name].remove(container)

                result = FunctionResult(
                    status_code=504,
                    payload={},
                    execution_duration_ms=elapsed_ms,
                    cold_start=is_cold,
                    invocation_id=invocation_id,
                    error_message=error_msg,
                )
            elif execution_container["error"]:
                err = execution_container["error"]
                logger.warning(f"[{invocation_id}] Handler raised exception: {err}")
                result = FunctionResult(
                    status_code=500,
                    payload={},
                    execution_duration_ms=elapsed_ms,
                    cold_start=is_cold,
                    invocation_id=invocation_id,
                    error_message=f"{type(err).__name__}: {str(err)}",
                )
            else:
                result = FunctionResult(
                    status_code=200,
                    payload=execution_container["result"] or {},
                    execution_duration_ms=elapsed_ms,
                    cold_start=is_cold,
                    invocation_id=invocation_id,
                    error_message=None,
                )

        # Store in idempotency cache if key provided and invocation succeeded
        if idempotency_key and result.status_code == 200:
            with self._lock:
                self._idempotency_store[idempotency_key] = result

        return result

    def get_container_stats(self, function_name: str) -> Dict[str, Any]:
        """Return diagnostic metrics on warm containers."""
        with self._lock:
            containers = self._warm_containers.get(function_name, [])
            return {
                "function_name": function_name,
                "warm_container_count": len(containers),
                "containers": [
                    {
                        "container_id": c.container_id,
                        "invocation_count": c.invocation_count,
                        "idle_seconds": round(time.time() - c.last_used_at, 2),
                    }
                    for c in containers
                ],
            }

    def clear_containers(self) -> None:
        """Evict all warm containers (forcing cold starts for next calls)."""
        with self._lock:
            self._warm_containers.clear()
            self._idempotency_store.clear()
