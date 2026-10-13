"""
Serverless Computing Runtime Service (Unit IV - Emerging Paradigms).

This module implements an event-driven serverless function execution service
(FaaS - Function-as-a-Service) featuring:
1. Event invocation model: accepts structured event payloads with an execution context.
2. Explicit function allowlist registry: prevents unregistered or private execution.
3. Input validation: schema/key validation rejecting malformed invocations (HTTP 400).
4. Strict execution timeout enforcement: uses process-level isolation (`multiprocessing.Process`)
   to genuinely terminate timed-out executions, preventing background zombie execution on Windows.
5. Persistent invocation auditing: durable records (ID, status, timestamps, duration, error,
   result metadata) persisted to SQLite for traceability across restarts.
6. Durable idempotency: deduplication store surviving worker/runtime restarts.
7. Cold Start vs. Warm Container lifecycle: simulates runtime container spinning
   and reuse across repeated invocations.
8. Standalone HTTP service: exposes `/invoke`, `/invocations/<id>`, `/health`, `/stats`.

=============================================================================
DOCUMENTED RESOURCE LIMITS & GUARANTEES:
=============================================================================
- Wall-Clock Execution Timeout: STRICTLY ENFORCED. Every function execution with process
  isolation is wrapped in an OS child process (`multiprocessing.Process`). Upon reaching
  `timeout_sec`, `proc.terminate()` followed by `proc.kill()` is unconditionally executed.
  This guarantees that child processes cannot continue running in the background or emit
  unauthorized side effects.
- Memory Limits (RSS/VMS): DOCUMENTED LIMITATION. On raw Windows operating systems without
  Linux cgroups or Docker containerization, OS-level hard memory caps cannot be enforced
  natively by standard Python `multiprocessing`. The `memory_mb` field in `FunctionContext`
  is maintained as metadata for billing/capacity simulation but is not hardware-capped.
- CPU Bandwidth / CFS Quotas: DOCUMENTED LIMITATION. CPU throttling is not enforced at the
  OS process level on bare Windows. Full resource isolation requires Docker / container runtimes.
- Thread Fallback Policy: By default, non-pickleable callables raise `UnsupportedHandlerError`.
  The runtime NEVER silently degrades into unkillable threads. Thread execution is only permitted
  if the caller explicitly passes `require_process_isolation=False`, with explicit logging
  acknowledging that timeouts cannot terminate threads.
"""

from __future__ import annotations

import argparse
import json
import logging
import multiprocessing as mp
import os
import pickle
import sqlite3
import tempfile
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger("serverless_runtime")


# ===========================================================================
# Data Models & Result Containers
# ===========================================================================
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
    """Structured outcome of a serverless invocation."""
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


@dataclass
class InvocationRecord:
    """Authoritative persistent record of a function invocation."""
    invocation_id: str
    function_name: str
    status: str  # SUCCESS, TIMEOUT, FAILED, VALIDATION_ERROR, NOT_REGISTERED
    start_time_iso: str
    end_time_iso: str
    duration_ms: float
    timeout_sec: float
    error_type: Optional[str]
    error_message: Optional[str]
    result_metadata: Dict[str, Any]
    idempotency_key: Optional[str]
    created_at_iso: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "invocation_id": self.invocation_id,
            "function_name": self.function_name,
            "status": self.status,
            "start_time_iso": self.start_time_iso,
            "end_time_iso": self.end_time_iso,
            "duration_ms": round(self.duration_ms, 2),
            "timeout_sec": self.timeout_sec,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "result_metadata": self.result_metadata,
            "idempotency_key": self.idempotency_key,
            "created_at_iso": self.created_at_iso,
        }


class UnsupportedHandlerError(ValueError):
    """
    Raised when a serverless handler cannot be safely executed with
    terminable process isolation (e.g., non-pickleable local closure on Windows).
    """
    pass


# Type aliases
ServerlessHandler = Callable[[Dict[str, Any], FunctionContext], Dict[str, Any]]
InputValidator = Callable[[Dict[str, Any]], Tuple[bool, Optional[str]]]


def _is_pickleable(fn: Any) -> bool:
    """Test whether a callable can be serialized across OS process boundaries."""
    try:
        pickle.dumps(fn)
        return True
    except Exception:
        return False


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


# ===========================================================================
# Standard Built-in Handlers for Testing & Verification
# ===========================================================================
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


def validated_math_handler(event: Dict[str, Any], context: FunctionContext) -> Dict[str, Any]:
    """Stateless math calculation handler with explicit schema validation."""
    op = event.get("operation")
    x = float(event.get("x", 0))
    y = float(event.get("y", 0))
    if op == "add":
        return {"result": x + y, "operation": op}
    elif op == "multiply":
        return {"result": x * y, "operation": op}
    raise ValueError(f"Unsupported math operation '{op}'")


# ===========================================================================
# Persistent Storage for Invocations and Idempotency
# ===========================================================================
class PersistentInvocationStore:
    """Durable SQLite storage engine for function invocations and idempotency records."""

    def __init__(self, db_path: Optional[Path | str] = None) -> None:
        if db_path is not None:
            self.db_path = Path(db_path)
        else:
            self.db_path = Path(tempfile.gettempdir()) / f"serverless_{uuid.uuid4().hex[:8]}.db"
        self._lock = threading.Lock()
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=10.0)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._lock, self._get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS serverless_invocations (
                    invocation_id TEXT PRIMARY KEY,
                    function_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    start_time_iso TEXT NOT NULL,
                    end_time_iso TEXT NOT NULL,
                    duration_ms REAL NOT NULL,
                    timeout_sec REAL NOT NULL,
                    error_type TEXT,
                    error_message TEXT,
                    result_metadata_json TEXT,
                    idempotency_key TEXT,
                    created_at_iso TEXT NOT NULL
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_inv_fn ON serverless_invocations(function_name)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_inv_idemp ON serverless_invocations(idempotency_key)")

            conn.execute("""
                CREATE TABLE IF NOT EXISTS idempotency_records (
                    idempotency_key TEXT PRIMARY KEY,
                    invocation_id TEXT NOT NULL,
                    function_name TEXT NOT NULL,
                    status_code INTEGER NOT NULL,
                    payload_json TEXT NOT NULL,
                    error_message TEXT,
                    created_at_iso TEXT NOT NULL
                )
            """)
            conn.commit()

    def save_invocation(self, record: InvocationRecord) -> None:
        with self._lock, self._get_connection() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO serverless_invocations (
                    invocation_id, function_name, status, start_time_iso, end_time_iso,
                    duration_ms, timeout_sec, error_type, error_message, result_metadata_json,
                    idempotency_key, created_at_iso
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.invocation_id,
                    record.function_name,
                    record.status,
                    record.start_time_iso,
                    record.end_time_iso,
                    record.duration_ms,
                    record.timeout_sec,
                    record.error_type,
                    record.error_message,
                    json.dumps(record.result_metadata),
                    record.idempotency_key,
                    record.created_at_iso,
                ),
            )
            conn.commit()

    def get_invocation(self, invocation_id: str) -> Optional[Dict[str, Any]]:
        with self._lock, self._get_connection() as conn:
            cur = conn.execute("SELECT * FROM serverless_invocations WHERE invocation_id = ?", (invocation_id,))
            row = cur.fetchone()
            if not row:
                return None
            d = dict(row)
            d["result_metadata"] = json.loads(d.pop("result_metadata_json", "{}") or "{}")
            return d

    def list_invocations(self, function_name: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock, self._get_connection() as conn:
            if function_name:
                cur = conn.execute(
                    "SELECT * FROM serverless_invocations WHERE function_name = ? ORDER BY created_at_iso DESC LIMIT ?",
                    (function_name, limit),
                )
            else:
                cur = conn.execute(
                    "SELECT * FROM serverless_invocations ORDER BY created_at_iso DESC LIMIT ?",
                    (limit,),
                )
            results = []
            for row in cur.fetchall():
                d = dict(row)
                d["result_metadata"] = json.loads(d.pop("result_metadata_json", "{}") or "{}")
                results.append(d)
            return results

    def save_idempotency(
        self,
        key: str,
        invocation_id: str,
        function_name: str,
        status_code: int,
        payload: Dict[str, Any],
        error_message: Optional[str] = None,
    ) -> None:
        now_iso = datetime.now(timezone.utc).isoformat()
        with self._lock, self._get_connection() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO idempotency_records (
                    idempotency_key, invocation_id, function_name, status_code, payload_json, error_message, created_at_iso
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    key,
                    invocation_id,
                    function_name,
                    status_code,
                    json.dumps(payload),
                    error_message,
                    now_iso,
                ),
            )
            conn.commit()

    def get_idempotency(self, key: str) -> Optional[Dict[str, Any]]:
        with self._lock, self._get_connection() as conn:
            cur = conn.execute("SELECT * FROM idempotency_records WHERE idempotency_key = ?", (key,))
            row = cur.fetchone()
            if not row:
                return None
            d = dict(row)
            d["payload"] = json.loads(d.pop("payload_json", "{}") or "{}")
            return d

    def clear(self) -> None:
        with self._lock, self._get_connection() as conn:
            conn.execute("DELETE FROM serverless_invocations")
            conn.execute("DELETE FROM idempotency_records")
            conn.commit()


# ===========================================================================
# Execution Container / Micro-VM Abstraction
# ===========================================================================
class ServerlessContainer:
    """Simulates an isolated execution container / micro-VM (e.g., Firecracker / gVisor)."""

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


# ===========================================================================
# Core Serverless Runtime
# ===========================================================================
class ServerlessRuntime:
    """
    Local runtime orchestrator for serverless functions.
    
    Provides:
    - Explicit function registration & input validation schemas.
    - Cold-start container provisioning (with configurable latency simulation).
    - Warm-container reuse and idle container reclamation.
    - Process-level execution timeout enforcement with genuine termination.
    - Durable invocation persistence and idempotency tracking across restarts.
    """

    def __init__(
        self,
        simulated_cold_start_delay_sec: float = 0.05,
        container_idle_ttl_sec: float = 5.0,
        persistence_db_path: Optional[Path | str] = None,
    ) -> None:
        self.simulated_cold_start_delay_sec = simulated_cold_start_delay_sec
        self.container_idle_ttl_sec = container_idle_ttl_sec
        self._registry: Dict[str, ServerlessHandler] = {}
        self._isolation_modes: Dict[str, bool] = {}
        self._validators: Dict[str, Optional[InputValidator]] = {}
        # Warm container pool: function_name -> list of ServerlessContainer
        self._warm_containers: Dict[str, list[ServerlessContainer]] = {}
        # In-memory idempotency storage: idempotency_key -> FunctionResult
        self._idempotency_store: Dict[str, FunctionResult] = {}
        self._lock = threading.Lock()
        self.store = PersistentInvocationStore(persistence_db_path)

    def register_function(
        self,
        name: str,
        handler: ServerlessHandler,
        require_process_isolation: bool = True,
        input_validator: Optional[InputValidator] = None,
        required_keys: Optional[List[str]] = None,
    ) -> None:
        """
        Register a stateless handler function with the runtime.

        Args:
            name: Identifier for the serverless function.
            handler: Callable taking (event: Dict[str, Any], context: FunctionContext) -> Dict[str, Any].
            require_process_isolation: If True (default), validates that the handler is pickleable
                across OS process boundaries on Windows so execution timeouts can be strictly enforced
                by terminating the child process.
            input_validator: Optional custom validator callback returning (is_valid, error_message).
            required_keys: Optional list of mandatory keys in event payload.
        """
        if require_process_isolation and not _is_pickleable(handler):
            raise UnsupportedHandlerError(
                f"Handler '{name}' is unsupported: function '{getattr(handler, '__qualname__', str(handler))}' "
                f"cannot be pickled across OS process boundaries on Windows. "
                f"To guarantee strict timeout enforcement and prevent background zombie execution, "
                f"handlers must be module-level pickleable callables. (Pass "
                f"require_process_isolation=False to explicitly opt into non-terminable thread execution)."
            )

        # Build key validator if required_keys provided
        effective_validator = input_validator
        if required_keys is not None and effective_validator is None:
            def _key_val(event: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
                missing = [k for k in required_keys if k not in event]
                if missing:
                    return False, f"Missing required payload key(s): {', '.join(missing)}"
                return True, None
            effective_validator = _key_val

        with self._lock:
            self._registry[name] = handler
            self._isolation_modes[name] = require_process_isolation
            self._validators[name] = effective_validator
            if name not in self._warm_containers:
                self._warm_containers[name] = []
        logger.info(
            "Registered serverless function '%s' (process_isolation=%s, validator=%s)",
            name,
            require_process_isolation,
            bool(effective_validator),
        )

    def invoke(
        self,
        function_name: str,
        event: Dict[str, Any],
        timeout_sec: float = 5.0,
        idempotency_key: Optional[str] = None,
        force_cold_start: bool = False,
    ) -> FunctionResult:
        """
        Invoke a registered serverless function synchronously.
        """
        invocation_id = f"inv-{uuid.uuid4().hex[:8]}"
        start_time_iso = datetime.now(timezone.utc).isoformat()
        start_perf = time.perf_counter()

        # 1. Idempotency Check (In-memory + Durable SQLite)
        if idempotency_key:
            with self._lock:
                if idempotency_key in self._idempotency_store:
                    cached = self._idempotency_store[idempotency_key]
                    logger.info("In-memory idempotency hit for key '%s'", idempotency_key)
                    return FunctionResult(
                        status_code=cached.status_code,
                        payload=cached.payload,
                        execution_duration_ms=0.0,
                        cold_start=False,
                        invocation_id=cached.invocation_id,
                        error_message=cached.error_message,
                        cached_idempotent=True,
                    )

            # Check durable SQLite idempotency store across restarts
            durable_idemp = self.store.get_idempotency(idempotency_key)
            if durable_idemp:
                logger.info("Durable SQLite idempotency hit for key '%s'", idempotency_key)
                return FunctionResult(
                    status_code=durable_idemp["status_code"],
                    payload=durable_idemp["payload"],
                    execution_duration_ms=0.0,
                    cold_start=False,
                    invocation_id=durable_idemp["invocation_id"],
                    error_message=durable_idemp.get("error_message"),
                    cached_idempotent=True,
                )

        # 2. Handler Lookup & Allowlist Verification
        with self._lock:
            handler = self._registry.get(function_name)
            validator = self._validators.get(function_name)

        if not handler:
            err_msg = f"Function '{function_name}' not registered in serverless runtime."
            end_time_iso = datetime.now(timezone.utc).isoformat()
            self.store.save_invocation(
                InvocationRecord(
                    invocation_id=invocation_id,
                    function_name=function_name,
                    status="NOT_REGISTERED",
                    start_time_iso=start_time_iso,
                    end_time_iso=end_time_iso,
                    duration_ms=0.0,
                    timeout_sec=timeout_sec,
                    error_type="FunctionNotFoundError",
                    error_message=err_msg,
                    result_metadata={"requested_name": function_name},
                    idempotency_key=idempotency_key,
                    created_at_iso=start_time_iso,
                )
            )
            return FunctionResult(
                status_code=404,
                payload={},
                execution_duration_ms=0.0,
                cold_start=False,
                invocation_id=invocation_id,
                error_message=err_msg,
            )

        # 3. Input Validation
        if validator is not None:
            is_valid, val_err = validator(event)
            if not is_valid:
                err_msg = f"Input validation failed: {val_err}"
                end_time_iso = datetime.now(timezone.utc).isoformat()
                self.store.save_invocation(
                    InvocationRecord(
                        invocation_id=invocation_id,
                        function_name=function_name,
                        status="VALIDATION_ERROR",
                        start_time_iso=start_time_iso,
                        end_time_iso=end_time_iso,
                        duration_ms=0.0,
                        timeout_sec=timeout_sec,
                        error_type="ValidationError",
                        error_message=err_msg,
                        result_metadata={"payload_keys": list(event.keys())},
                        idempotency_key=idempotency_key,
                        created_at_iso=start_time_iso,
                    )
                )
                return FunctionResult(
                    status_code=400,
                    payload={},
                    execution_duration_ms=0.0,
                    cold_start=False,
                    invocation_id=invocation_id,
                    error_message=err_msg,
                )

        # 4. Container Allocation (Cold vs Warm start)
        is_cold = False
        container: Optional[ServerlessContainer] = None

        with self._lock:
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

        # Simulate cold-start initialization
        if is_cold and self.simulated_cold_start_delay_sec > 0:
            time.sleep(self.simulated_cold_start_delay_sec)

        context = FunctionContext(
            function_name=function_name,
            invocation_id=invocation_id,
            execution_timeout_sec=timeout_sec,
            cold_start=is_cold,
        )

        # 5. Execution Isolation Model
        use_process_isolation = self._isolation_modes.get(function_name, True)

        if use_process_isolation:
            out_queue = mp.Queue()
            proc = mp.Process(
                target=_serverless_process_runner,
                args=(handler, event, context.to_dict(), out_queue),
                name=f"faas-worker-{invocation_id}",
            )
            proc.start()
            proc.join(timeout=timeout_sec)
            elapsed_ms = (time.perf_counter() - start_perf) * 1000.0

            if proc.is_alive():
                # TIMEOUT CUTOFF: Terminate and kill child process to guarantee zero zombie execution
                error_msg = (
                    f"Task timed out after {timeout_sec:.2f} seconds "
                    f"(execution limit exceeded, process terminated)"
                )
                logger.error("[%s] %s", invocation_id, error_msg)
                try:
                    proc.terminate()
                    proc.join(timeout=0.2)
                    if proc.is_alive():
                        proc.kill()
                        proc.join(timeout=0.2)
                except Exception as term_err:
                    logger.warning("Error terminating worker process: %s", term_err)
                finally:
                    out_queue.close()
                    out_queue.cancel_join_thread()

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
                end_time_iso = datetime.now(timezone.utc).isoformat()
                self.store.save_invocation(
                    InvocationRecord(
                        invocation_id=invocation_id,
                        function_name=function_name,
                        status="TIMEOUT",
                        start_time_iso=start_time_iso,
                        end_time_iso=end_time_iso,
                        duration_ms=elapsed_ms,
                        timeout_sec=timeout_sec,
                        error_type="TimeoutError",
                        error_message=error_msg,
                        result_metadata={"terminated": True},
                        idempotency_key=idempotency_key,
                        created_at_iso=start_time_iso,
                    )
                )
            else:
                # Process completed normally
                try:
                    if not out_queue.empty():
                        payload_data = out_queue.get(timeout=0.2)
                        out_queue.close()
                        out_queue.join_thread()
                        if payload_data.get("success"):
                            res_payload = payload_data.get("result") or {}
                            result = FunctionResult(
                                status_code=200,
                                payload=res_payload,
                                execution_duration_ms=elapsed_ms,
                                cold_start=is_cold,
                                invocation_id=invocation_id,
                                error_message=None,
                            )
                            end_time_iso = datetime.now(timezone.utc).isoformat()
                            self.store.save_invocation(
                                InvocationRecord(
                                    invocation_id=invocation_id,
                                    function_name=function_name,
                                    status="SUCCESS",
                                    start_time_iso=start_time_iso,
                                    end_time_iso=end_time_iso,
                                    duration_ms=elapsed_ms,
                                    timeout_sec=timeout_sec,
                                    error_type=None,
                                    error_message=None,
                                    result_metadata={"keys": list(res_payload.keys())},
                                    idempotency_key=idempotency_key,
                                    created_at_iso=start_time_iso,
                                )
                            )
                        else:
                            err_type = payload_data.get("error_type", "RuntimeError")
                            err_msg = payload_data.get("error_msg", "Unknown error")
                            logger.warning("[%s] Handler exception: %s: %s", invocation_id, err_type, err_msg)
                            result = FunctionResult(
                                status_code=500,
                                payload={},
                                execution_duration_ms=elapsed_ms,
                                cold_start=is_cold,
                                invocation_id=invocation_id,
                                error_message=f"{err_type}: {err_msg}",
                            )
                            end_time_iso = datetime.now(timezone.utc).isoformat()
                            self.store.save_invocation(
                                InvocationRecord(
                                    invocation_id=invocation_id,
                                    function_name=function_name,
                                    status="FAILED",
                                    start_time_iso=start_time_iso,
                                    end_time_iso=end_time_iso,
                                    duration_ms=elapsed_ms,
                                    timeout_sec=timeout_sec,
                                    error_type=err_type,
                                    error_message=err_msg,
                                    result_metadata={},
                                    idempotency_key=idempotency_key,
                                    created_at_iso=start_time_iso,
                                )
                            )
                    else:
                        out_queue.close()
                        out_queue.cancel_join_thread()
                        err_msg = "Process terminated prematurely without returning output."
                        result = FunctionResult(
                            status_code=500,
                            payload={},
                            execution_duration_ms=elapsed_ms,
                            cold_start=is_cold,
                            invocation_id=invocation_id,
                            error_message=err_msg,
                        )
                        end_time_iso = datetime.now(timezone.utc).isoformat()
                        self.store.save_invocation(
                            InvocationRecord(
                                invocation_id=invocation_id,
                                function_name=function_name,
                                status="FAILED",
                                start_time_iso=start_time_iso,
                                end_time_iso=end_time_iso,
                                duration_ms=elapsed_ms,
                                timeout_sec=timeout_sec,
                                error_type="PrematureTerminationError",
                                error_message=err_msg,
                                result_metadata={},
                                idempotency_key=idempotency_key,
                                created_at_iso=start_time_iso,
                            )
                        )
                except Exception as read_exc:
                    err_msg = f"Failed to read process output: {read_exc}"
                    result = FunctionResult(
                        status_code=500,
                        payload={},
                        execution_duration_ms=elapsed_ms,
                        cold_start=is_cold,
                        invocation_id=invocation_id,
                        error_message=err_msg,
                    )
                    end_time_iso = datetime.now(timezone.utc).isoformat()
                    self.store.save_invocation(
                        InvocationRecord(
                            invocation_id=invocation_id,
                            function_name=function_name,
                            status="FAILED",
                            start_time_iso=start_time_iso,
                            end_time_iso=end_time_iso,
                            duration_ms=elapsed_ms,
                            timeout_sec=timeout_sec,
                            error_type="QueueReadError",
                            error_message=err_msg,
                            result_metadata={},
                            idempotency_key=idempotency_key,
                            created_at_iso=start_time_iso,
                        )
                    )
        else:
            # Explicit Opt-in Thread Isolation (Documented: timeouts cannot terminate threads)
            logger.warning("[%s] Function '%s' executing with opt-in thread isolation.", invocation_id, function_name)
            container_res: Dict[str, Any] = {"result": None, "error": None, "completed": False}

            def worker_target() -> None:
                try:
                    container_res["result"] = handler(event, context)
                    container_res["completed"] = True
                except Exception as exc:
                    container_res["error"] = exc
                    container_res["completed"] = True

            exec_thread = threading.Thread(target=worker_target, daemon=True)
            exec_thread.start()
            exec_thread.join(timeout=timeout_sec)
            elapsed_ms = (time.perf_counter() - start_perf) * 1000.0

            if exec_thread.is_alive() and not container_res["completed"]:
                error_msg = f"Task timed out after {timeout_sec:.2f} seconds (thread isolation timeout limit)"
                result = FunctionResult(
                    status_code=504,
                    payload={},
                    execution_duration_ms=elapsed_ms,
                    cold_start=is_cold,
                    invocation_id=invocation_id,
                    error_message=error_msg,
                )
            elif container_res["error"]:
                err = container_res["error"]
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
                    payload=container_res["result"] or {},
                    execution_duration_ms=elapsed_ms,
                    cold_start=is_cold,
                    invocation_id=invocation_id,
                    error_message=None,
                )

        # 6. Durable Idempotency Caching (upon HTTP 200 success)
        if idempotency_key and result.status_code == 200:
            with self._lock:
                self._idempotency_store[idempotency_key] = result
            self.store.save_idempotency(
                key=idempotency_key,
                invocation_id=invocation_id,
                function_name=function_name,
                status_code=result.status_code,
                payload=result.payload,
            )

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
        """Evict all warm containers and in-memory caches."""
        with self._lock:
            self._warm_containers.clear()
            self._idempotency_store.clear()
            if self.store:
                self.store.clear()

    def clear_persistence(self) -> None:
        """Clear durable SQLite tables."""
        if self.store:
            self.store.clear()


# ===========================================================================
# Standalone HTTP Server for Serverless FaaS Execution
# ===========================================================================
class ServerlessServiceHTTPHandler(BaseHTTPRequestHandler):
    """HTTP endpoint router for standalone Serverless runtime service."""

    runtime: Optional[ServerlessRuntime] = None

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
            self._send_json(200, {
                "status": "ok",
                "registered_functions": list(self.runtime._registry.keys()),
            })
        elif self.path.startswith("/invocations/"):
            inv_id = self.path[len("/invocations/") :]
            rec = self.runtime.store.get_invocation(inv_id)
            if rec:
                self._send_json(200, rec)
            else:
                self._send_json(404, {"error": "NotFound", "invocation_id": inv_id})
        elif self.path.startswith("/stats/"):
            fn_name = self.path[len("/stats/") :]
            stats = self.runtime.get_container_stats(fn_name)
            self._send_json(200, stats)
        else:
            self._send_json(404, {"error": "NotFound"})

    def do_POST(self) -> None:
        content_length = int(self.headers.get("Content-Length", 0))
        raw_body = self.rfile.read(content_length).decode("utf-8") if content_length > 0 else "{}"
        try:
            d = json.loads(raw_body)
        except Exception:
            d = {}

        if self.path == "/invoke":
            fn_name = d.get("function_name", "")
            event = d.get("event", {})
            timeout_sec = float(d.get("timeout_sec", 1.0))
            idempotency_key = d.get("idempotency_key")
            res = self.runtime.invoke(
                function_name=fn_name,
                event=event,
                timeout_sec=timeout_sec,
                idempotency_key=idempotency_key,
            )
            self._send_json(res.status_code, res.to_dict())
        else:
            self._send_json(404, {"error": "UnknownEndpoint"})


def start_serverless_service(runtime: ServerlessRuntime, port: int) -> HTTPServer:
    """Start local threaded HTTP server for ServerlessRuntime."""
    handler = type("BoundServerlessHandler", (ServerlessServiceHTTPHandler,), {"runtime": runtime})
    server = HTTPServer(("127.0.0.1", port), handler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    return server


def main() -> None:
    parser = argparse.ArgumentParser(description="Standalone Serverless Computing Service.")
    parser.add_argument("--port", type=int, default=9840, help="Port to listen on")
    parser.add_argument("--db", default="serverless_invocations.db", help="Persistent SQLite file")
    args = parser.parse_args()

    rt = ServerlessRuntime(persistence_db_path=args.db)
    # Register standard handlers
    rt.register_function("echo", echo_handler)
    rt.register_function("slow", slow_timeout_handler)
    rt.register_function("faulty", failing_handler)
    rt.register_function("idempotent", idempotent_test_handler)
    rt.register_function(
        "math",
        validated_math_handler,
        required_keys=["operation", "x", "y"],
    )

    server = start_serverless_service(rt, args.port)
    print(f"Serverless service online on http://127.0.0.1:{args.port} (DB: {args.db})...")
    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        server.shutdown()


if __name__ == "__main__":
    main()
