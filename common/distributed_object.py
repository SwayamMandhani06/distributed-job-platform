"""
Distributed Object-Based Systems: Remote Object and Proxy/Stub Architecture
(Unit IV — Emerging Paradigms).

Theoretical Background (Distributed Object Models):
---------------------------------------------------
In Distributed Object-Based Systems (e.g., Java RMI, CORBA, Microsoft DCOM), remote
objects reside in the address space of a remote server process, but clients interact
with them as if they were ordinary local objects.

Architectural Roles:
--------------------
1. Client:
   Calls methods on a local interface without knowing the object resides on a remote host.
2. Client-Side Proxy (Stub):
   Implements the remote interface locally. It intercepts method invocations, marshals
   (serializes) method names and arguments into a standard network payload, transmits the
   request across the network, waits for the reply, and unmarshals the return value or raises
   a remote exception.
3. Communication Layer (Transport):
   Carries serialized request and response messages between client and server runtimes,
   handling connection lifecycles, network timeouts, and transport errors.
4. Server-Side Dispatcher (Skeleton):
   Receives serialized requests, unmarshals the method name and parameters, dispatches the call
   to the actual Server Object, captures the return value or exception, marshals the result,
   and transmits the response back to the client proxy.
5. Server Object (Servant):
   The actual implementation executing business logic on the remote machine.

How Remote Invocations Differ from Local Method Calls:
------------------------------------------------------
* Memory Space: Local calls jump directly to memory addresses within the same process heap.
  Remote calls cross address space and network boundaries.
* Parameter Semantics: Local calls pass references (pointers). Remote calls pass copies
  (pass-by-value / serialization) unless remote object references are explicitly used.
* Failure Modes: Local calls fail only if the algorithm throws or the host crashes. Remote
  calls can experience partial failures: network timeouts, message corruption, host unreachability,
  or server-side deadlocks.
* Latency: Local calls take nanoseconds; remote calls incur network latency (milliseconds).
"""

import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple
from uuid import uuid4

from common.logging_config import setup_logger

logger = setup_logger("dist_object")


class RemoteException(Exception):
    """Base exception for errors originating on the remote server object."""
    pass


class RemoteTimeoutError(RemoteException):
    """Raised when a remote invocation exceeds the configured timeout threshold."""
    pass


class RemoteCommunicationError(RemoteException):
    """Raised when the communication channel encounters a network partition or transport drop."""
    pass


@dataclass
class RemoteInvocationRequest:
    """Marshalled remote procedure call packet."""
    invocation_id: str
    service_name: str
    method_name: str
    args: List[Any]
    kwargs: Dict[str, Any]
    timestamp_iso: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def serialize(self) -> str:
        return json.dumps({
            "invocation_id": self.invocation_id,
            "service_name": self.service_name,
            "method_name": self.method_name,
            "args": self.args,
            "kwargs": self.kwargs,
            "timestamp_iso": self.timestamp_iso,
        })

    @classmethod
    def deserialize(cls, data_str: str) -> "RemoteInvocationRequest":
        d = json.loads(data_str)
        return cls(
            invocation_id=d["invocation_id"],
            service_name=d["service_name"],
            method_name=d["method_name"],
            args=d.get("args", []),
            kwargs=d.get("kwargs", {}),
            timestamp_iso=d.get("timestamp_iso", ""),
        )


@dataclass
class RemoteInvocationResponse:
    """Marshalled remote procedure return packet."""
    invocation_id: str
    success: bool
    result: Any = None
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    execution_time_ms: float = 0.0

    def serialize(self) -> str:
        return json.dumps({
            "invocation_id": self.invocation_id,
            "success": self.success,
            "result": self.result,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "execution_time_ms": self.execution_time_ms,
        })

    @classmethod
    def deserialize(cls, data_str: str) -> "RemoteInvocationResponse":
        d = json.loads(data_str)
        return cls(
            invocation_id=d["invocation_id"],
            success=d["success"],
            result=d.get("result"),
            error_type=d.get("error_type"),
            error_message=d.get("error_message"),
            execution_time_ms=d.get("execution_time_ms", 0.0),
        )


class RemoteCommunicationChannel:
    """Simulates network transport between client proxy and server skeleton.

    Allows configurable network latency, simulated packet drop, and timeout thresholds.
    """

    def __init__(
        self,
        simulated_latency_seconds: float = 0.0,
        packet_loss_probability: float = 0.0,
        default_timeout_seconds: float = 2.0,
    ) -> None:
        self.simulated_latency = float(simulated_latency_seconds)
        self.packet_loss_prob = float(packet_loss_probability)
        self.default_timeout = float(default_timeout_seconds)
        self.server_skeleton: Optional["RemoteServerSkeleton"] = None
        self.transmitted_packets: List[Dict[str, Any]] = []

    def bind_server(self, skeleton: "RemoteServerSkeleton") -> None:
        self.server_skeleton = skeleton

    def send_and_receive(
        self,
        serialized_request: str,
        timeout_seconds: Optional[float] = None,
    ) -> str:
        """Transmit serialized request over communication channel and return serialized response."""
        effective_timeout = timeout_seconds if timeout_seconds is not None else self.default_timeout

        if self.simulated_latency > effective_timeout:
            raise RemoteTimeoutError(
                f"Invocation timed out: channel latency {self.simulated_latency:.2f}s "
                f"exceeded client timeout threshold {effective_timeout:.2f}s."
            )

        if self.server_skeleton is None:
            raise RemoteCommunicationError("Cannot deliver request: no server skeleton bound to channel.")

        if self.packet_loss_prob >= 1.0:
            raise RemoteCommunicationError("Network partition: packet dropped by communication channel.")

        # Record transmission metrics
        req = RemoteInvocationRequest.deserialize(serialized_request)
        self.transmitted_packets.append({
            "direction": "CLIENT_TO_SERVER",
            "invocation_id": req.invocation_id,
            "method": f"{req.service_name}.{req.method_name}",
            "bytes": len(serialized_request),
        })

        # Server skeleton dispatches invocation
        serialized_resp = self.server_skeleton.dispatch(serialized_request)

        self.transmitted_packets.append({
            "direction": "SERVER_TO_CLIENT",
            "invocation_id": req.invocation_id,
            "method": f"{req.service_name}.{req.method_name}",
            "bytes": len(serialized_resp),
        })

        return serialized_resp


class RemoteServerSkeleton:
    """Server-side dispatcher (Skeleton) receiving requests and executing on Server Objects."""

    def __init__(self) -> None:
        # Registry of named server objects: service_name -> servant instance
        self.registered_services: Dict[str, Any] = {}
        # Explicit allowlist of callable methods per service: service_name -> Set[str]
        self.allowed_methods: Dict[str, Set[str]] = {}
        self.call_log: List[Dict[str, Any]] = []

    def register_service(
        self,
        service_name: str,
        servant_instance: Any,
        allowed_methods: Optional[Any] = None,
    ) -> None:
        name = str(service_name)
        self.registered_services[name] = servant_instance

        if allowed_methods is not None:
            # 1. Explicit allowlist provided via registration arguments
            self.allowed_methods[name] = {
                str(m) for m in allowed_methods if not str(m).startswith("_")
            }
        elif hasattr(servant_instance, "__remote_allowed_methods__"):
            # 2. Explicit service-declared allowlist via attribute
            declared = getattr(servant_instance, "__remote_allowed_methods__")
            self.allowed_methods[name] = {
                str(m) for m in declared if not str(m).startswith("_")
            }
        else:
            # 3. FAIL-SAFE: Omitted allowlist defaults to empty set.
            # Never automatically expose arbitrary public methods or attributes.
            self.allowed_methods[name] = set()
            logger.warning(
                "Service '%s' registered without explicit allowlist or __remote_allowed_methods__; "
                "defaulting to empty allowlist (fail-safe). No remote methods are exposed.",
                name,
            )

        logger.info(
            "Registered server object under service name '%s' with allowed methods: %s",
            name,
            sorted(list(self.allowed_methods[name])),
        )

    def dispatch(self, serialized_request: str) -> str:
        """Unmarshal request, enforce allowlists and underscore rejection, invoke servant method, and marshal response."""
        start_t = time.perf_counter()
        req = RemoteInvocationRequest.deserialize(serialized_request)
        servant = self.registered_services.get(req.service_name)

        if servant is None:
            resp = RemoteInvocationResponse(
                invocation_id=req.invocation_id,
                success=False,
                error_type="ServiceNotFoundError",
                error_message=f"No servant registered for service '{req.service_name}'.",
            )
            return resp.serialize()

        # Security check 1: Reject private and dunder names (starting with '_')
        if req.method_name.startswith("_"):
            resp = RemoteInvocationResponse(
                invocation_id=req.invocation_id,
                success=False,
                error_type="MethodNotAllowedError",
                error_message=f"Access denied: invocation of private or dunder method '{req.method_name}' is forbidden.",
            )
            return resp.serialize()

        # Security check 2: Check explicit allowlist of registered methods
        allowed = self.allowed_methods.get(req.service_name, set())
        if req.method_name not in allowed:
            resp = RemoteInvocationResponse(
                invocation_id=req.invocation_id,
                success=False,
                error_type="MethodNotAllowedError",
                error_message=f"Method '{req.method_name}' is not in the registered allowlist for service '{req.service_name}'.",
            )
            return resp.serialize()

        method = getattr(servant, req.method_name, None)
        if method is None or not callable(method):
            resp = RemoteInvocationResponse(
                invocation_id=req.invocation_id,
                success=False,
                error_type="MethodNotFoundError",
                error_message=f"Servant '{req.service_name}' has no callable method '{req.method_name}'.",
            )
            return resp.serialize()

        try:
            # Dynamic method invocation on remote servant
            logger.debug("[%s] Invoking %s.%s(*%s, **%s)", req.invocation_id, req.service_name, req.method_name, req.args, req.kwargs)
            res = method(*req.args, **req.kwargs)
            duration_ms = (time.perf_counter() - start_t) * 1000.0

            resp = RemoteInvocationResponse(
                invocation_id=req.invocation_id,
                success=True,
                result=res,
                execution_time_ms=round(duration_ms, 2),
            )
            self.call_log.append({
                "invocation_id": req.invocation_id,
                "service": req.service_name,
                "method": req.method_name,
                "status": "SUCCESS",
                "duration_ms": duration_ms,
            })
            return resp.serialize()
        except Exception as exc:
            duration_ms = (time.perf_counter() - start_t) * 1000.0
            resp = RemoteInvocationResponse(
                invocation_id=req.invocation_id,
                success=False,
                error_type=type(exc).__name__,
                error_message=str(exc),
                execution_time_ms=round(duration_ms, 2),
            )
            self.call_log.append({
                "invocation_id": req.invocation_id,
                "service": req.service_name,
                "method": req.method_name,
                "status": "EXCEPTION",
                "error": str(exc),
                "duration_ms": duration_ms,
            })
            logger.warning("[%s] Remote invocation raised %s: %s", req.invocation_id, type(exc).__name__, exc)
            return resp.serialize()


class RemoteClientProxy:
    """Client-side stub that intercepts calls, marshals parameters, and unmarshals results."""

    def __init__(
        self,
        service_name: str,
        channel: RemoteCommunicationChannel,
        timeout_seconds: Optional[float] = None,
    ) -> None:
        self._service_name = str(service_name)
        self._channel = channel
        self._timeout_seconds = timeout_seconds

    def __getattr__(self, method_name: str) -> Callable[..., Any]:
        """Dynamically intercept method calls on the proxy."""
        def remote_method_invoker(*args: Any, **kwargs: Any) -> Any:
            invocation_id = f"inv-{uuid4().hex[:8]}"
            req = RemoteInvocationRequest(
                invocation_id=invocation_id,
                service_name=self._service_name,
                method_name=method_name,
                args=list(args),
                kwargs=kwargs,
            )

            # 1. Marshal (serialize) parameters
            serialized_req = req.serialize()

            # 2. Transmit across communication layer
            serialized_resp = self._channel.send_and_receive(
                serialized_req,
                timeout_seconds=self._timeout_seconds,
            )

            # 3. Unmarshal response
            resp = RemoteInvocationResponse.deserialize(serialized_resp)

            # 4. Return result or raise remote exception
            if resp.success:
                return resp.result
            else:
                err_msg = f"Remote invocation '{self._service_name}.{method_name}' failed: [{resp.error_type}] {resp.error_message}"
                raise RemoteException(err_msg)

        return remote_method_invoker

    def __str__(self) -> str:
        """Route __str__ to remote invocation interceptor to allow server allowlist/rejection validation."""
        return self.__getattr__("__str__")()

    def __repr__(self) -> str:
        return f"<RemoteClientProxy service='{self._service_name}'>"


# ---------------------------------------------------------------------------
# Concrete Example: Remote Job Management Service
# ---------------------------------------------------------------------------
class JobManagementServant:
    """Server-side servant object managing distributed jobs."""

    __remote_allowed_methods__ = {
        "submit_job",
        "get_job_status",
        "calculate_workload_metrics",
    }

    def __init__(self) -> None:
        self.jobs: Dict[str, Dict[str, Any]] = {}

    def submit_job(self, job_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Remote method: submit a job to the remote server."""
        if not job_type:
            raise ValueError("job_type cannot be empty")
        job_id = f"job-{uuid4().hex[:6]}"
        record = {
            "job_id": job_id,
            "job_type": job_type,
            "payload": payload,
            "status": "queued",
            "submitted_at": datetime.now(timezone.utc).isoformat(),
        }
        self.jobs[job_id] = record
        return {"job_id": job_id, "status": "queued", "message": "Job registered on remote servant"}

    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Remote method: retrieve job status."""
        job = self.jobs.get(job_id)
        if not job:
            raise KeyError(f"Job '{job_id}' does not exist on remote servant.")
        return dict(job)

    def calculate_workload_metrics(self, node_weights: List[float]) -> float:
        """Remote method: calculate aggregate cluster workload."""
        if not node_weights:
            raise ValueError("node_weights list cannot be empty")
        return sum(node_weights) / len(node_weights)

    def _internal_helper(self) -> str:
        """Internal private method — not exposed remotely."""
        return "internal_secret_token"
