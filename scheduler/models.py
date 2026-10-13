"""
Pydantic data models for the Scheduler service.

WHY Pydantic models instead of plain dicts?
In distributed systems, services communicate over the network. Data contracts
(schemas) enforce that both sides agree on the shape of every message. Pydantic
gives us:
  • Automatic validation — malformed payloads are rejected *before* any
    business logic runs.
  • Security hardening — input size limits, regex whitelisting, and deep-nesting
    checks protect against DoS attacks and injection payloads.
  • Serialisation / deserialisation — ``model_dump()`` and ``model_validate()``
    map cleanly to JSON, which is the lingua franca of HTTP-based RPC.
  • Self-documenting API — FastAPI auto-generates OpenAPI docs directly from
    these models, so the scheduler's contract is always up-to-date.
"""

import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator

# Security and input validation limits (Phase 7 Hardening)
MAX_PAYLOAD_BYTES = 512 * 1024  # 512 KB
MAX_PAYLOAD_DEPTH = 10
JOB_TYPE_REGEX = re.compile(r"^[a-zA-Z0-9_-]{1,64}$")


class JobRequest(BaseModel):
    """Schema for an incoming job submission with defensive security validation.

    Attributes
    ----------
    job_type:
        A human-readable label identifying the category of work
        (e.g. ``"math_prime_sieve"``, ``"hash_checksum"``). Validated against alphanumeric chars.
    payload:
        Arbitrary key-value data the worker needs to execute the job.
        Protected by size (< 512KB) and nesting depth (< 10) constraints.
    submitted_at:
        UTC timestamp auto-generated when the request object is created.
    """

    job_type: str = Field(
        ...,
        min_length=1,
        max_length=64,
        description="Category of work to perform (alphanumeric, underscores, hyphens, 1-64 chars).",
        examples=["math_prime_sieve", "hash_checksum", "text_frequency"],
    )
    payload: Dict[str, Any] = Field(
        default_factory=dict,
        description="Arbitrary key-value data for job execution (max 512KB).",
    )
    submitted_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="UTC timestamp of when the job was submitted.",
    )

    @field_validator("job_type")
    @classmethod
    def validate_job_type(cls, v: str) -> str:
        clean = v.strip()
        if not clean:
            raise ValueError("job_type cannot be empty or whitespace only")
        if not JOB_TYPE_REGEX.match(clean):
            raise ValueError(
                f"job_type '{clean}' contains invalid characters; must be alphanumeric, hyphen, or underscore (1-64 chars)"
            )
        return clean

    @field_validator("payload")
    @classmethod
    def validate_payload(cls, v: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(v, dict):
            raise ValueError("payload must be a JSON object dictionary")

        # 1. Size constraint
        try:
            serialized = json.dumps(v)
        except Exception as e:
            raise ValueError(f"payload could not be serialized to JSON: {e}")

        payload_bytes = len(serialized.encode("utf-8"))
        if payload_bytes > MAX_PAYLOAD_BYTES:
            raise ValueError(
                f"payload size ({payload_bytes} bytes) exceeds maximum allowable limit of {MAX_PAYLOAD_BYTES} bytes (512 KB)"
            )

        # 2. Nesting depth and dunder key protection
        def check_depth_and_keys(obj: Any, depth: int = 1) -> None:
            if depth > MAX_PAYLOAD_DEPTH:
                raise ValueError(f"payload nesting depth exceeds maximum limit of {MAX_PAYLOAD_DEPTH}")
            if isinstance(obj, dict):
                for k, child in obj.items():
                    if not isinstance(k, str):
                        raise ValueError(f"payload keys must be strings, got: {type(k).__name__}")
                    if k.startswith("__") and k.endswith("__"):
                        raise ValueError(f"payload cannot contain reserved dunder key: '{k}'")
                    check_depth_and_keys(child, depth + 1)
            elif isinstance(obj, (list, tuple)):
                for item in obj:
                    check_depth_and_keys(item, depth + 1)

        check_depth_and_keys(v)
        return v


class JobResponse(BaseModel):
    """Schema for the response returned after a job is submitted.

    Attributes
    ----------
    job_id:
        A globally unique identifier (UUID4) assigned by the scheduler.
    status:
        Current state of the job (e.g. ``"accepted"``, ``"completed"``, ``"failed"``).
    message:
        Human-readable detail about the outcome.
    correlation_id:
        Distributed tracing correlation ID associated with this request.
    """

    job_id: str = Field(
        default_factory=lambda: str(uuid4()),
        description="Unique identifier assigned to this job by the scheduler.",
    )
    status: str = Field(
        ...,
        description="Current state of the job.",
        examples=["accepted", "completed", "failed"],
    )
    message: str = Field(
        ...,
        description="Human-readable detail about what happened.",
    )
    correlation_id: Optional[str] = Field(
        default=None,
        description="Distributed tracing correlation ID.",
    )
