"""
Pydantic data models for the Scheduler service.

WHY Pydantic models instead of plain dicts?
In distributed systems, services communicate over the network.  Data contracts
(schemas) enforce that both sides agree on the shape of every message.  Pydantic
gives us:
  • Automatic validation — malformed payloads are rejected *before* any
    business logic runs.
  • Serialisation / deserialisation — ``model_dump()`` and ``model_validate()``
    map cleanly to JSON, which is the lingua franca of HTTP-based RPC.
  • Self-documenting API — FastAPI auto-generates OpenAPI docs directly from
    these models, so the scheduler's contract is always up-to-date.

These properties become even more important in later stages when multiple
workers, message queues, and persistent storage all need to agree on the
same data shapes.
"""

from datetime import datetime, timezone
from typing import Any, Dict
from uuid import uuid4

from pydantic import BaseModel, Field


class JobRequest(BaseModel):
    """Schema for an incoming job submission.

    Attributes
    ----------
    job_type:
        A human-readable label identifying the kind of work
        (e.g. ``"email"``, ``"csv_processing"``).
    payload:
        Arbitrary key-value data the worker needs to execute the job.
    submitted_at:
        UTC timestamp auto-generated when the request object is created.
        Using a ``default_factory`` keeps the model pure — callers don't
        need to supply it, yet it's always present for auditing.
    """

    job_type: str = Field(
        ...,
        min_length=1,
        description="Category of work to perform (e.g. 'email', 'thumbnail').",
        examples=["email", "csv_processing", "thumbnail"],
    )
    payload: Dict[str, Any] = Field(
        default_factory=dict,
        description="Arbitrary data the worker needs to execute this job.",
    )
    submitted_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="UTC timestamp of when the job was submitted.",
    )


class JobResponse(BaseModel):
    """Schema for the response returned after a job is submitted.

    Attributes
    ----------
    job_id:
        A globally unique identifier (UUID4) assigned by the scheduler.
    status:
        Current state of the job (e.g. ``"accepted"``, ``"completed"``,
        ``"failed"``).
    message:
        Human-readable detail about the outcome, often including the
        worker's reply.
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
