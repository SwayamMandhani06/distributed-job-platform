"""
Pydantic data models for the Worker service.

The worker re-uses the same ``JobRequest`` schema defined in the scheduler
package.  Re-importing (rather than duplicating) keeps the data contract in
one place — a single source of truth — which is a best practice in any
service-oriented architecture.

``WorkerResponse`` is the worker's own output schema, returned after task
execution.
"""

from pydantic import BaseModel, Field


class WorkerResponse(BaseModel):
    """Schema for the result returned by a worker after executing a job.

    Attributes
    ----------
    status:
        Execution outcome — ``"completed"`` or ``"failed"``.
    worker_id:
        Identifier of the worker instance that handled this job.
        In later stages each worker will register a unique ID; for now
        we use a static default.
    execution_time_seconds:
        Wall-clock time the worker spent on this job, in seconds.
    """

    status: str = Field(
        ...,
        description="Execution outcome.",
        examples=["completed", "failed"],
    )
    worker_id: str = Field(
        ...,
        description="Identifier of the worker that executed the job.",
    )
    execution_time_seconds: float = Field(
        ...,
        ge=0,
        description="Time the worker spent executing the job, in seconds.",
    )
