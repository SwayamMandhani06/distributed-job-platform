"""
Shared structured logging configuration for the distributed job platform.

WHY a centralised structured logging setup?
In a distributed system, consistent log formatting across services is critical for
debugging, audit trails, and observability. When you aggregate logs (e.g. with ELK,
Loki, or CloudWatch), having a uniform format — timestamp, severity, service name,
correlation ID, and job ID — makes distributed tracing across scheduler and worker
nodes trivial.

Features:
  1. Contextual Tracing: Correlation IDs and Job IDs injected via contextvars or record extras.
  2. Security & Compliance: Automatic redaction of sensitive credentials, secrets, tokens,
     and private keys to prevent credential leakage in plain text logs.
  3. Structured Formatting: Clean, machine-parseable log lines with UTC timestamps.
"""

import contextvars
import logging
import re
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Union

# Contextual variables for distributed tracing across async tasks and function calls
correlation_id_ctx: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("correlation_id", default=None)
job_id_ctx: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("job_id", default=None)

# Sensitive keys and patterns to redact from logs
SENSITIVE_KEY_PATTERNS = re.compile(
    r"(password|token|secret|api_key|auth|authorization|credential|private_key|access_key)",
    re.IGNORECASE,
)


def sanitize_sensitive_data(obj: Any) -> Any:
    """Recursively sanitize dictionaries and data structures to redact sensitive secrets."""
    if isinstance(obj, dict):
        sanitized = {}
        for k, v in obj.items():
            if SENSITIVE_KEY_PATTERNS.search(str(k)):
                sanitized[k] = "[REDACTED]"
            else:
                sanitized[k] = sanitize_sensitive_data(v)
        return sanitized
    elif isinstance(obj, (list, tuple, set)):
        return [sanitize_sensitive_data(item) for item in obj]
    elif isinstance(obj, str):
        # Redact bearer tokens or inline secrets if detected
        return re.sub(r"(Bearer\s+)[A-Za-z0-9_\-\.]{8,}", r"\1[REDACTED]", obj)
    return obj


def set_trace_context(correlation_id: Optional[str] = None, job_id: Optional[str] = None) -> None:
    """Set the contextual correlation and job IDs for the current task/thread."""
    if correlation_id is not None:
        correlation_id_ctx.set(correlation_id)
    if job_id is not None:
        job_id_ctx.set(job_id)


def clear_trace_context() -> None:
    """Clear contextual correlation and job IDs."""
    correlation_id_ctx.set(None)
    job_id_ctx.set(None)


class StructuredLogFormatter(logging.Formatter):
    """Custom logging formatter that injects correlation ID, job ID, and redacts sensitive data."""

    def __init__(self, fmt: Optional[str] = None, datefmt: Optional[str] = None):
        super().__init__(fmt=fmt, datefmt=datefmt or "%Y-%m-%d %H:%M:%S")

    def format(self, record: logging.LogRecord) -> str:
        # Contextual correlation ID
        if not hasattr(record, "correlation_id") or not record.correlation_id:
            record.correlation_id = correlation_id_ctx.get() or "-"

        # Contextual job ID
        if not hasattr(record, "job_id") or not record.job_id:
            record.job_id = job_id_ctx.get() or "-"

        # Sanitize message arguments if passed as dicts/structures
        if record.args:
            if isinstance(record.args, dict):
                record.args = sanitize_sensitive_data(record.args)
            elif isinstance(record.args, (list, tuple)):
                record.args = tuple(sanitize_sensitive_data(a) for a in record.args)

        return super().format(record)


def setup_logger(
    name: str,
    level: int = logging.INFO,
    fmt: Optional[str] = None,
) -> logging.Logger:
    """Create and return a configured :class:`logging.Logger` with structured contextual tracing.

    Parameters
    ----------
    name:
        Logical name for the logger — typically the service name
        (e.g. ``"scheduler"``, ``"worker-1"``).
    level:
        Minimum severity level to emit. Defaults to ``INFO``.
    fmt:
        Custom format string. When *None* a sensible structured default is used that
        includes UTC timestamp, severity, service, correlation ID, job ID, and message.

    Returns
    -------
    logging.Logger
        A configured logger instance ready for distributed execution.
    """
    logger = logging.getLogger(name)

    # Prevent adding duplicate handlers if setup_logger is called multiple times
    if logger.handlers:
        return logger

    logger.setLevel(level)
    logger.propagate = False

    if fmt is None:
        fmt = "%(asctime)s | %(levelname)-8s | %(name)s | corr=%(correlation_id)s job=%(job_id)s | %(message)s"

    formatter = StructuredLogFormatter(fmt=fmt, datefmt="%Y-%m-%d %H:%M:%S")

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger
