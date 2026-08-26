"""
Shared logging configuration for the distributed job platform.

WHY a centralised logging setup?
In a distributed system, consistent log formatting across services is critical for
debugging and observability.  When you later aggregate logs (e.g. with ELK or Loki),
having a uniform format — timestamp, severity, service name — makes correlation
across scheduler and worker nodes trivial.

This module provides a single ``setup_logger`` factory so every service gets the
same structured output without duplicating configuration.
"""

import logging
import sys
from typing import Optional


def setup_logger(
    name: str,
    level: int = logging.INFO,
    fmt: Optional[str] = None,
) -> logging.Logger:
    """Create and return a configured :class:`logging.Logger`.

    Parameters
    ----------
    name:
        Logical name for the logger — typically the service name
        (e.g. ``"scheduler"``, ``"worker-1"``).
    level:
        Minimum severity level to emit.  Defaults to ``INFO``.
    fmt:
        Custom format string.  When *None* a sensible default is used that
        includes a UTC timestamp, level, service name, and the message.

    Returns
    -------
    logging.Logger
        A logger instance ready to use.

    Example
    -------
    >>> logger = setup_logger("scheduler")
    >>> logger.info("Job %s submitted", job_id)
    2026-08-26 12:00:00 | INFO     | scheduler | Job abc-123 submitted
    """
    logger = logging.getLogger(name)

    # Prevent adding duplicate handlers if setup_logger is called more than
    # once for the same name (e.g. during testing or hot-reload).
    if logger.handlers:
        return logger

    logger.setLevel(level)

    # Prevent messages from propagating to parent loggers — avoids
    # duplicate output when using hierarchical names like "scheduler.client".
    logger.propagate = False

    if fmt is None:
        fmt = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"

    formatter = logging.Formatter(fmt, datefmt="%Y-%m-%d %H:%M:%S")

    # Stream handler → stdout so container runtimes (Docker / K8s) can
    # capture logs natively.
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger
