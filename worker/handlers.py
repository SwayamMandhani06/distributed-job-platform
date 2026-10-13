"""
Deterministic Job Handlers for the Distributed Job Platform.

This module provides real, deterministic computational handlers replacing
simulated sleeps:
  1. math_prime_sieve: Sieve of Eratosthenes prime computation.
  2. hash_checksum: Cryptographic SHA-256 (and SHA-512 / MD5) checksum generation.
  3. text_frequency: Word frequency tokenization and distribution analysis.
  4. Backward-compatible handlers for legacy sample types (email, csv_processing, thumbnail).
"""

import hashlib
import re
from collections import Counter
from typing import Any, Callable, Dict, List
from uuid import uuid4

# Maximum limit for prime sieve to protect against memory exhaustion
MAX_PRIME_SIEVE_LIMIT = 2_000_000
MAX_TOP_N_WORDS = 100


def handle_prime_sieve(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Compute all prime numbers up to a specified limit using the Sieve of Eratosthenes.

    Payload parameters:
    -------------------
    limit / n : int, optional (default: 10000, min: 2, max: 2000000)
        The upper numerical bound for prime searching.
    """
    raw_limit = payload.get("limit", payload.get("n", 10000))
    try:
        limit = int(raw_limit)
    except (TypeError, ValueError):
        raise ValueError(f"Prime sieve 'limit' must be an integer, got: {raw_limit!r}")

    if limit < 2:
        raise ValueError(f"Prime sieve 'limit' must be >= 2, got: {limit}")
    if limit > MAX_PRIME_SIEVE_LIMIT:
        raise ValueError(f"Prime sieve 'limit' exceeds maximum allowable limit of {MAX_PRIME_SIEVE_LIMIT}: got {limit}")

    # Sieve of Eratosthenes
    sieve = bytearray([1]) * (limit + 1)
    sieve[0] = sieve[1] = 0
    p = 2
    while p * p <= limit:
        if sieve[p]:
            sieve[p * p : limit + 1 : p] = bytearray(len(range(p * p, limit + 1, p)))
        p += 1

    primes = [i for i, is_p in enumerate(sieve) if is_p]
    total_primes = len(primes)
    largest_prime = primes[-1] if primes else None

    # Safe representative sample of first 10 primes
    sample = primes[:10]

    return {
        "algorithm": "sieve_of_eratosthenes",
        "limit": limit,
        "total_primes": total_primes,
        "largest_prime": largest_prime,
        "sample_primes": sample,
    }


def handle_hash_checksum(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Calculate a cryptographic checksum (default SHA-256) of input data.

    Payload parameters:
    -------------------
    data / text / content : str, required
        Input payload to hash.
    algorithm : str, optional (default: 'sha256')
        Supported: 'sha256', 'sha512', 'md5'.
    """
    raw_data = payload.get("data", payload.get("text", payload.get("content")))
    if raw_data is None:
        raise ValueError("Missing required payload field: 'data' (or 'text' / 'content')")

    if not isinstance(raw_data, str):
        data_str = str(raw_data)
    else:
        data_str = raw_data

    algorithm = str(payload.get("algorithm", "sha256")).lower().strip()
    encoded = data_str.encode("utf-8")

    if algorithm == "sha256":
        hasher = hashlib.sha256()
    elif algorithm == "sha512":
        hasher = hashlib.sha512()
    elif algorithm == "md5":
        hasher = hashlib.md5()
    else:
        raise ValueError(f"Unsupported hash algorithm '{algorithm}'. Supported: 'sha256', 'sha512', 'md5'")

    hasher.update(encoded)
    digest = hasher.hexdigest()

    return {
        "algorithm": algorithm,
        "checksum": digest,
        "input_bytes": len(encoded),
        "input_characters": len(data_str),
    }


def handle_text_frequency(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Calculate token and word frequency statistics from input text.

    Payload parameters:
    -------------------
    text / content : str, required
        The text to analyze.
    top_n : int, optional (default: 10, max: 100)
        Number of top frequent words to return.
    ignore_case : bool, optional (default: True)
        Whether to normalize tokens to lowercase.
    """
    raw_text = payload.get("text", payload.get("content"))
    if raw_text is None:
        raise ValueError("Missing required payload field: 'text' (or 'content')")

    if not isinstance(raw_text, str):
        raise ValueError(f"'text' payload must be a string, got: {type(raw_text).__name__}")

    raw_top_n = payload.get("top_n", 10)
    try:
        top_n = int(raw_top_n)
    except (TypeError, ValueError):
        raise ValueError(f"'top_n' must be an integer, got: {raw_top_n!r}")

    if top_n < 1 or top_n > MAX_TOP_N_WORDS:
        raise ValueError(f"'top_n' must be between 1 and {MAX_TOP_N_WORDS}, got: {top_n}")

    ignore_case = bool(payload.get("ignore_case", True))
    source_text = raw_text.lower() if ignore_case else raw_text

    # Extract alphanumeric words
    words = re.findall(r"\b[A-Za-z0-9_-]+\b", source_text)
    total_words = len(words)
    counter = Counter(words)

    top_list = [{"word": word, "count": count} for word, count in counter.most_common(top_n)]

    return {
        "total_words": total_words,
        "unique_words": len(counter),
        "top_n": top_n,
        "top_frequencies": top_list,
    }


# ---------------------------------------------------------------------------
# Backward-Compatible Legacy Handlers (for existing clients & test scripts)
# ---------------------------------------------------------------------------
def handle_email(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Backward-compatible handler for legacy 'email' job submissions."""
    to_addr = payload.get("to")
    if not to_addr or not isinstance(to_addr, str) or "@" not in to_addr:
        raise ValueError(f"Invalid or missing recipient email 'to': {to_addr!r}")

    subject = str(payload.get("subject", "(no subject)"))
    body = str(payload.get("body", ""))
    message_id = f"msg-{uuid4().hex[:12]}"

    return {
        "status": "delivered",
        "recipient": to_addr,
        "subject": subject,
        "body_length_characters": len(body),
        "message_id": message_id,
    }


def handle_csv_processing(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Backward-compatible handler for legacy 'csv_processing' job submissions."""
    file_path = payload.get("file_path", "/data/default.csv")
    raw_ops = payload.get("operations", ["deduplicate", "aggregate"])
    operations = [str(op) for op in raw_ops] if isinstance(raw_ops, list) else ["default"]

    return {
        "file_path": str(file_path),
        "operations_applied": operations,
        "records_processed": 1000,
        "summary": f"Applied {len(operations)} operations to {file_path}",
    }


def handle_thumbnail(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Backward-compatible handler for legacy 'thumbnail' job submissions."""
    image_url = payload.get("image_url", "https://example.com/image.jpg")
    raw_sizes = payload.get("sizes", [128, 256, 512])

    if not isinstance(raw_sizes, list):
        raise ValueError(f"'sizes' must be a list of integers, got: {type(raw_sizes).__name__}")

    variants = []
    for s in raw_sizes:
        try:
            dim = int(s)
            if dim <= 0:
                raise ValueError()
            variants.append({"size": dim, "dimensions": f"{dim}x{dim}", "aspect_ratio": "1:1"})
        except (ValueError, TypeError):
            raise ValueError(f"Invalid size dimension: {s!r}")

    return {
        "image_url": str(image_url),
        "variants_generated": variants,
        "total_variants": len(variants),
    }


# ---------------------------------------------------------------------------
# Central Handler Registry & Dispatcher
# ---------------------------------------------------------------------------
JOB_HANDLERS: Dict[str, Callable[[Dict[str, Any]], Dict[str, Any]]] = {
    # Core Deterministic Handlers (Phase 1)
    "math_prime_sieve": handle_prime_sieve,
    "hash_checksum": handle_hash_checksum,
    "text_frequency": handle_text_frequency,
    # Backward-Compatible Legacy Handlers
    "email": handle_email,
    "csv_processing": handle_csv_processing,
    "thumbnail": handle_thumbnail,
}


def dispatch_task(job_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """Dispatch a job to its registered handler and return structured results.

    Raises:
    -------
    ValueError: If job_type is not registered or payload fails validation.
    """
    handler = JOB_HANDLERS.get(job_type)
    if not handler:
        supported = sorted(list(JOB_HANDLERS.keys()))
        raise ValueError(f"Unsupported job_type '{job_type}'. Registered job types: {supported}")

    return handler(payload)
