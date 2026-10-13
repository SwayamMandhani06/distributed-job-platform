"""
Redis-backed Message Queue client for job dispatch and workload distribution.

WHY Message-Oriented Middleware (Redis Lists)?
----------------------------------------------
In Stage 1 & 2, the Scheduler invoked Workers via synchronous HTTP RPC calls.
That tight coupling meant:
  1. The Scheduler needed to know the exact IP address and port of every Worker (lacking Location Transparency).
  2. If all Workers were busy or offline, job submissions failed immediately (lacking Availability / Failure Isolation).
  3. The Scheduler was blocked for the full execution time of every job (poor Producer responsiveness).

By introducing Redis as a shared message queue (Producer-Consumer pattern):
  * **Location Transparency**: Producers (Scheduler) and Consumers (Workers) do not know about each other's network addresses; they only interact with the queue.
  * **Temporal Decoupling**: Jobs can be submitted even if zero workers are online. They remain safely queued in Redis until a worker connects.
  * **Asynchronous Execution**: `POST /submit_job` returns in milliseconds with status `'queued'`, freeing client connection threads immediately.

WHY BRPOP (Blocking Pop) over sleep-polling (RPOP + sleep)?
----------------------------------------------------------
Polling with `RPOP` and `time.sleep()` causes either high CPU/network overhead (if sleep interval is low) or high latency (if sleep interval is high).
`BRPOP` blocks the socket connection efficiently at the OS kernel level until Redis pushes new data, delivering:
  1. Zero busy-waiting CPU overhead on worker nodes.
  2. Instant worker wake-up the exact millisecond a job is enqueued.
  3. Natural mutual exclusion: Redis guarantees each item in a list is popped by EXACTLY ONE consumer, making worker load-balancing lock-free.
"""

import time
from typing import Optional

import redis

from common.config import (
    REDIS_HOST,
    REDIS_PORT,
    REDIS_DB,
    QUEUE_NAME,
    PROCESSING_QUEUE_NAME,
    REDIS_BRPOP_TIMEOUT_SECONDS,
)
from common.logging_config import setup_logger

logger = setup_logger("queue_client")


class JobQueue:
    """Wrapper around Redis providing reliable Producer-Consumer queue primitives.

    Architecture (At-Least-Once Delivery Pattern):
    ---------------------------------------------
    Uses the reliable queue pattern via BRPOPLPUSH:
      1. Producer calls `push_job(job_id)` -> LPUSH into `job_queue`.
      2. Consumer calls `claim_job(timeout)` -> BRPOPLPUSH from `job_queue` into `job_queue:processing`.
         This atomic transfer guarantees the job is NEVER lost if a consumer crashes mid-flight.
      3. Consumer executes task and persists result to database.
      4. Only AFTER durable database write succeeds, consumer calls `ack_job(job_id)` -> LREM from `job_queue:processing`.
      5. If a worker dies or exceeds its lease, the coordinator can call `requeue_job(job_id)`.
    """

    def __init__(
        self,
        host: str = REDIS_HOST,
        port: int = REDIS_PORT,
        db: int = REDIS_DB,
        queue_name: str = QUEUE_NAME,
        processing_queue: str = PROCESSING_QUEUE_NAME,
        max_retries: int = 3,
        retry_delay_seconds: float = 2.0,
        redis_client: Optional[redis.Redis] = None,
    ) -> None:
        """Initialize Redis connection client with retry resilience."""
        self.queue_name = queue_name
        self.processing_queue = processing_queue
        if redis_client is not None:
            self.redis_client = redis_client
        else:
            self.redis_client = redis.Redis(
                host=host,
                port=port,
                db=db,
                decode_responses=True,
                protocol=2,
                socket_connect_timeout=2.0,
                socket_timeout=5.0,
            )

        # Resilient connection check
        connected = False
        for attempt in range(1, max_retries + 1):
            try:
                self.redis_client.ping()
                connected = True
                break
            except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as exc:
                logger.warning(
                    "Redis connection attempt %d/%d failed (%s). Retrying in %.1fs...",
                    attempt,
                    max_retries,
                    exc,
                    retry_delay_seconds,
                )
                time.sleep(retry_delay_seconds)

        if connected:
            logger.info(
                "Initialized JobQueue connection to Redis at %s:%d (queue='%s', processing='%s')",
                host,
                port,
                queue_name,
                processing_queue,
            )
        else:
            logger.error("Could not connect to Redis at %s:%d after %d attempts.", host, port, max_retries)

    def push_job(self, job_id: str) -> None:
        """Push a job_id onto the left of the Redis list (Producer action)."""
        self.redis_client.lpush(self.queue_name, job_id)
        logger.info("Enqueued job %s onto '%s'", job_id, self.queue_name)

    def claim_job(self, timeout: int = REDIS_BRPOP_TIMEOUT_SECONDS) -> Optional[str]:
        """Atomically pop a job from queue_name and push onto processing_queue (Consumer claim).

        Uses BRPOPLPUSH so the item is never lost if the consumer crashes immediately after claiming.
        """
        try:
            job_id = self.redis_client.brpoplpush(self.queue_name, self.processing_queue, timeout=timeout)
            if job_id:
                logger.info("Claimed job %s into processing list '%s'", job_id, self.processing_queue)
                return job_id
            return None
        except (redis.exceptions.TimeoutError, redis.exceptions.ConnectionError):
            return None

    def pop_job(self, timeout: int = REDIS_BRPOP_TIMEOUT_SECONDS) -> Optional[str]:
        """Backward-compatible alias for claim_job()."""
        return self.claim_job(timeout=timeout)

    def register_claim(
        self,
        job_id: str,
        claim_token: str,
        lease_seconds: float = 30.0,
        ttl_seconds: Optional[float] = None,
    ) -> None:
        """Record the active claim token in Redis with a TTL slightly longer than lease duration."""
        try:
            claim_key = f"job_claim:{job_id}"
            duration = ttl_seconds if ttl_seconds is not None else lease_seconds
            self.redis_client.set(claim_key, claim_token, ex=int(duration + 10))
        except Exception as exc:
            logger.warning("Failed to record claim token in Redis for job %s: %s", job_id, exc)

    def ack_job(self, job_id: str, claim_token: Optional[str] = None) -> bool:
        """Acknowledge job completion by removing it from the processing queue.

        If claim_token is provided, verifies that the caller owns the active claim.
        If another worker has reclaimed the job (token mismatch), the ACK is rejected
        to prevent evicting the newer attempt's processing entry.
        """
        try:
            claim_key = f"job_claim:{job_id}"
            if claim_token is not None:
                recorded = self.redis_client.get(claim_key)
                if recorded is not None and recorded != claim_token:
                    logger.warning(
                        "ACK rejected for job %s: claim token mismatch (caller=%s, recorded=%s)",
                        job_id,
                        claim_token,
                        recorded,
                    )
                    return False
                self.redis_client.delete(claim_key)

            removed = self.redis_client.lrem(self.processing_queue, 1, job_id)
            if removed and int(removed) > 0:
                logger.info("Acknowledged job %s (removed from '%s')", job_id, self.processing_queue)
                return True
            return False
        except Exception as exc:
            logger.error("Failed to ACK job %s: %s", job_id, exc)
            return False

    def requeue_job(self, job_id: str) -> None:
        """Atomically remove a job from processing_queue and push it back to queue_name."""
        try:
            claim_key = f"job_claim:{job_id}"
            pipe = self.redis_client.pipeline()
            pipe.delete(claim_key)
            pipe.lrem(self.processing_queue, 1, job_id)
            pipe.lpush(self.queue_name, job_id)
            pipe.execute()
            logger.info("Requeued job %s from '%s' back to '%s'", job_id, self.processing_queue, self.queue_name)
        except Exception as exc:
            logger.error("Failed to requeue job %s: %s", job_id, exc)

    def remove_from_processing(self, job_id: str) -> bool:
        """Remove a job directly from processing_queue (e.g. when moving to dead_letter)."""
        try:
            claim_key = f"job_claim:{job_id}"
            self.redis_client.delete(claim_key)
            removed = self.redis_client.lrem(self.processing_queue, 1, job_id)
            return bool(removed > 0)
        except Exception as exc:
            logger.error("Failed to remove job %s from processing queue: %s", job_id, exc)
            return False

    def queue_length(self) -> int:
        """Return current number of pending jobs in the queue."""
        try:
            return self.redis_client.llen(self.queue_name)
        except Exception:
            return -1

    def processing_length(self) -> int:
        """Return current number of in-flight claimed jobs in the processing queue."""
        try:
            return self.redis_client.llen(self.processing_queue)
        except Exception:
            return -1

    def get_processing_jobs(self) -> list:
        """Return all job IDs currently in the processing queue."""
        try:
            return self.redis_client.lrange(self.processing_queue, 0, -1)
        except Exception:
            return []
