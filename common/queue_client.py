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
    REDIS_BRPOP_TIMEOUT_SECONDS,
)
from common.logging_config import setup_logger

logger = setup_logger("queue_client")


class JobQueue:
    """Wrapper around Redis providing Producer-Consumer queue primitives with connection resilience."""

    def __init__(
        self,
        host: str = REDIS_HOST,
        port: int = REDIS_PORT,
        db: int = REDIS_DB,
        queue_name: str = QUEUE_NAME,
        max_retries: int = 3,
        retry_delay_seconds: float = 2.0,
    ) -> None:
        """Initialize Redis connection client with retry resilience.

        Parameters
        ----------
        host:
            Redis hostname.
        port:
            Redis port.
        db:
            Redis database index.
        queue_name:
            Redis list key name for jobs.
        max_retries:
            Attempts to connect before raising an error.
        retry_delay_seconds:
            Delay between connection retry attempts.
        """
        self.queue_name = queue_name
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
                "Initialized JobQueue connection to Redis at %s:%d (db=%d, queue='%s')",
                host,
                port,
                db,
                queue_name,
            )
        else:
            logger.error("Could not connect to Redis at %s:%d after %d attempts.", host, port, max_retries)

    def push_job(self, job_id: str) -> None:
        """Push a job_id onto the left of the Redis list (Producer action).

        Parameters
        ----------
        job_id:
            Unique identifier of the job to execute.
        """
        self.redis_client.lpush(self.queue_name, job_id)
        logger.info("Enqueued job %s onto '%s'", job_id, self.queue_name)

    def pop_job(self, timeout: int = REDIS_BRPOP_TIMEOUT_SECONDS) -> Optional[str]:
        """Block-wait and pop a job_id from the right of the Redis list (Consumer action).

        Parameters
        ----------
        timeout:
            Max seconds to block waiting for an item.

        Returns
        -------
        str or None
            The job_id string if popped within timeout, else None.
        """
        try:
            result = self.redis_client.brpop(self.queue_name, timeout=timeout)
            if result:
                _, job_id = result
                return job_id
            return None
        except (redis.exceptions.TimeoutError, redis.exceptions.ConnectionError):
            # Socket timeout during BRPOP is normal when idle or if Redis socket resets
            return None

    def queue_length(self) -> int:
        """Return current number of pending jobs in the queue."""
        try:
            return self.redis_client.llen(self.queue_name)
        except Exception:
            return -1
