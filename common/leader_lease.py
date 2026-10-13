"""
Leader Lease and Monotonic Fencing Epoch Manager (Unit III — Synchronization).

Why Leader Leases alone are NOT Fencing:
----------------------------------------
In an asynchronous distributed system with unpredictable network delays and GC pauses:
1. Leader A acquires a 6-second lease.
2. Leader A encounters a 7-second stop-the-world GC pause.
3. The lease expires in Redis.
4. Leader B acquires the lease, increments the fencing epoch (e.g. from 10 to 11),
   and starts authoritative operations.
5. Leader A wakes up. In its own memory, it believes it is still leader.
6. If Leader A issues delayed writes to the database, it would corrupt state.

Solution: Fencing Tokens (Epochs)
---------------------------------
Every time a leader acquires a lease, a monotonically increasing epoch number is generated.
All authoritative state transitions in the persistent datastore (SQLite) require:
  WHERE ... AND leader_epoch >= last_seen_epoch.
When Leader B increments the epoch, Leader A's delayed writes are definitively rejected.

Redis Persistence and Restart Assumptions:
------------------------------------------
1. Unpersisted Redis Restart: If Redis restarts without AOF persistence enabled,
   in-memory keys (including the epoch counter) reset to 0.
2. Floor Recovery: To prevent new leaders from acquiring regressed epochs (< previously
   committed SQLite epochs), `try_acquire_lease` queries the durable highest accepted
   epoch from SQLite (`min_epoch`).
3. Atomic Floor Initialization: The Redis Lua script compares the Redis counter against
   `min_epoch`. If `current_epoch < min_epoch`, it sets the counter to `min_epoch` before
   calling `INCR`. Because Redis executes Lua scripts atomically, this guarantees monotonic
   fencing tokens across Redis crashes without race conditions or counter regressions.
4. Old Leader Fencing: An old leader cannot renew or release a newer leader's lease because
   `renew_lease` and `release_lease` check both `leader_id` and exact `epoch`.
"""

import json
import logging
import threading
import time
from typing import Any, Dict, Optional, Tuple

import redis

from common.config import (
    LEADER_EPOCH_KEY,
    LEADER_LEASE_KEY,
    LEADER_LEASE_TTL_SECONDS,
    LEADER_RENEWAL_INTERVAL_SECONDS,
    REDIS_HOST,
    REDIS_PORT,
    REDIS_DB,
)
from common.logging_config import setup_logger

logger = setup_logger("leader_lease")


class LeaderLeaseManager:
    """Manages leader lease acquisition, renewal, and fencing epochs backed by Redis."""

    def __init__(
        self,
        leader_id: str,
        redis_client: Optional[redis.Redis] = None,
        lease_key: str = LEADER_LEASE_KEY,
        epoch_key: str = LEADER_EPOCH_KEY,
        ttl_seconds: float = LEADER_LEASE_TTL_SECONDS,
        renewal_interval: float = LEADER_RENEWAL_INTERVAL_SECONDS,
    ) -> None:
        self.leader_id = leader_id
        self.lease_key = lease_key
        self.epoch_key = epoch_key
        self.ttl_seconds = float(ttl_seconds)
        self.renewal_interval = float(renewal_interval)
        self._is_leader = False
        self._current_epoch = 0
        self._lock = threading.Lock()

        if redis_client is not None:
            self.redis_client = redis_client
        else:
            self.redis_client = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                db=REDIS_DB,
                decode_responses=True,
                protocol=2,
                socket_timeout=1.0,
            )

    @property
    def is_leader(self) -> bool:
        with self._lock:
            return self._is_leader

    @property
    def current_epoch(self) -> int:
        with self._lock:
            return self._current_epoch

    def force_leadership(self, epoch: int = 1) -> None:
        """Explicitly assign leadership to this instance (primarily for testing and standalone mode)."""
        with self._lock:
            self._is_leader = True
            self._current_epoch = epoch

    def step_down(self) -> None:
        """Relinquish local leadership without deleting Redis keys."""
        with self._lock:
            self._is_leader = False

    def try_acquire_lease(self, min_epoch: Optional[int] = None) -> Tuple[bool, int]:
        """Atomically attempt to acquire the leadership lease using Redis.

        Recovers fencing epoch floor from SQLite if Redis has restarted, ensuring
        epochs never regress below durable SQLite records even after Redis state loss.

        Returns (acquired: bool, epoch: int).
        """
        # If min_epoch is not explicitly provided, read the durable epoch floor from SQLite
        if min_epoch is None:
            try:
                from common.database import get_current_fencing_epoch
                durable_record = get_current_fencing_epoch(scope="scheduler_recovery")
                min_epoch = int(durable_record["epoch"]) if durable_record and "epoch" in durable_record else 0
            except Exception as dberr:
                logger.debug("Could not read durable fencing epoch floor from SQLite: %s", dberr)
                min_epoch = 0

        # Lua script: Atomic test-and-set with epoch floor guarantee and monotonic increment
        lua_acquire = """
        local current = redis.call('GET', KEYS[1])
        local min_epoch = tonumber(ARGV[3]) or 0

        if not current then
            local current_epoch = tonumber(redis.call('GET', KEYS[2])) or 0
            if current_epoch < min_epoch then
                redis.call('SET', KEYS[2], min_epoch)
            end
            local epoch = redis.call('INCR', KEYS[2])
            local payload = cjson.encode({leader_id = ARGV[1], epoch = epoch})
            redis.call('SET', KEYS[1], payload, 'EX', tonumber(ARGV[2]))
            return {1, epoch}
        else
            local data = cjson.decode(current)
            if data.leader_id == ARGV[1] then
                redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
                return {1, data.epoch}
            else
                return {0, data.epoch}
            end
        end
        """
        try:
            res = self.redis_client.eval(
                lua_acquire,
                2,
                self.lease_key,
                self.epoch_key,
                self.leader_id,
                int(self.ttl_seconds),
                int(min_epoch or 0),
            )
            acquired = bool(res[0] == 1)
            epoch = int(res[1])

            with self._lock:
                was_leader = self._is_leader
                self._is_leader = acquired
                self._current_epoch = epoch

            if acquired and not was_leader:
                logger.info(
                    "LEADERSHIP ACQUIRED: Instance %s is now LEADER (Epoch %d, TTL=%.1fs)",
                    self.leader_id,
                    epoch,
                    self.ttl_seconds,
                )
            return acquired, epoch
        except Exception as exc:
            # Redis failure / disconnect -> step down immediately for safety
            with self._lock:
                if self._is_leader:
                    logger.warning("Lost Redis connection. Stepping down from leadership: %s", exc)
                self._is_leader = False
            return False, self._current_epoch

    def renew_lease(self) -> bool:
        """Renew active leadership lease if this instance is still recognized in Redis."""
        with self._lock:
            if not self._is_leader:
                return False
            active_epoch = self._current_epoch

        lua_renew = """
        local current = redis.call('GET', KEYS[1])
        if current then
            local data = cjson.decode(current)
            if data.leader_id == ARGV[1] and data.epoch == tonumber(ARGV[2]) then
                redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]))
                return 1
            end
        end
        return 0
        """
        try:
            res = self.redis_client.eval(
                lua_renew,
                1,
                self.lease_key,
                self.leader_id,
                active_epoch,
                int(self.ttl_seconds),
            )
            success = bool(res == 1)
            with self._lock:
                if not success and self._is_leader:
                    logger.warning(
                        "LEADERSHIP LOST: Instance %s failed lease renewal (expired or superseded)",
                        self.leader_id,
                    )
                    self._is_leader = False
            return success
        except Exception as exc:
            logger.error("Error during lease renewal: %s", exc)
            with self._lock:
                self._is_leader = False
            return False

    def release_lease(self) -> bool:
        """Voluntarily surrender leadership lease upon graceful shutdown."""
        with self._lock:
            if not self._is_leader:
                return False
            active_epoch = self._current_epoch
            self._is_leader = False

        lua_release = """
        local current = redis.call('GET', KEYS[1])
        if current then
            local data = cjson.decode(current)
            if data.leader_id == ARGV[1] and data.epoch == tonumber(ARGV[2]) then
                redis.call('DEL', KEYS[1])
                return 1
            end
        end
        return 0
        """
        try:
            res = self.redis_client.eval(
                lua_release,
                1,
                self.lease_key,
                self.leader_id,
                active_epoch,
            )
            logger.info("LEADERSHIP RELEASED gracefully by instance %s (Epoch %d)", self.leader_id, active_epoch)
            return bool(res == 1)
        except Exception as exc:
            logger.error("Error releasing lease: %s", exc)
            return False

    def get_leader_status(self) -> Dict[str, Any]:
        """Return current cluster leadership diagnostics."""
        try:
            raw = self.redis_client.get(self.lease_key)
            ttl = self.redis_client.ttl(self.lease_key)
            if raw:
                try:
                    data = json.loads(raw) if isinstance(raw, str) else raw
                except Exception:
                    data = {"raw": str(raw)}
                return {
                    "is_leader": bool(data.get("leader_id") == self.leader_id),
                    "current_leader": data.get("leader_id"),
                    "epoch": data.get("epoch", self._current_epoch),
                    "lease_ttl_remaining_seconds": max(0, ttl),
                    "local_instance_id": self.leader_id,
                    "status": "active_leader" if data.get("leader_id") == self.leader_id else "standby",
                }
            return {
                "is_leader": False,
                "current_leader": None,
                "epoch": self._current_epoch,
                "lease_ttl_remaining_seconds": 0,
                "local_instance_id": self.leader_id,
                "status": "no_leader",
            }
        except Exception as exc:
            return {
                "is_leader": self.is_leader,
                "error": str(exc),
                "local_instance_id": self.leader_id,
                "status": "unreachable",
            }
