"""LeaseWatcher: background coroutine that reclaims orphaned in-flight jobs.

When a dispatcher worker crashes mid-dispatch (or is killed hard), the lease
key expires but the slot counter is not decremented and the job hash is left
in status=dispatched. The lease watcher scans the inflight ZSET every
`lease_watcher_interval_seconds` and reclaims any job whose lease has expired.
"""

from __future__ import annotations

import asyncio
import datetime
import time
from typing import TYPE_CHECKING

import redis.asyncio as aioredis

from app.config import AppConfig
from app.observability import metrics as m
from app.observability.jsonl_logger import JSONLLogger, build_base_event
from app.state import ErrorClass, EventType, JobStatus
from app.storage.redis_keys import (
    dlq_key,
    inflight_key,
    job_done_channel,
    lease_key,
    slots_in_use_key,
)


class LeaseWatcher:
    """Periodic background task that reclaims orphaned dispatched jobs."""

    def __init__(
        self,
        redis: aioredis.Redis,
        config: AppConfig,
        logger: JSONLLogger,
    ) -> None:
        self._redis = redis
        self._config = config
        self._logger = logger
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    def start(self) -> asyncio.Task[None]:
        """Start the lease watcher as an asyncio background task."""
        self._task = asyncio.create_task(self._run(), name="lease-watcher")
        return self._task

    async def stop(self, timeout: float = 5.0) -> None:
        """Signal stop and wait for the task to exit."""
        self._stop.set()
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=timeout)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._task.cancel()

    async def _run(self) -> None:
        """Periodic scan loop."""
        interval = self._config.scheduler.lease_watcher_interval_seconds
        while not self._stop.is_set():
            try:
                await self._scan_all_models()
            except Exception as exc:
                m.record_redis_error("lease_watcher_scan")
            try:
                await asyncio.wait_for(
                    asyncio.shield(asyncio.Event().wait()),
                    timeout=float(interval),
                )
            except asyncio.TimeoutError:
                pass  # expected — just the sleep

    async def _scan_all_models(self) -> None:
        """Scan all configured models for expired leases."""
        for model in self._config.all_model_names():
            await self._scan_model(model)

    async def _scan_model(self, model: str) -> None:
        """Find inflight jobs whose lease key has expired and reclaim them."""
        model_cfg = self._config.model_config_for(model)
        lease_ttl = max(model_cfg.sync_timeout_seconds, 60) + 30
        now = time.time()
        # Jobs dispatched more than (lease_ttl) seconds ago are candidates
        cutoff = now - lease_ttl

        try:
            candidates = await self._redis.zrangebyscore(
                inflight_key(model),
                min=0,
                max=cutoff,
            )
        except aioredis.RedisError:
            m.record_redis_error("zrangebyscore_inflight")
            return

        for raw_job_id in candidates:
            job_id: str = raw_job_id.decode() if isinstance(raw_job_id, bytes) else raw_job_id
            await self._reclaim_if_orphaned(model, job_id, cutoff, now)

    async def _reclaim_if_orphaned(
        self, model: str, job_id: str, cutoff: float, now: float
    ) -> None:
        """Check lease key; if absent, mark the job failed/worker_crash."""
        lkey = lease_key(job_id)
        try:
            lease_exists = await self._redis.exists(lkey)
        except aioredis.RedisError:
            m.record_redis_error("lease_exists_check")
            return

        if lease_exists:
            return  # lease is still alive — worker is just slow

        # Lease expired without being cleared → worker crash
        # Determine how long ago it was dispatched
        try:
            score_raw = await self._redis.zscore(inflight_key(model), job_id)
        except aioredis.RedisError:
            score_raw = None
        dispatch_epoch = float(score_raw) if score_raw is not None else (now - 60)
        lease_age_ms = int((now - dispatch_epoch) * 1000)

        # Mark failed
        complete_ts = _iso_now()
        try:
            async with self._redis.pipeline() as pipe:
                pipe.hset(
                    f"job:{job_id}",
                    mapping={
                        "status": JobStatus.FAILED.value,
                        "error_class": ErrorClass.WORKER_CRASH.value,
                        "complete_ts": complete_ts,
                    },
                )
                pipe.expire(f"job:{job_id}", 3600)
                pipe.decr(slots_in_use_key(model))
                pipe.zrem(inflight_key(model), job_id)
                pipe.sadd(dlq_key(model), job_id)
                await pipe.execute()
        except aioredis.RedisError:
            m.record_redis_error("lease_reclaim_write")
            return

        # Publish terminal signal
        try:
            await self._redis.publish(job_done_channel(job_id), JobStatus.FAILED.value)
        except aioredis.RedisError:
            m.record_redis_error("lease_reclaim_publish")

        # Update metrics
        m.record_worker_crash(model)
        m.record_failure(model, ErrorClass.WORKER_CRASH.value)

        # Retrieve caller info for log
        try:
            caller_raw = await self._redis.hget(f"job:{job_id}", "caller")
            caller = caller_raw.decode() if isinstance(caller_raw, bytes) else (caller_raw or "")
            req_id_raw = await self._redis.hget(f"job:{job_id}", "request_id")
            request_id = req_id_raw.decode() if isinstance(req_id_raw, bytes) else (req_id_raw or "")
            prio_raw = await self._redis.hget(f"job:{job_id}", "priority")
            priority = int(prio_raw) if prio_raw else 5
        except Exception:
            caller, request_id, priority = "", "", 5

        await self._logger.emit(
            {
                **build_base_event(
                    EventType.WORKER_CRASH.value, job_id, request_id, model, caller, {}, priority
                ),
                "lease_age_ms": lease_age_ms,
            }
        )

        # Update DLQ gauge
        try:
            dlq_size = await self._redis.scard(dlq_key(model))
            m.update_dlq_depth(model, int(dlq_size or 0))
        except aioredis.RedisError:
            pass


def _iso_now() -> str:
    """Current UTC time as ISO8601 string."""
    return datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
