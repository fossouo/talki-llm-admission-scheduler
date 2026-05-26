"""IntegrityChecker: 60s reconciliation of slot counters vs actual inflight.

If the slots_in_use counter has drifted above the actual number of inflight
jobs (e.g. due to a crash that bypassed normal cleanup), this background
coroutine hard-resets the counter to the correct value.
"""

from __future__ import annotations

import asyncio

import redis.asyncio as aioredis

from app.config import AppConfig
from app.observability import metrics as m
from app.observability.jsonl_logger import JSONLLogger
from app.storage.redis_keys import inflight_key, slots_in_use_key


class IntegrityChecker:
    """Periodic counter reconciliation between slots:*:in_use and inflight ZSETs."""

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
        """Start the integrity checker as a background asyncio task."""
        self._task = asyncio.create_task(self._run(), name="integrity-checker")
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
        """Periodic reconciliation loop."""
        interval = self._config.scheduler.integrity_check_interval_seconds
        while not self._stop.is_set():
            try:
                await self._check_all_models()
            except Exception:
                m.record_redis_error("integrity_check")
            try:
                await asyncio.wait_for(
                    asyncio.shield(asyncio.Event().wait()),
                    timeout=float(interval),
                )
            except asyncio.TimeoutError:
                pass  # expected — this is the sleep

    async def _check_all_models(self) -> None:
        """Run reconciliation for each configured model."""
        for model in self._config.all_model_names():
            await self._check_model(model)

    async def _check_model(self, model: str) -> None:
        """Reconcile slots:*:in_use against ZCARD inflight:*."""
        in_use_key = slots_in_use_key(model)
        ifl_key = inflight_key(model)

        try:
            actual_raw, recorded_raw = await asyncio.gather(
                self._redis.zcard(ifl_key),
                self._redis.get(in_use_key),
            )
        except aioredis.RedisError:
            m.record_redis_error("integrity_read")
            return

        actual = int(actual_raw or 0)
        recorded = int(recorded_raw or 0)

        # Tolerance: allow up to 2 in-flight that haven't been added to inflight ZSET yet
        if recorded > actual + 2:
            # Hard reset to truth
            try:
                await self._redis.set(in_use_key, actual)
            except aioredis.RedisError:
                m.record_redis_error("integrity_write")
                return

            m.record_redis_error("drift_correction")
            await self._logger.emit(
                {
                    "event": "slot_drift_corrected",
                    "model": model,
                    "recorded": recorded,
                    "actual": actual,
                }
            )

        # Always update the Prometheus gauge to reflect current state
        model_cfg = self._config.model_config_for(model)
        m.update_slot_gauges(model, actual, model_cfg.max_concurrency)

        # Update queue depth gauge
        try:
            from app.storage.redis_keys import queue_key
            depth = await self._redis.zcard(queue_key(model))
            m.update_queue_depth(model, int(depth or 0))
        except aioredis.RedisError:
            pass
