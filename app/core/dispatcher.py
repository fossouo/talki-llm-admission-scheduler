"""DispatcherWorker: one asyncio task per model.

Loops on BZPOPMIN, claims a concurrency slot atomically via Lua, forwards
the job to LiteLLM, and marks the terminal state in Redis.
"""

from __future__ import annotations

import asyncio
import datetime
import time
import uuid
from typing import Any

import httpx
import redis.asyncio as aioredis

from app.backend.litellm_forwarder import LiteLLMForwarder
from app.config import AppConfig
from app.observability import metrics as m
from app.observability.jsonl_logger import (
    JSONLLogger,
    build_base_event,
    build_completed_event,
    build_dispatched_event,
)
from app.state import ErrorClass, EventType, JobStatus
from app.storage.job_store import JobStore
from app.storage.redis_keys import (
    dlq_key,
    inflight_key,
    job_done_channel,
    lease_key,
    queue_key,
    slots_capacity_key,
    slots_in_use_key,
)
from app.storage.redis_lua import LuaScripts


class DispatcherWorker:
    """Asyncio worker that pops jobs from the queue and dispatches them.

    One worker is started per model at application startup.
    """

    def __init__(
        self,
        model: str,
        redis: aioredis.Redis,
        config: AppConfig,
        lua: LuaScripts,
        forwarder: LiteLLMForwarder,
        logger: JSONLLogger,
    ) -> None:
        self.model = model
        self._redis = redis
        self._config = config
        self._model_cfg = config.model_config_for(model)
        self._lua = lua
        self._forwarder = forwarder
        self._logger = logger
        self._stop = asyncio.Event()
        self._worker_id = f"{model}-{uuid.uuid4().hex[:8]}"
        self._task: asyncio.Task[None] | None = None

    @property
    def is_alive(self) -> bool:
        """Return True if the worker task is still running."""
        return self._task is not None and not self._task.done()

    def start(self) -> asyncio.Task[None]:
        """Start the dispatch loop as an asyncio task."""
        self._task = asyncio.create_task(self.run(), name=f"dispatcher-{self.model}")
        return self._task

    async def stop(self, timeout: float = 5.0) -> None:
        """Signal stop and wait for the task to exit."""
        self._stop.set()
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=timeout)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._task.cancel()

    async def run(self) -> None:
        """Main dispatch loop: pop, claim slot, forward, mark terminal."""
        poll_timeout = self._config.scheduler.worker_poll_timeout_seconds
        lease_ttl = max(self._model_cfg.sync_timeout_seconds, 60) + 30

        while not self._stop.is_set():
            try:
                item = await self._redis.bzpopmin(queue_key(self.model), timeout=poll_timeout)
            except (aioredis.RedisError, ConnectionError) as exc:
                m.record_redis_error("bzpopmin")
                await asyncio.sleep(1.0)
                continue

            if item is None:
                continue  # timeout — check _stop flag and loop

            try:
                _, job_id_bytes, _score = item
                job_id: str = job_id_bytes.decode() if isinstance(job_id_bytes, bytes) else job_id_bytes
            except Exception:
                continue

            # Wait for a free slot — atomic Lua claim
            await self._wait_for_slot_and_claim(job_id, lease_ttl)

    async def _wait_for_slot_and_claim(self, job_id: str, lease_ttl: int) -> None:
        """Claim a slot for job_id, then forward and finalize."""
        in_use_key = slots_in_use_key(self.model)
        cap_key = slots_capacity_key(self.model)

        while not self._stop.is_set():
            ts_iso = _iso_now()
            try:
                ok = await self._lua.evalsha_claim_slot(
                    self._redis, in_use_key, cap_key, job_id, ts_iso
                )
            except aioredis.RedisError:
                m.record_redis_error("evalsha_claim_slot")
                await asyncio.sleep(0.1)
                continue

            if ok == 1:
                break
            await asyncio.sleep(0.05)  # brief backoff while at capacity
        else:
            # Worker is stopping — do not pick up this job
            # Re-queue so another worker or restart can handle it
            return

        # Set lease and track inflight
        try:
            async with self._redis.pipeline() as pipe:
                pipe.setex(lease_key(job_id), lease_ttl, self._worker_id)
                pipe.zadd(inflight_key(self.model), {job_id: time.time()})
                await pipe.execute()
        except aioredis.RedisError:
            m.record_redis_error("lease_set")

        # Retrieve job payload
        try:
            job_hash = await self._redis.hgetall(f"job:{job_id}")
        except aioredis.RedisError:
            m.record_redis_error("job_hgetall")
            await self._cleanup_slot(job_id, JobStatus.FAILED, ErrorClass.WORKER_CRASH)
            return

        if not job_hash:
            # Job was cancelled or expired before we could dispatch
            await self._cleanup_slot_minimal(job_id)
            return

        def _field(key: str) -> str:
            k = key.encode() if isinstance(key, str) else key
            v = job_hash.get(k, b"")
            return v.decode() if isinstance(v, bytes) else v

        model = _field("model") or self.model
        caller = _field("caller")
        request_id = _field("request_id")
        priority_str = _field("priority")
        enqueue_ts_str = _field("enqueue_ts")
        priority = int(priority_str) if priority_str else 5

        # Compute wait time
        try:
            enqueue_ts = datetime.datetime.fromisoformat(enqueue_ts_str).timestamp()
        except Exception:
            enqueue_ts = time.time()
        wait_s = max(0.0, time.time() - enqueue_ts)

        # Log dispatched — backend_target is always None here; it is resolved
        # from the LiteLLM response headers after the forward completes.
        # kv_pressure_hint is always None in v0.2 (probe wired in v0.3).
        await self._logger.emit(
            build_dispatched_event(
                job_id,
                request_id,
                model,
                caller,
                {},
                priority,
                wait_ms=int(wait_s * 1000),
                backend_target=None,
                kv_pressure_hint=None,
            )
        )
        m.queue_wait_seconds.labels(model=model).observe(wait_s)

        # Retrieve payload from a secondary hash field (stored by admission controller)
        # The full request payload is in 'payload_json' field
        payload_json_bytes = job_hash.get(b"payload_json") or job_hash.get("payload_json", b"{}")
        import json
        try:
            payload = json.loads(
                payload_json_bytes.decode() if isinstance(payload_json_bytes, bytes) else payload_json_bytes
            )
        except Exception:
            payload = {}

        # Forward to LiteLLM — returns (response_body, backend_target)
        backend_start = time.time()
        try:
            response, backend_target = await self._forwarder.forward(
                payload,
                timeout_s=float(self._model_cfg.sync_timeout_seconds),
            )
            backend_s = time.time() - backend_start
            await self._mark_completed(
                job_id, request_id, model, caller, priority,
                response, wait_s, backend_s, backend_target,
            )
        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code
            if status_code == 429:
                error_class = ErrorClass.BACKEND_429
            elif status_code >= 500:
                error_class = ErrorClass.BACKEND_5XX
            else:
                error_class = ErrorClass.BACKEND_5XX
            await self._mark_failed(
                job_id, request_id, model, caller, priority,
                error_class, str(exc), status_code,
            )
        except httpx.TimeoutException as exc:
            await self._mark_failed(
                job_id, request_id, model, caller, priority,
                ErrorClass.BACKEND_TIMEOUT, str(exc), None,
            )
        except httpx.ConnectError as exc:
            await self._mark_failed(
                job_id, request_id, model, caller, priority,
                ErrorClass.NETWORK_ERROR, str(exc), None,
            )
        except asyncio.CancelledError:
            await self._mark_cancelled_dispatched(job_id, request_id, model, caller, priority)
            raise
        except Exception as exc:
            await self._mark_failed(
                job_id, request_id, model, caller, priority,
                ErrorClass.BACKEND_5XX, str(exc), None,
            )
        finally:
            await self._release_slot(job_id)

    async def _mark_completed(
        self,
        job_id: str,
        request_id: str,
        model: str,
        caller: str,
        priority: int,
        response: dict[str, Any],
        wait_s: float,
        backend_s: float,
        backend_target: str | None = None,
    ) -> None:
        """Write completed status and response blob, then publish.

        backend_target (v0.2): physical backend string extracted by the
        forwarder from LiteLLM response headers.  Stored in the JSONL log.
        Frequently None during Phase 0 shadow mode until the Xeon LiteLLM
        proxy build is verified to emit x-litellm-deployment headers.
        """
        import gzip, json

        complete_ts = _iso_now()
        runtime_s = backend_s

        # Extract token counts from response if present
        usage = response.get("usage") or {}
        tokens_prompt = usage.get("prompt_tokens", 0)
        tokens_completion = usage.get("completion_tokens", 0)

        blob = gzip.compress(json.dumps(response).encode())
        try:
            async with self._redis.pipeline() as pipe:
                pipe.hset(
                    f"job:{job_id}",
                    mapping={
                        "status": JobStatus.COMPLETED.value,
                        "complete_ts": complete_ts,
                        "response_blob": blob,
                    },
                )
                pipe.expire(f"job:{job_id}", 3600)
                await pipe.execute()
        except aioredis.RedisError:
            m.record_redis_error("mark_completed")

        await self._publish(job_id, JobStatus.COMPLETED.value)
        m.record_completion(model, wait_s, runtime_s, backend_s)
        await self._logger.emit(
            build_completed_event(
                job_id,
                request_id,
                model,
                caller,
                {},
                priority,
                wait_ms=int(wait_s * 1000),
                runtime_ms=int(runtime_s * 1000),
                tokens_prompt=tokens_prompt,
                tokens_completion=tokens_completion,
                backend_target=backend_target,
            )
        )

    async def _mark_failed(
        self,
        job_id: str,
        request_id: str,
        model: str,
        caller: str,
        priority: int,
        error_class: ErrorClass,
        error_message: str,
        http_status: int | None,
    ) -> None:
        """Write failed status and publish."""
        complete_ts = _iso_now()
        try:
            mapping: dict[str, Any] = {
                "status": JobStatus.FAILED.value,
                "complete_ts": complete_ts,
                "error_class": error_class.value,
                "error_message": error_message[:500],
            }
            if http_status is not None:
                mapping["http_status"] = str(http_status)
            async with self._redis.pipeline() as pipe:
                pipe.hset(f"job:{job_id}", mapping=mapping)
                pipe.expire(f"job:{job_id}", 3600)
                await pipe.execute()
        except aioredis.RedisError:
            m.record_redis_error("mark_failed")

        await self._publish(job_id, JobStatus.FAILED.value)
        m.record_failure(model, error_class.value)
        await self._logger.emit(
            {
                **build_base_event(
                    EventType.FAILED.value, job_id, request_id, model, caller, {}, priority
                ),
                "error_class": error_class.value,
                "error_message": error_message[:500],
                "http_status": http_status,
            }
        )

    async def _mark_cancelled_dispatched(
        self,
        job_id: str,
        request_id: str,
        model: str,
        caller: str,
        priority: int,
    ) -> None:
        """Mark a job cancelled while it was being dispatched."""
        complete_ts = _iso_now()
        try:
            async with self._redis.pipeline() as pipe:
                pipe.hset(
                    f"job:{job_id}",
                    mapping={"status": JobStatus.CANCELLED.value, "complete_ts": complete_ts},
                )
                pipe.expire(f"job:{job_id}", 3600)
                await pipe.execute()
        except aioredis.RedisError:
            m.record_redis_error("mark_cancelled")

        await self._publish(job_id, JobStatus.CANCELLED.value)
        m.record_cancel(model)
        await self._logger.emit(
            {
                **build_base_event(
                    EventType.CANCELLED.value, job_id, request_id, model, caller, {}, priority
                ),
                "was_dispatched": True,
            }
        )

    async def _cleanup_slot(
        self, job_id: str, status: JobStatus, error_class: ErrorClass
    ) -> None:
        """Release slot and mark job failed (used on unexpected errors)."""
        try:
            await self._redis.hset(
                f"job:{job_id}",
                mapping={
                    "status": status.value,
                    "error_class": error_class.value,
                    "complete_ts": _iso_now(),
                },
            )
        except aioredis.RedisError:
            pass
        await self._release_slot(job_id)
        await self._publish(job_id, status.value)

    async def _cleanup_slot_minimal(self, job_id: str) -> None:
        """Release slot without modifying job hash (job was already cancelled)."""
        await self._release_slot(job_id)

    async def _release_slot(self, job_id: str) -> None:
        """Decrement in-use counter, remove inflight entry, delete lease."""
        try:
            async with self._redis.pipeline() as pipe:
                pipe.decr(slots_in_use_key(self.model))
                pipe.zrem(inflight_key(self.model), job_id)
                pipe.delete(lease_key(job_id))
                await pipe.execute()
        except aioredis.RedisError:
            m.record_redis_error("release_slot")

    async def _publish(self, job_id: str, status: str) -> None:
        """Publish terminal status to the job_done:{job_id} channel."""
        try:
            await self._redis.publish(job_done_channel(job_id), status)
        except aioredis.RedisError:
            m.record_redis_error("publish")


def _iso_now() -> str:
    """Current UTC time as ISO8601 string."""
    return datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
