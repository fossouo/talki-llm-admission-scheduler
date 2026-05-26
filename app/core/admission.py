"""AdmissionController: single entry point for all job submissions.

Both the sync POST /v1/chat/completions path and the async POST /jobs path
call admit(). The controller:
  1. Checks idempotency — returns cached job_id on key hit.
  2. Always enqueues via ZADD (single code path, avoids race between
     "admit-direct" and "worker-claims"). The dispatcher worker picks
     up immediately via BZPOPMIN.
  3. Rejects with 429 if the queue is at max_queue_depth.
"""

from __future__ import annotations

import datetime
import hashlib
import time
from typing import TYPE_CHECKING, Any

import redis.asyncio as aioredis

from app.config import AppConfig
from app.core.uuid7 import uuid7
from app.observability import metrics as m
from app.observability.jsonl_logger import JSONLLogger, build_base_event
from app.state import AdmissionOutcome, AdmissionResult, JobRequest, JobStatus
from app.storage.redis_keys import idem_key, queue_key, seq_key

if TYPE_CHECKING:
    from app.storage.redis_lua import LuaScripts

# Priority tier multiplier: score = tier * PRIORITY_MULTIPLIER + enqueue_ns_truncated + seq
PRIORITY_MULTIPLIER = 1_000_000_000_000  # 1e12


class AdmissionController:
    """Controls job admission into the priority queue."""

    def __init__(
        self,
        redis: aioredis.Redis,
        config: AppConfig,
        lua: "LuaScripts",
        logger: JSONLLogger,
    ) -> None:
        self._redis = redis
        self._config = config
        self._lua = lua
        self._logger = logger

    async def admit(
        self,
        *,
        model: str,
        payload: dict[str, Any],
        priority: int | None,
        caller: str,
        caller_metadata: dict[str, Any] | None,
        idempotency_key: str | None,
    ) -> tuple[AdmissionOutcome, JobRequest | None]:
        """Admit a request: check idempotency, enqueue, or reject.

        Returns (outcome, job_request). job_request is None on idempotency hit
        (no new job was created).
        """
        model_cfg = self._config.model_config_for(model)
        effective_priority = priority if priority is not None else model_cfg.priority

        request_id = uuid7()

        # 1. Idempotency check
        if idempotency_key:
            idem_hash = _sha256(idempotency_key)
            ikey = idem_key(idem_hash)
            existing_job_id: bytes | None = await self._redis.get(ikey)
            if existing_job_id is not None:
                jid = (
                    existing_job_id.decode()
                    if isinstance(existing_job_id, bytes)
                    else existing_job_id
                )
                m.record_admission(model, AdmissionResult.IDEMPOTENT_HIT.value)
                return (
                    AdmissionOutcome(
                        result=AdmissionResult.IDEMPOTENT_HIT,
                        job_id=jid,
                        status=JobStatus.QUEUED,
                        existing_job_id=jid,
                    ),
                    None,
                )

        job_id = uuid7()
        job_req = JobRequest(
            job_id=job_id,
            request_id=request_id,
            model=model,
            payload=payload,
            priority=effective_priority,
            caller=caller,
            caller_metadata=caller_metadata or {},
            idempotency_key=idempotency_key,
        )

        # 2. Try to enqueue atomically (rejects if at max_queue_depth)
        queue_k = queue_key(model)
        from app.storage.redis_keys import job_key as jk

        # Composite score: priority_tier * 1e12 + enqueue_epoch_ns (lower = higher urgency)
        seq = await self._redis.incr(seq_key(model))
        enqueue_ns = int(time.time_ns()) % PRIORITY_MULTIPLIER
        # Ensure seq contributes only as a tie-breaker within the same nanosecond
        score = float(effective_priority * PRIORITY_MULTIPLIER + enqueue_ns + (seq % 1000))

        enqueue_ts = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
        field_values = [
            "model", model,
            "status", JobStatus.QUEUED.value,
            "priority", str(effective_priority),
            "enqueue_ts", enqueue_ts,
            "caller", caller,
            "request_id", request_id,
        ]

        queued = await self._lua.evalsha_enqueue_job(
            self._redis,
            queue_k,
            jk(job_id),
            job_id,
            score,
            model_cfg.max_queue_depth,
            field_values,
        )

        if not queued:
            # Queue is full — reject
            queue_depth = await self._redis.zcard(queue_k)
            estimated_drain_s = _estimate_drain(
                int(queue_depth or 0), model_cfg.max_concurrency
            )
            m.record_admission(model, AdmissionResult.REJECTED.value)
            await self._logger.emit(
                {
                    **build_base_event(
                        "rejected", job_id, request_id, model, caller, caller_metadata, effective_priority
                    ),
                    "queue_depth": queue_depth,
                    "capacity": model_cfg.max_queue_depth,
                }
            )
            return (
                AdmissionOutcome(
                    result=AdmissionResult.REJECTED,
                    job_id=job_id,
                    status=JobStatus.REJECTED,
                    estimated_drain_s=estimated_drain_s,
                ),
                job_req,
            )

        # 3. Record idempotency key if provided
        if idempotency_key:
            idem_hash = _sha256(idempotency_key)
            await self._redis.set(idem_key(idem_hash), job_id, ex=86400, nx=True)

        # 4. Get current queue position for response
        queue_pos = await self._redis.zrank(queue_k, job_id)

        m.record_admission(model, AdmissionResult.QUEUED.value)
        current_depth = await self._redis.zcard(queue_k)
        await self._logger.emit(
            {
                **build_base_event(
                    "queued", job_id, request_id, model, caller, caller_metadata, effective_priority
                ),
                "queue_depth_at_admission": current_depth,
            }
        )

        return (
            AdmissionOutcome(
                result=AdmissionResult.QUEUED,
                job_id=job_id,
                status=JobStatus.QUEUED,
                position_in_queue=int(queue_pos) if queue_pos is not None else 0,
            ),
            job_req,
        )


def _sha256(value: str) -> str:
    """Return hex SHA256 digest of a string."""
    return hashlib.sha256(value.encode()).hexdigest()


def _estimate_drain(queue_depth: int, max_concurrency: int) -> int:
    """Rough estimate of drain time in seconds (assume 10s avg per job)."""
    if max_concurrency < 1:
        return 60
    avg_job_s = 10
    return max(5, (queue_depth // max(1, max_concurrency)) * avg_job_s)
