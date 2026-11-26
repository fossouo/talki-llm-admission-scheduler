from __future__ import annotations
import datetime
import hashlib
import time
import json
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

PRIORITY_MULTIPLIER = 1_000_000_000_000

class AdmissionController:
    def __init__(self, redis: aioredis.Redis, config: AppConfig, lua: "LuaScripts", logger: JSONLLogger) -> None:
        self._redis = redis
        self._config = config
        self._lua = lua
        self._logger = logger

    async def admit(self, *, model: str, payload: dict[str, Any], priority: int | None, caller: str, caller_metadata: dict[str, Any] | None, idempotency_key: str | None) -> tuple[AdmissionOutcome, JobRequest | None]:
        model_cfg = self._config.model_config_for(model)
        effective_priority = priority if priority is not None else model_cfg.priority
        request_id = uuid7()
        if idempotency_key:
            idem_hash = hashlib.sha256(idempotency_key.encode()).hexdigest()
            ikey = idem_key(idem_hash)
            existing_job_id = await self._redis.get(ikey)
            if existing_job_id is not None:
                jid = existing_job_id.decode() if isinstance(existing_job_id, bytes) else existing_job_id
                m.record_admission(model, AdmissionResult.IDEMPOTENT_HIT.value)
                return (AdmissionOutcome(result=AdmissionResult.IDEMPOTENT_HIT, job_id=jid, status=JobStatus.QUEUED, existing_job_id=jid), None)
        
        job_id = uuid7()
        job_req = JobRequest(job_id=job_id, request_id=request_id, model=model, payload=payload, priority=effective_priority, caller=caller, caller_metadata=caller_metadata or {}, idempotency_key=idempotency_key)
        
        queue_k = queue_key(model)
        from app.storage.redis_keys import job_key as jk
        seq = await self._redis.incr(seq_key(model))
        enqueue_ns = int(time.time_ns()) % PRIORITY_MULTIPLIER
        score = float(effective_priority * PRIORITY_MULTIPLIER + enqueue_ns + (seq % 1000))
        enqueue_ts = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
        
        # FIX: Include payload_json in field_values for the dispatcher worker
        field_values = [
            "model", model,
            "status", JobStatus.QUEUED.value,
            "priority", str(effective_priority),
            "enqueue_ts", enqueue_ts,
            "caller", caller,
            "request_id", request_id,
            "payload_json", json.dumps(payload)
        ]
        
        queued = await self._lua.evalsha_enqueue_job(self._redis, queue_k, jk(job_id), job_id, score, model_cfg.max_queue_depth, field_values)
        if not queued:
            queue_depth = await self._redis.zcard(queue_k)
            m.record_admission(model, AdmissionResult.REJECTED.value)
            await self._logger.emit({**build_base_event("rejected", job_id, request_id, model, caller, caller_metadata, effective_priority), "queue_depth": queue_depth, "capacity": model_cfg.max_queue_depth})
            return (AdmissionOutcome(result=AdmissionResult.REJECTED, job_id=job_id, status=JobStatus.REJECTED, estimated_drain_s=60), job_req)
            
        if idempotency_key:
            idem_hash = hashlib.sha256(idempotency_key.encode()).hexdigest()
            await self._redis.set(idem_key(idem_hash), job_id, ex=86400, nx=True)
            
        queue_pos = await self._redis.zrank(queue_k, job_id)
        m.record_admission(model, AdmissionResult.QUEUED.value)
        current_depth = await self._redis.zcard(queue_k)
        await self._logger.emit({**build_base_event("queued", job_id, request_id, model, caller, caller_metadata, effective_priority), "queue_depth_at_admission": current_depth})
        return (AdmissionOutcome(result=AdmissionResult.QUEUED, job_id=job_id, status=JobStatus.QUEUED, position_in_queue=int(queue_pos) if queue_pos is not None else 0), job_req)
