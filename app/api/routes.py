"""All HTTP endpoints for the admission scheduler.

Endpoints:
  POST /v1/chat/completions  — sync (default) or async (X-Submit-Async: true)
  POST /jobs                 — explicit async submission
  GET  /jobs/{job_id}        — poll status / retrieve result
  POST /jobs/{job_id}/cancel — cancel a queued or dispatched job
  GET  /queues               — queue snapshot per model
  GET  /metrics              — Prometheus exposition
  GET  /health               — liveness/readiness
"""

from __future__ import annotations

import asyncio
import datetime
import json
import time
from typing import Any

import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse, PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app.api.schemas import (
    CancelResponse,
    ChatCompletionRequest,
    HealthResponse,
    JobStatusResponse,
    JobSubmitRequest,
    JobSubmitResponse,
    ModelQueueSnapshot,
    QueuesResponse,
    WorkerStatus,
)
from app.core.admission import AdmissionController
from app.observability import metrics as m
from app.state import AdmissionResult, JobStatus
from app.storage.redis_keys import (
    dlq_key,
    inflight_key,
    job_done_channel,
    job_key,
    queue_key,
    slots_capacity_key,
    slots_in_use_key,
)

router = APIRouter()

# ---------------------------------------------------------------------------
# Dependency helpers
# ---------------------------------------------------------------------------


def get_redis(request: Request) -> aioredis.Redis:
    """FastAPI dependency: return shared Redis client from app state."""
    return request.app.state.redis


def get_admission(request: Request) -> AdmissionController:
    """FastAPI dependency: return shared AdmissionController from app state."""
    return request.app.state.admission


# ---------------------------------------------------------------------------
# POST /v1/chat/completions
# ---------------------------------------------------------------------------


@router.post("/v1/chat/completions")
async def chat_completions(
    request: Request,
    body: ChatCompletionRequest,
    x_submit_async: str | None = Header(default=None, alias="X-Submit-Async"),
    x_caller: str | None = Header(default=None, alias="X-Caller"),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    async_mode: bool = Query(default=False, alias="async"),
    redis: aioredis.Redis = Depends(get_redis),
    admission: AdmissionController = Depends(get_admission),
) -> Response:
    """OpenAI-compatible chat completion with admission control.

    Sync by default; switches to async submit-and-poll if:
      - Header X-Submit-Async: true, OR
      - Query ?async=true
    """
    use_async = async_mode or (x_submit_async or "").lower() == "true"
    caller = x_caller or "unknown"
    model = body.model
    priority = body.metadata.get("priority") if body.metadata else None
    payload = body.model_dump(exclude_none=True)

    # Persist payload so the dispatcher can retrieve it
    outcome, job_req = await admission.admit(
        model=model,
        payload=payload,
        priority=priority,
        caller=caller,
        caller_metadata=body.metadata,
        idempotency_key=idempotency_key,
    )

    if outcome.result == AdmissionResult.REJECTED:
        headers = {}
        if outcome.estimated_drain_s:
            headers["Retry-After"] = str(outcome.estimated_drain_s)
        return JSONResponse(
            status_code=429,
            content={"error": "queue_full", "job_id": outcome.job_id},
            headers=headers,
        )

    if outcome.result == AdmissionResult.IDEMPOTENT_HIT:
        job_id = outcome.existing_job_id or outcome.job_id
        return JSONResponse(
            content={"job_id": job_id, "status": "queued"},
            headers={"X-Job-Id": job_id},
        )

    # Store payload in Redis for dispatcher retrieval
    if job_req is not None:
        await _store_payload(redis, outcome.job_id, payload)

    if use_async:
        return JSONResponse(
            status_code=202,
            content={
                "job_id": outcome.job_id,
                "status": outcome.status.value,
                "position_in_queue": outcome.position_in_queue,
            },
            headers={"X-Job-Id": outcome.job_id},
        )

    # Sync: subscribe to pub/sub channel and wait for completion
    cfg = request.app.state.config
    model_cfg = cfg.model_config_for(model)
    sync_timeout = model_cfg.sync_timeout_seconds

    result = await _wait_for_completion(
        redis=redis,
        job_id=outcome.job_id,
        timeout_s=float(sync_timeout),
    )

    job_response_headers = {"X-Job-Id": outcome.job_id}

    if result is None:
        # Timeout — job continues in the queue
        m.record_timeout(model, "sync")
        return JSONResponse(
            status_code=504,
            content={
                "error": "sync_timeout",
                "job_id": outcome.job_id,
                "message": f"Request timed out after {sync_timeout}s. Poll via X-Job-Id.",
            },
            headers=job_response_headers,
        )

    if result["status"] == JobStatus.COMPLETED.value:
        response_blob = await _load_response_blob(redis, outcome.job_id)
        if response_blob:
            return JSONResponse(content=response_blob, headers=job_response_headers)

    return JSONResponse(
        status_code=_status_to_http(result["status"]),
        content={
            "error": result.get("error_class", "failed"),
            "job_id": outcome.job_id,
            "status": result["status"],
        },
        headers=job_response_headers,
    )


# ---------------------------------------------------------------------------
# POST /jobs
# ---------------------------------------------------------------------------


@router.post("/jobs", status_code=202)
async def submit_job(
    request: Request,
    body: JobSubmitRequest,
    x_caller: str | None = Header(default=None, alias="X-Caller"),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    redis: aioredis.Redis = Depends(get_redis),
    admission: AdmissionController = Depends(get_admission),
) -> JobSubmitResponse:
    """Explicit async job submission. Returns 202 immediately."""
    caller = x_caller or "unknown"
    payload = body.model_dump(exclude_none=True)
    priority = body.priority

    outcome, job_req = await admission.admit(
        model=body.model,
        payload=payload,
        priority=priority,
        caller=caller,
        caller_metadata=body.metadata,
        idempotency_key=idempotency_key,
    )

    if outcome.result == AdmissionResult.REJECTED:
        raise HTTPException(
            status_code=429,
            detail={
                "error": "queue_full",
                "estimated_drain_s": outcome.estimated_drain_s,
            },
        )

    if job_req is not None:
        await _store_payload(redis, outcome.job_id, payload)

    return JobSubmitResponse(
        job_id=outcome.job_id,
        status=outcome.status.value,
        position_in_queue=outcome.position_in_queue,
    )


# ---------------------------------------------------------------------------
# GET /jobs/{job_id}
# ---------------------------------------------------------------------------


@router.get("/jobs/{job_id}")
async def get_job(
    job_id: str,
    redis: aioredis.Redis = Depends(get_redis),
) -> JobStatusResponse:
    """Return job status and result (if terminal)."""
    data = await redis.hgetall(job_key(job_id))
    if not data:
        raise HTTPException(status_code=404, detail="Job not found")

    def _f(key: str) -> str | None:
        k = key.encode() if isinstance(key, str) else key
        v = data.get(k)
        if v is None:
            return None
        return v.decode() if isinstance(v, bytes) else v

    status = _f("status") or "unknown"

    # If terminal, check TTL to return 410 Gone if expired
    ttl = await redis.ttl(job_key(job_id))
    if ttl == -2:
        raise HTTPException(status_code=410, detail="Job result expired")

    result_blob: dict[str, Any] | None = None
    if status == JobStatus.COMPLETED.value:
        result_blob = await _load_response_blob(redis, job_id)

    # Compute wait_ms and runtime_ms from timestamps
    wait_ms = runtime_ms = None
    enqueue_ts_str = _f("enqueue_ts")
    dispatch_ts_str = _f("dispatch_ts")
    complete_ts_str = _f("complete_ts")
    try:
        if enqueue_ts_str and dispatch_ts_str:
            et = datetime.datetime.fromisoformat(enqueue_ts_str).timestamp()
            dt = datetime.datetime.fromisoformat(dispatch_ts_str).timestamp()
            wait_ms = int(max(0.0, dt - et) * 1000)
        if dispatch_ts_str and complete_ts_str:
            dt = datetime.datetime.fromisoformat(dispatch_ts_str).timestamp()
            ct = datetime.datetime.fromisoformat(complete_ts_str).timestamp()
            runtime_ms = int(max(0.0, ct - dt) * 1000)
    except Exception:
        pass

    http_status_str = _f("http_status")

    return JobStatusResponse(
        job_id=job_id,
        status=status,
        model=_f("model"),
        priority=int(_f("priority") or 5),
        enqueue_ts=enqueue_ts_str,
        dispatch_ts=dispatch_ts_str,
        complete_ts=complete_ts_str,
        wait_ms=wait_ms,
        runtime_ms=runtime_ms,
        error_class=_f("error_class"),
        error_message=_f("error_message"),
        http_status=int(http_status_str) if http_status_str else None,
        result=result_blob,
    )


# ---------------------------------------------------------------------------
# POST /jobs/{job_id}/cancel
# ---------------------------------------------------------------------------


@router.post("/jobs/{job_id}/cancel")
async def cancel_job(
    job_id: str,
    request: Request,
    redis: aioredis.Redis = Depends(get_redis),
) -> CancelResponse:
    """Cancel a queued or dispatched job."""
    from app.storage.redis_lua import LuaScripts

    data = await redis.hgetall(job_key(job_id))
    if not data:
        raise HTTPException(status_code=404, detail="Job not found")

    def _f(key: str) -> str | None:
        k = key.encode() if isinstance(key, str) else key
        v = data.get(k)
        return v.decode() if isinstance(v, bytes) else v

    status = _f("status") or "unknown"
    model = _f("model") or "unknown"

    terminal = {
        JobStatus.COMPLETED.value,
        JobStatus.FAILED.value,
        JobStatus.CANCELLED.value,
        JobStatus.TIMEOUT.value,
        JobStatus.REJECTED.value,
    }
    if status in terminal:
        return CancelResponse(job_id=job_id, status=status)

    lua: LuaScripts = request.app.state.lua
    cancel_ts = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()

    if status == JobStatus.QUEUED.value:
        removed = await lua.evalsha_cancel_queued(
            redis,
            queue_key(model),
            job_key(job_id),
            job_id,
            cancel_ts,
        )
        if removed == 1:
            await redis.expire(job_key(job_id), 3600)
            await redis.publish(job_done_channel(job_id), JobStatus.CANCELLED.value)
            m.record_cancel(model)
            return CancelResponse(job_id=job_id, status=JobStatus.CANCELLED.value)
        # Fall through: status may have changed between hgetall and evalsha

    # Job is dispatched — mark cancelling; dispatcher will handle CancelledError
    await redis.hset(job_key(job_id), "status", JobStatus.CANCELLING.value)
    return CancelResponse(job_id=job_id, status=JobStatus.CANCELLING.value)


# ---------------------------------------------------------------------------
# GET /queues
# ---------------------------------------------------------------------------


@router.get("/queues")
async def get_queues(
    request: Request,
    redis: aioredis.Redis = Depends(get_redis),
) -> QueuesResponse:
    """Return a snapshot of queue state per model."""
    config = request.app.state.config
    snapshots: dict[str, ModelQueueSnapshot] = {}

    for model in config.all_model_names():
        model_cfg = config.model_config_for(model)
        try:
            depth_raw, in_use_raw = await asyncio.gather(
                redis.zcard(queue_key(model)),
                redis.get(slots_in_use_key(model)),
            )
            depth = int(depth_raw or 0)
            in_use = int(in_use_raw or 0)

            # Oldest item in queue: lowest score member's enqueue ts
            oldest_ms: int | None = None
            oldest_members = await redis.zrange(queue_key(model), 0, 0, withscores=True)
            if oldest_members:
                oldest_job_id_bytes, _score = oldest_members[0]
                oldest_job_id = (
                    oldest_job_id_bytes.decode()
                    if isinstance(oldest_job_id_bytes, bytes)
                    else oldest_job_id_bytes
                )
                et_raw = await redis.hget(job_key(oldest_job_id), "enqueue_ts")
                if et_raw:
                    et_str = et_raw.decode() if isinstance(et_raw, bytes) else et_raw
                    try:
                        et = datetime.datetime.fromisoformat(et_str).timestamp()
                        oldest_ms = int((time.time() - et) * 1000)
                    except Exception:
                        pass

            snapshots[model] = ModelQueueSnapshot(
                depth=depth,
                in_use=in_use,
                capacity=model_cfg.max_concurrency,
                oldest_enqueue_ms=oldest_ms,
            )
        except aioredis.RedisError:
            m.record_redis_error("get_queues")
            snapshots[model] = ModelQueueSnapshot(depth=0, in_use=0, capacity=model_cfg.max_concurrency)

    return QueuesResponse(queues=snapshots)


# ---------------------------------------------------------------------------
# GET /metrics
# ---------------------------------------------------------------------------


@router.get("/metrics")
async def metrics() -> Response:
    """Prometheus metrics exposition."""
    data = generate_latest()
    return Response(content=data, media_type=CONTENT_TYPE_LATEST)


# ---------------------------------------------------------------------------
# GET /health
# ---------------------------------------------------------------------------


@router.get("/health")
async def health(request: Request) -> Response:
    """Liveness/readiness check. Returns 200 if Redis + at least one worker are alive."""
    from app.backend.litellm_forwarder import LiteLLMForwarder

    redis_client: aioredis.Redis = request.app.state.redis
    forwarder: LiteLLMForwarder = request.app.state.forwarder
    workers: dict[str, Any] = request.app.state.workers

    # Check Redis
    redis_status = "ok"
    try:
        await redis_client.ping()
    except Exception:
        redis_status = "down"

    # Check LiteLLM (best-effort, 5s timeout)
    litellm_status = await forwarder.check_health()

    # Check workers
    worker_statuses = {
        model: WorkerStatus(alive=worker.is_alive)
        for model, worker in workers.items()
    }
    any_worker_alive = any(ws.alive for ws in worker_statuses.values())

    overall = "ok"
    if redis_status == "down" or not any_worker_alive:
        overall = "down"
    elif litellm_status != "ok":
        overall = "degraded"

    http_code = 200 if overall != "down" else 503
    body = HealthResponse(
        status=overall,
        redis=redis_status,
        litellm=litellm_status,
        workers=worker_statuses,
    )
    return JSONResponse(
        status_code=http_code,
        content=body.model_dump(),
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


async def _wait_for_completion(
    redis: aioredis.Redis,
    job_id: str,
    timeout_s: float,
) -> dict[str, Any] | None:
    """Subscribe to job_done:{job_id} and wait up to timeout_s seconds.

    Returns a dict with at least 'status' key, or None on timeout.
    """
    channel = job_done_channel(job_id)
    pubsub = redis.pubsub()
    await pubsub.subscribe(channel)
    try:
        deadline = time.monotonic() + timeout_s
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            message = await pubsub.get_message(
                ignore_subscribe_messages=True,
                timeout=min(remaining, 1.0),
            )
            if message is not None and message.get("type") == "message":
                data = message["data"]
                status = data.decode() if isinstance(data, bytes) else data
                return {"status": status}
            # Also check if job reached terminal via direct Redis read
            # (handles the case where we subscribed after publish)
            status_raw = await redis.hget(job_key(job_id), "status")
            if status_raw:
                status_str = status_raw.decode() if isinstance(status_raw, bytes) else status_raw
                terminal = {
                    JobStatus.COMPLETED.value,
                    JobStatus.FAILED.value,
                    JobStatus.CANCELLED.value,
                    JobStatus.TIMEOUT.value,
                    JobStatus.REJECTED.value,
                }
                if status_str in terminal:
                    return {"status": status_str}
    finally:
        try:
            await pubsub.unsubscribe(channel)
            await pubsub.aclose()
        except Exception:
            pass


async def _store_payload(redis: aioredis.Redis, job_id: str, payload: dict[str, Any]) -> None:
    """Persist the full request payload in the job hash for the dispatcher."""
    try:
        await redis.hset(job_key(job_id), "payload_json", json.dumps(payload))
    except aioredis.RedisError:
        m.record_redis_error("store_payload")


async def _load_response_blob(
    redis: aioredis.Redis, job_id: str
) -> dict[str, Any] | None:
    """Load and decompress the response blob from the job hash."""
    import gzip

    raw = await redis.hget(job_key(job_id), "response_blob")
    if raw is None:
        return None
    try:
        return json.loads(gzip.decompress(raw))
    except Exception:
        return None


def _status_to_http(status: str) -> int:
    """Map job status string to HTTP status code for sync error responses."""
    mapping = {
        JobStatus.FAILED.value: 502,
        JobStatus.TIMEOUT.value: 504,
        JobStatus.CANCELLED.value: 409,
        JobStatus.REJECTED.value: 429,
    }
    return mapping.get(status, 500)
