"""Integration tests for backend failure modes.

Each failure scenario uses a dedicated fake LiteLLM app injected before server
start (set ``fake.app`` BEFORE ``await fake.start()``). We verify that:
  - LiteLLM 503 → error_class=backend_5xx, slot released.
  - LiteLLM 429 → error_class=backend_429, slot released.
  - Backend timeout  → error_class=backend_timeout (or timeout status).
  - Slot is always released after a failure.
"""

from __future__ import annotations

import asyncio

import fakeredis.aioredis as fake_aioredis
import httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.config import AppConfig
from app.observability import metrics as m
from app.state import ErrorClass
from app.storage.redis_keys import slots_capacity_key, slots_in_use_key
from tests.fakes.fake_litellm import FakeLiteLLMServer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_error_app(status_code: int, sleep_s: float = 0.0) -> FastAPI:
    """Return a FastAPI app that always responds with *status_code*."""
    error_app = FastAPI(title=f"ErrorFake-{status_code}")

    @error_app.post("/v1/chat/completions")
    async def _error(req: Request):
        if sleep_s > 0:
            await asyncio.sleep(sleep_s)
        return JSONResponse({"error": f"fake {status_code}"}, status_code=status_code)

    @error_app.get("/health")
    async def _health():
        return {"status": "ok"}

    return error_app


async def _make_app_with_fake(fake_litellm_url: str, log_dir: str, sync_timeout: int = 3):
    """Build a minimal test app wired to a specific fake LiteLLM URL."""
    from app.backend.litellm_forwarder import LiteLLMForwarder
    from app.core.admission import AdmissionController
    from app.core.dispatcher import DispatcherWorker
    from app.core.integrity import IntegrityChecker
    from app.core.lease_watcher import LeaseWatcher
    from app.observability.jsonl_logger import JSONLLogger
    from app.storage.redis_lua import LuaScripts
    from fastapi import FastAPI as _FastAPI

    raw = {
        "scheduler": {
            "redis_url": "redis://localhost",
            "backend_litellm_url": fake_litellm_url,
            "worker_poll_timeout_seconds": 1,
            "lease_watcher_interval_seconds": 60,
            "integrity_check_interval_seconds": 60,
            "log_dir": log_dir,
        },
        "defaults": {
            "max_concurrency": 4,
            "max_queue_depth": 40,
            "max_wait_seconds": 10,
            "sync_timeout_seconds": sync_timeout,
            "priority": 5,
        },
        "models": {
            "chat-fast": {
                "max_concurrency": 4,
                "max_queue_depth": 40,
                "max_wait_seconds": 10,
                "sync_timeout_seconds": sync_timeout,
                "priority": 5,
            }
        },
    }
    config = AppConfig.model_validate(raw)

    redis = fake_aioredis.FakeRedis(connected=True, version=(7, 0, 0))
    fapp = _FastAPI(title="FailureTest")
    from app.api.routes import router
    fapp.include_router(router)
    fapp.state.redis = redis
    fapp.state.config = config

    lua = LuaScripts()
    await lua.load_all(redis)
    fapp.state.lua = lua

    for model_name, model_cfg in config.models.items():
        await redis.set(slots_capacity_key(model_name), model_cfg.max_concurrency)
        await redis.setnx(slots_in_use_key(model_name), 0)
        m.update_slot_gauges(model_name, 0, model_cfg.max_concurrency)

    logger = JSONLLogger(log_dir=log_dir)
    await logger.start()
    fapp.state.logger = logger

    forwarder = LiteLLMForwarder(backend_url=fake_litellm_url)
    await forwarder.start()
    fapp.state.forwarder = forwarder

    admission = AdmissionController(redis=redis, config=config, lua=lua, logger=logger)
    fapp.state.admission = admission

    workers = {}
    for model_name in config.all_model_names():
        worker = DispatcherWorker(
            model=model_name, redis=redis, config=config,
            lua=lua, forwarder=forwarder, logger=logger,
        )
        worker.start()
        workers[model_name] = worker
    fapp.state.workers = workers

    lw = LeaseWatcher(redis=redis, config=config, logger=logger)
    lw.start()
    fapp.state.lease_watcher = lw
    ic = IntegrityChecker(redis=redis, config=config, logger=logger)
    ic.start()
    fapp.state.integrity = ic

    return fapp, redis, logger, forwarder, workers, lw, ic


async def _teardown(redis, logger, forwarder, workers, lw, ic):
    await ic.stop()
    await lw.stop()
    for w in workers.values():
        await w.stop()
    await logger.stop()
    await forwarder.stop()
    await redis.aclose()


async def _submit_and_wait(c: httpx.AsyncClient, model="chat-fast", timeout_s=12.0):
    """Submit a job and poll until terminal; return final GET response."""
    resp = await c.post(
        "/jobs",
        json={"model": model, "messages": [{"role": "user", "content": "fail test"}]},
    )
    assert resp.status_code == 202
    job_id = resp.json()["job_id"]

    terminal = {"completed", "failed", "timeout", "cancelled", "rejected"}
    deadline = asyncio.get_event_loop().time() + timeout_s
    while asyncio.get_event_loop().time() < deadline:
        get_resp = await c.get(f"/jobs/{job_id}")
        if get_resp.status_code in (404, 410):
            return get_resp
        body = get_resp.json()
        if body["status"] in terminal:
            return get_resp
        await asyncio.sleep(0.3)
    raise TimeoutError(f"Job {job_id} never reached terminal state")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_backend_5xx_sets_error_class(tmp_path):
    """LiteLLM 503 → job error_class must be backend_5xx."""
    # Build the custom error app BEFORE starting the server
    fake = FakeLiteLLMServer()
    fake.app = _make_error_app(503)
    await fake.start()

    log_dir = str(tmp_path / "5xx-logs")
    fapp, redis, logger, forwarder, workers, lw, ic = await _make_app_with_fake(
        fake.base_url, log_dir
    )

    try:
        transport = httpx.ASGITransport(app=fapp)
        async with httpx.AsyncClient(transport=transport, base_url="http://ts", timeout=15.0) as c:
            final_resp = await _submit_and_wait(c)
    finally:
        await _teardown(redis, logger, forwarder, workers, lw, ic)
        await fake.stop()

    assert final_resp.status_code == 200
    body = final_resp.json()
    assert body["status"] == "failed"
    assert body.get("error_class") == ErrorClass.BACKEND_5XX.value


@pytest.mark.asyncio
async def test_backend_429_sets_error_class(tmp_path):
    """LiteLLM 429 → job error_class must be backend_429."""
    fake = FakeLiteLLMServer()
    fake.app = _make_error_app(429)
    await fake.start()

    log_dir = str(tmp_path / "429-logs")
    fapp, redis, logger, forwarder, workers, lw, ic = await _make_app_with_fake(
        fake.base_url, log_dir
    )

    try:
        transport = httpx.ASGITransport(app=fapp)
        async with httpx.AsyncClient(transport=transport, base_url="http://ts", timeout=15.0) as c:
            final_resp = await _submit_and_wait(c)
    finally:
        await _teardown(redis, logger, forwarder, workers, lw, ic)
        await fake.stop()

    assert final_resp.status_code == 200
    body = final_resp.json()
    assert body["status"] == "failed"
    assert body.get("error_class") == ErrorClass.BACKEND_429.value


@pytest.mark.asyncio
async def test_backend_timeout_sets_error_class(tmp_path):
    """Fake with latency > dispatcher timeout → error_class=backend_timeout.

    We configure the fake to sleep 6 s and set sync_timeout_seconds=1 on the
    scheduler.  The dispatcher's httpx call will time out, and the job must
    surface as failed / backend_timeout (or the sync endpoint returns 504).
    """
    fake = FakeLiteLLMServer()
    # Always sleep 6s — dispatcher timeout is 1s
    fake.app = _make_error_app(200, sleep_s=6.0)
    # Override the post handler to return valid choices after sleep
    slow_app = FastAPI(title="SlowFake")

    @slow_app.post("/v1/chat/completions")
    async def _slow(req: Request):
        await asyncio.sleep(6)
        return JSONResponse({"choices": []}, status_code=200)

    @slow_app.get("/health")
    async def _health():
        return {"status": "ok"}

    fake.app = slow_app
    await fake.start()

    log_dir = str(tmp_path / "timeout-logs")
    fapp, redis, logger, forwarder, workers, lw, ic = await _make_app_with_fake(
        fake.base_url, log_dir, sync_timeout=1
    )

    try:
        transport = httpx.ASGITransport(app=fapp)
        async with httpx.AsyncClient(transport=transport, base_url="http://ts", timeout=20.0) as c:
            final_resp = await _submit_and_wait(c, timeout_s=15.0)
    finally:
        await _teardown(redis, logger, forwarder, workers, lw, ic)
        await fake.stop()

    if final_resp.status_code == 200:
        body = final_resp.json()
        assert body["status"] in ("failed", "timeout"), (
            f"Expected failed/timeout, got {body['status']}"
        )
        if body["status"] == "failed":
            assert body.get("error_class") == ErrorClass.BACKEND_TIMEOUT.value, (
                f"Expected backend_timeout error_class, got {body.get('error_class')}"
            )


@pytest.mark.asyncio
async def test_slot_released_after_failure(tmp_path):
    """After a backend failure, the slot counter must be decremented (released)."""
    fake = FakeLiteLLMServer()
    fake.app = _make_error_app(503)
    await fake.start()

    log_dir = str(tmp_path / "slot-release-logs")
    fapp, redis, logger, forwarder, workers, lw, ic = await _make_app_with_fake(
        fake.base_url, log_dir
    )

    try:
        transport = httpx.ASGITransport(app=fapp)
        async with httpx.AsyncClient(transport=transport, base_url="http://ts", timeout=15.0) as c:
            await _submit_and_wait(c)

        # Check slot was released
        in_use = int(await redis.get(slots_in_use_key("chat-fast")) or 0)
    finally:
        await _teardown(redis, logger, forwarder, workers, lw, ic)
        await fake.stop()

    assert in_use == 0, f"Slot not released after failure: in_use={in_use}"
