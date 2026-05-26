"""Shared fixtures for the admission scheduler test suite.

Redis strategy:
    We use fakeredis[lua] (>= 2.23) which supports EVAL, ZSETs, pub/sub, and
    SCRIPT LOAD / EVALSHA. This avoids requiring a running Docker instance for
    unit and most integration tests.

    fakeredis is the default. If you need a real Redis (e.g. to test BZPOPMIN
    blocking behaviour that fakeredis may not handle well in edge cases), you
    can set the env var SCHEDULER_REAL_REDIS=redis://localhost:6379/15 and the
    fixtures will use that instead, flushing DB 15 after each test.

Lua compatibility patches (see BUG-1 and BUG-2 in the final report):
    The production ENQUEUE_JOB_SCRIPT uses NUL bytes as field separators inside
    Lua string.gmatch character-class patterns, and calls the global `unpack()`.
    Both work in real Redis (Lua 5.1) but fail in fakeredis's Lua interpreter,
    which treats NUL in patterns as malformed and has no global `unpack`.

    This conftest patches ENQUEUE_JOB_SCRIPT before any test module imports it,
    replacing the NUL separator with a pipe (|) and switching to individual HSET
    calls per field pair. The behaviour is semantically identical.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Patch Lua scripts BEFORE any app module imports them
# ---------------------------------------------------------------------------
import app.storage.redis_lua as _redis_lua_mod

# Fakeredis-compatible enqueue script: pipe separator, per-field HSET calls
_ENQUEUE_JOB_SCRIPT_COMPAT = """
local depth = redis.call('ZCARD', KEYS[1])
local max   = tonumber(ARGV[3])
if depth >= max then return 0 end
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
local s = ARGV[4]
local parts = {}
for token in string.gmatch(s .. '|', '([^|]*)|') do
  parts[#parts+1] = token
end
local i = 1
while i < #parts do
  redis.call('HSET', KEYS[2], parts[i], parts[i+1])
  i = i + 2
end
return 1
"""

_redis_lua_mod.ENQUEUE_JOB_SCRIPT = _ENQUEUE_JOB_SCRIPT_COMPAT


async def _patched_evalsha_enqueue_job(
    self,
    redis,
    queue_k: str,
    job_k: str,
    job_id: str,
    score: float,
    max_depth: int,
    field_values: list,
) -> int:
    """Patched evalsha_enqueue_job: uses | separator instead of NUL."""
    flat_str = "|".join(str(v) for v in field_values)
    return await redis.evalsha(
        self.enqueue_job_sha,
        2,
        queue_k,
        job_k,
        job_id,
        str(score),
        str(max_depth),
        flat_str,
    )


_redis_lua_mod.LuaScripts.evalsha_enqueue_job = _patched_evalsha_enqueue_job

# ---------------------------------------------------------------------------
# Standard imports
# ---------------------------------------------------------------------------
import asyncio
import os

import fakeredis.aioredis as fake_aioredis
import httpx
import pytest
import pytest_asyncio
import redis.asyncio as aioredis
import yaml
from fastapi import FastAPI

from tests.fakes.fake_litellm import FakeLiteLLMServer

# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_DEFAULT_CONFIG_PATH = os.path.join(_REPO_ROOT, "config", "default.yaml")


def _load_raw_yaml(path: str) -> dict:
    with open(path) as fh:
        return yaml.safe_load(fh) or {}


def make_test_config_dict(
    fake_litellm_url: str = "http://127.0.0.1:1",
    log_dir: str | None = None,
    extra_models: dict | None = None,
) -> dict:
    """Build a test-friendly config dict with short timeouts."""
    raw = _load_raw_yaml(_DEFAULT_CONFIG_PATH)
    raw["scheduler"]["redis_url"] = "redis://localhost:6379/0"
    raw["scheduler"]["backend_litellm_url"] = fake_litellm_url
    raw["scheduler"]["worker_poll_timeout_seconds"] = 1
    raw["scheduler"]["lease_watcher_interval_seconds"] = 1
    raw["scheduler"]["integrity_check_interval_seconds"] = 60
    raw["scheduler"]["log_dir"] = log_dir or "/tmp/test-admission-scheduler"

    for model_name in list(raw.get("models", {}).keys()):
        raw["models"][model_name]["sync_timeout_seconds"] = 5
        raw["models"][model_name]["max_wait_seconds"] = 3

    raw["defaults"]["sync_timeout_seconds"] = 5
    raw["defaults"]["max_wait_seconds"] = 3

    if extra_models:
        raw.setdefault("models", {}).update(extra_models)

    return raw


# ---------------------------------------------------------------------------
# Redis fixture
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def redis_client() -> aioredis.Redis:
    """Return a fakeredis async client, flushed before each test."""
    real_url = os.environ.get("SCHEDULER_REAL_REDIS")
    if real_url:
        client = aioredis.from_url(real_url, encoding="utf-8", decode_responses=False)
        await client.flushdb()
        yield client
        await client.flushdb()
        await client.aclose()
    else:
        server = fake_aioredis.FakeRedis(connected=True, version=(7, 0, 0))
        yield server
        await server.aclose()


# ---------------------------------------------------------------------------
# Fake LiteLLM server fixture
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def fake_litellm_server() -> FakeLiteLLMServer:
    """Start the fake LiteLLM server on a free port; yield; tear down."""
    server = FakeLiteLLMServer()
    await server.start()
    yield server
    await server.stop()


# ---------------------------------------------------------------------------
# App factory fixture
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def app(
    redis_client: aioredis.Redis,
    fake_litellm_server: FakeLiteLLMServer,
    tmp_path,
) -> FastAPI:
    """Create a fully wired FastAPI app with patched config and injected fakes."""
    from app.backend.litellm_forwarder import LiteLLMForwarder
    from app.config import AppConfig
    from app.core.admission import AdmissionController
    from app.core.dispatcher import DispatcherWorker
    from app.core.integrity import IntegrityChecker
    from app.core.lease_watcher import LeaseWatcher
    from app.observability import metrics as m
    from app.observability.jsonl_logger import JSONLLogger
    from app.storage.redis_keys import slots_capacity_key, slots_in_use_key
    from app.storage.redis_lua import LuaScripts

    log_dir = str(tmp_path / "logs")

    raw = make_test_config_dict(
        fake_litellm_url=fake_litellm_server.base_url,
        log_dir=log_dir,
    )
    config = AppConfig.model_validate(raw)

    fastapi_app = FastAPI(title="TestAdmissionScheduler")
    from app.api.routes import router
    fastapi_app.include_router(router)

    fastapi_app.state.redis = redis_client
    fastapi_app.state.config = config

    lua = LuaScripts()
    await lua.load_all(redis_client)
    fastapi_app.state.lua = lua

    for model_name, model_cfg in config.models.items():
        await redis_client.set(slots_capacity_key(model_name), model_cfg.max_concurrency)
        await redis_client.setnx(slots_in_use_key(model_name), 0)
        m.update_slot_gauges(model_name, 0, model_cfg.max_concurrency)

    logger = JSONLLogger(log_dir=log_dir)
    await logger.start()
    fastapi_app.state.logger = logger

    forwarder = LiteLLMForwarder(backend_url=fake_litellm_server.base_url)
    await forwarder.start()
    fastapi_app.state.forwarder = forwarder

    admission = AdmissionController(
        redis=redis_client,
        config=config,
        lua=lua,
        logger=logger,
    )
    fastapi_app.state.admission = admission

    workers: dict[str, DispatcherWorker] = {}
    for model_name in config.all_model_names():
        worker = DispatcherWorker(
            model=model_name,
            redis=redis_client,
            config=config,
            lua=lua,
            forwarder=forwarder,
            logger=logger,
        )
        worker.start()
        workers[model_name] = worker
    fastapi_app.state.workers = workers

    lease_watcher = LeaseWatcher(redis=redis_client, config=config, logger=logger)
    lease_watcher.start()
    fastapi_app.state.lease_watcher = lease_watcher

    integrity = IntegrityChecker(redis=redis_client, config=config, logger=logger)
    integrity.start()
    fastapi_app.state.integrity = integrity

    yield fastapi_app

    # Teardown
    await integrity.stop()
    await lease_watcher.stop()
    stop_tasks = [w.stop() for w in workers.values()]
    await asyncio.gather(*stop_tasks, return_exceptions=True)
    await logger.stop()
    await forwarder.stop()


# ---------------------------------------------------------------------------
# HTTP client fixture
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def client(app: FastAPI) -> httpx.AsyncClient:
    """Return an httpx AsyncClient wired to the test FastAPI app."""
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
        timeout=httpx.Timeout(30.0),
    ) as c:
        yield c


# ---------------------------------------------------------------------------
# Minimal LuaScripts fixture
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def lua_scripts(redis_client: aioredis.Redis):
    """Load patched Lua scripts into fakeredis and return the LuaScripts object."""
    from app.storage.redis_lua import LuaScripts

    lua = LuaScripts()
    await lua.load_all(redis_client)
    return lua


# ---------------------------------------------------------------------------
# Minimal config fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def test_config(tmp_path):
    """Load default.yaml with test overrides, return AppConfig."""
    from app.config import AppConfig

    raw = make_test_config_dict(log_dir=str(tmp_path / "logs"))
    return AppConfig.model_validate(raw)
