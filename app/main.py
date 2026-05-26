"""FastAPI application factory and lifespan manager.

Usage:
    uvicorn app.main:create_app --factory --config config/default.yaml

Or programmatically:
    from app.main import create_app
    app = create_app("config/default.yaml")
"""

from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from typing import AsyncIterator

import redis.asyncio as aioredis
from fastapi import FastAPI

from app.backend.litellm_forwarder import LiteLLMForwarder
from app.config import AppConfig, load_config
from app.core.admission import AdmissionController
from app.core.dispatcher import DispatcherWorker
from app.core.integrity import IntegrityChecker
from app.core.lease_watcher import LeaseWatcher
from app.observability import metrics as m
from app.observability.jsonl_logger import JSONLLogger
from app.storage.redis_keys import slots_capacity_key, slots_in_use_key
from app.storage.redis_lua import LuaScripts

# Default config path — can be overridden via env var or factory arg
_DEFAULT_CONFIG_PATH = os.environ.get(
    "SCHEDULER_CONFIG",
    os.path.join(os.path.dirname(__file__), "..", "config", "default.yaml"),
)


def create_app(config_path: str = _DEFAULT_CONFIG_PATH) -> FastAPI:
    """Create and configure the FastAPI application.

    Args:
        config_path: Path to the YAML configuration file.

    Returns:
        Configured FastAPI instance ready to be served by uvicorn.
    """
    config = load_config(config_path)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        """Start all background tasks on startup; clean up on shutdown."""
        await _startup(app, config)
        try:
            yield
        finally:
            await _shutdown(app)

    app = FastAPI(
        title="Talki LLM Admission Scheduler",
        version="0.1.0",
        description=(
            "Priority queue side-car for LiteLLM. Provides per-model concurrency "
            "control, bounded queuing, sync/async submission, and JSONL lifecycle logging."
        ),
        lifespan=lifespan,
    )

    # Register routes
    from app.api.routes import router
    app.include_router(router)

    return app


async def _startup(app: FastAPI, config: AppConfig) -> None:
    """Initialize all shared resources and start background tasks."""
    sched = config.scheduler

    # ------------------------------------------------------------------ Redis
    redis = aioredis.from_url(
        sched.redis_url,
        encoding="utf-8",
        decode_responses=False,
        socket_connect_timeout=5,
        socket_timeout=5,
    )
    try:
        await redis.ping()
    except Exception as exc:
        raise RuntimeError(f"Cannot connect to Redis at {sched.redis_url}: {exc}") from exc

    app.state.redis = redis

    # ------------------------------------------------------------------ Lua scripts
    lua = LuaScripts()
    await lua.load_all(redis)
    app.state.lua = lua

    # ------------------------------------------------------------------ Capacity keys
    # Set capacity keys for all configured models (idempotent — does not overwrite)
    for model_name, model_cfg in config.models.items():
        cap_key = slots_capacity_key(model_name)
        in_use_key = slots_in_use_key(model_name)
        await redis.set(cap_key, model_cfg.max_concurrency)
        # Only set in_use to 0 if it doesn't already exist (survive restarts)
        await redis.setnx(in_use_key, 0)
        # Initialize Prometheus capacity gauge
        m.update_slot_gauges(model_name, 0, model_cfg.max_concurrency)

    # ------------------------------------------------------------------ Logger
    logger = JSONLLogger(log_dir=sched.log_dir)
    await logger.start()
    app.state.logger = logger

    # ------------------------------------------------------------------ Forwarder
    forwarder = LiteLLMForwarder(backend_url=sched.backend_litellm_url)
    await forwarder.start()
    app.state.forwarder = forwarder

    # ------------------------------------------------------------------ Admission controller
    admission = AdmissionController(
        redis=redis,
        config=config,
        lua=lua,
        logger=logger,
    )
    app.state.admission = admission

    # ------------------------------------------------------------------ Dispatcher workers (1 per model)
    workers: dict[str, DispatcherWorker] = {}
    for model_name in config.all_model_names():
        worker = DispatcherWorker(
            model=model_name,
            redis=redis,
            config=config,
            lua=lua,
            forwarder=forwarder,
            logger=logger,
        )
        worker.start()
        workers[model_name] = worker
    app.state.workers = workers

    # ------------------------------------------------------------------ Lease watcher
    lease_watcher = LeaseWatcher(redis=redis, config=config, logger=logger)
    lease_watcher.start()
    app.state.lease_watcher = lease_watcher

    # ------------------------------------------------------------------ Integrity checker
    integrity = IntegrityChecker(redis=redis, config=config, logger=logger)
    integrity.start()
    app.state.integrity = integrity

    # ------------------------------------------------------------------ Store config for routes
    app.state.config = config


async def _shutdown(app: FastAPI) -> None:
    """Gracefully stop all background tasks and close connections."""
    # Stop integrity checker
    if hasattr(app.state, "integrity"):
        await app.state.integrity.stop()

    # Stop lease watcher
    if hasattr(app.state, "lease_watcher"):
        await app.state.lease_watcher.stop()

    # Stop dispatcher workers
    if hasattr(app.state, "workers"):
        stop_tasks = [w.stop() for w in app.state.workers.values()]
        await asyncio.gather(*stop_tasks, return_exceptions=True)

    # Flush and stop logger
    if hasattr(app.state, "logger"):
        await app.state.logger.stop()

    # Close httpx client
    if hasattr(app.state, "forwarder"):
        await app.state.forwarder.stop()

    # Close Redis connection
    if hasattr(app.state, "redis"):
        await app.state.redis.aclose()


# Allow running directly with uvicorn app.main:app for simple cases
# (uses default config path from env or relative default.yaml)
app = create_app()
