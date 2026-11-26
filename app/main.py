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

_DEFAULT_CONFIG_PATH = os.environ.get("SCHEDULER_CONFIG", "config/default.yaml")

def create_app(config_path: str = _DEFAULT_CONFIG_PATH) -> FastAPI:
    config = load_config(config_path)
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        await _startup(app, config)
        try: yield
        finally: await _shutdown(app)
    app = FastAPI(title="Talki Admission Scheduler", lifespan=lifespan)
    from app.api.routes import router
    app.include_router(router)
    return app

async def _startup(app: FastAPI, config: AppConfig) -> None:
    sched = config.scheduler
    redis = aioredis.from_url(sched.redis_url, decode_responses=False)
    app.state.redis = redis
    lua = LuaScripts(); await lua.load_all(redis); app.state.lua = lua
    for model_name, model_cfg in config.models.items():
        await redis.set(slots_capacity_key(model_name), model_cfg.max_concurrency)
        await redis.setnx(slots_in_use_key(model_name), 0)
        m.update_slot_gauges(model_name, 0, model_cfg.max_concurrency)
    logger = JSONLLogger(log_dir=sched.log_dir); await logger.start(); app.state.logger = logger
    
    # Passing the master key to the forwarder
    master_key = os.environ.get("LITELLM_MASTER_KEY")
    forwarder = LiteLLMForwarder(backend_url=sched.backend_litellm_url, api_key=master_key)
    await forwarder.start(); app.state.forwarder = forwarder
    
    admission = AdmissionController(redis=redis, config=config, lua=lua, logger=logger)
    app.state.admission = admission
    workers = {}
    for model_name in config.all_model_names():
        worker = DispatcherWorker(model=model_name, redis=redis, config=config, lua=lua, forwarder=forwarder, logger=logger)
        worker.start(); workers[model_name] = worker
    app.state.workers = workers
    lease_watcher = LeaseWatcher(redis=redis, config=config, logger=logger); lease_watcher.start(); app.state.lease_watcher = lease_watcher
    integrity = IntegrityChecker(redis=redis, config=config, logger=logger); integrity.start(); app.state.integrity = integrity
    app.state.config = config

async def _shutdown(app: FastAPI) -> None:
    if hasattr(app.state, "integrity"): await app.state.integrity.stop()
    if hasattr(app.state, "lease_watcher"): await app.state.lease_watcher.stop()
    if hasattr(app.state, "workers"):
        await asyncio.gather(*[w.stop() for w in app.state.workers.values()], return_exceptions=True)
    if hasattr(app.state, "logger"): await app.state.logger.stop()
    if hasattr(app.state, "forwarder"): await app.state.forwarder.stop()
    if hasattr(app.state, "redis"): await app.state.redis.aclose()

app = create_app()
