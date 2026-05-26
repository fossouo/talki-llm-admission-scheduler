"""Integration tests for the LeaseWatcher orphan reclaim logic.

Strategy: inject a job that looks like it was dispatched (status=dispatched,
in inflight ZSET, no lease key, stale dispatch timestamp). Trigger the scan
manually. Assert the job is marked failed/worker_crash and the slot is
decremented.
"""

from __future__ import annotations

import asyncio
import time

import fakeredis.aioredis as fake_aioredis
import pytest
import pytest_asyncio

from app.config import AppConfig
from app.core.lease_watcher import LeaseWatcher
from app.observability import metrics as m
from app.observability.jsonl_logger import JSONLLogger
from app.state import ErrorClass, JobStatus
from app.storage.redis_keys import (
    dlq_key,
    inflight_key,
    job_key,
    lease_key,
    slots_capacity_key,
    slots_in_use_key,
)
from app.storage.redis_lua import LuaScripts


@pytest_asyncio.fixture
async def watcher_setup(tmp_path):
    """Build a LeaseWatcher with a short interval and short lease TTL."""
    raw = {
        "scheduler": {
            "redis_url": "redis://localhost",
            "backend_litellm_url": "http://fake",
            "worker_poll_timeout_seconds": 1,
            "lease_watcher_interval_seconds": 1,
            "integrity_check_interval_seconds": 60,
            "log_dir": str(tmp_path / "lease-logs"),
        },
        "defaults": {
            "max_concurrency": 2,
            "max_queue_depth": 10,
            "max_wait_seconds": 5,
            "sync_timeout_seconds": 5,
            "priority": 5,
        },
        "models": {
            "test-watcher-model": {
                "max_concurrency": 2,
                "max_queue_depth": 10,
                "max_wait_seconds": 5,
                # lease_ttl = max(sync_timeout, 60) + 30 = 90s
                # But we'll manually use expired dispatch timestamps
                "sync_timeout_seconds": 5,
                "priority": 5,
            }
        },
    }
    config = AppConfig.model_validate(raw)
    redis = fake_aioredis.FakeRedis(connected=True, version=(7, 0, 0))
    logger = JSONLLogger(log_dir=str(tmp_path / "lease-logs"))
    await logger.start()

    # Set capacity
    await redis.set(slots_capacity_key("test-watcher-model"), 2)
    await redis.set(slots_in_use_key("test-watcher-model"), 0)

    watcher = LeaseWatcher(redis=redis, config=config, logger=logger)

    yield watcher, redis, config, logger

    await logger.stop()
    await redis.aclose()


@pytest.mark.asyncio
async def test_orphan_job_reclaimed(watcher_setup):
    """An inflight job with expired lease and no lease key is marked failed/worker_crash."""
    watcher, redis, config, logger = watcher_setup
    model = "test-watcher-model"

    # Inject an orphan job:
    # 1. Create the job hash with status=dispatched
    job_id = "orphan-job-001"
    await redis.hset(
        job_key(job_id),
        mapping={
            "status": JobStatus.DISPATCHED.value,
            "model": model,
            "caller": "test",
            "request_id": "req-001",
            "priority": "5",
            "enqueue_ts": "2026-01-01T00:00:00+00:00",
            "dispatch_ts": "2026-01-01T00:00:01+00:00",
        },
    )

    # 2. Add to inflight ZSET with a stale dispatch timestamp (far in the past)
    stale_dispatch_ts = time.time() - 999  # 999 seconds ago — well past any lease TTL
    await redis.zadd(inflight_key(model), {job_id: stale_dispatch_ts})

    # 3. Increment slot counter (simulate active dispatch)
    await redis.set(slots_in_use_key(model), 1)

    # 4. Do NOT create the lease key (simulate crash that cleared it)

    # Run the scan
    await watcher._scan_model(model)

    # Assert: job is marked failed/worker_crash
    data = await redis.hgetall(job_key(job_id))
    status = data.get(b"status", b"").decode()
    error_class = data.get(b"error_class", b"").decode()

    assert status == JobStatus.FAILED.value, f"Expected failed, got {status}"
    assert error_class == ErrorClass.WORKER_CRASH.value, f"Expected worker_crash, got {error_class}"


@pytest.mark.asyncio
async def test_orphan_slot_decremented(watcher_setup):
    """After reclaiming an orphan, the slot counter must be decremented."""
    watcher, redis, config, logger = watcher_setup
    model = "test-watcher-model"

    job_id = "orphan-job-002"
    await redis.hset(
        job_key(job_id),
        mapping={
            "status": JobStatus.DISPATCHED.value,
            "model": model,
            "caller": "test",
            "request_id": "req-002",
            "priority": "5",
            "enqueue_ts": "2026-01-01T00:00:00+00:00",
        },
    )
    stale_ts = time.time() - 999
    await redis.zadd(inflight_key(model), {job_id: stale_ts})
    await redis.set(slots_in_use_key(model), 1)  # 1 slot occupied

    await watcher._scan_model(model)

    in_use_after = int(await redis.get(slots_in_use_key(model)) or 0)
    assert in_use_after == 0, f"Slot not decremented: in_use={in_use_after}"


@pytest.mark.asyncio
async def test_orphan_added_to_dlq(watcher_setup):
    """After reclaiming an orphan, the job_id must appear in the DLQ SET."""
    watcher, redis, config, logger = watcher_setup
    model = "test-watcher-model"

    job_id = "orphan-job-003"
    await redis.hset(
        job_key(job_id),
        mapping={
            "status": JobStatus.DISPATCHED.value,
            "model": model,
            "caller": "test",
            "request_id": "req-003",
            "priority": "5",
        },
    )
    stale_ts = time.time() - 999
    await redis.zadd(inflight_key(model), {job_id: stale_ts})
    await redis.set(slots_in_use_key(model), 1)

    await watcher._scan_model(model)

    in_dlq = await redis.sismember(dlq_key(model), job_id)
    assert in_dlq, f"Job {job_id} not found in DLQ after reclaim"


@pytest.mark.asyncio
async def test_live_lease_not_reclaimed(watcher_setup):
    """A job with an active lease key must NOT be reclaimed."""
    watcher, redis, config, logger = watcher_setup
    model = "test-watcher-model"

    job_id = "live-job-001"
    await redis.hset(
        job_key(job_id),
        mapping={
            "status": JobStatus.DISPATCHED.value,
            "model": model,
            "caller": "test",
            "request_id": "req-live",
            "priority": "5",
        },
    )
    stale_ts = time.time() - 999  # looks old, but lease is still alive
    await redis.zadd(inflight_key(model), {job_id: stale_ts})
    await redis.set(slots_in_use_key(model), 1)

    # Create the lease key (alive)
    await redis.setex(lease_key(job_id), 300, "worker-xyz")

    await watcher._scan_model(model)

    # Status must remain dispatched (not reclaimed)
    data = await redis.hgetall(job_key(job_id))
    status = data.get(b"status", b"").decode()
    assert status == JobStatus.DISPATCHED.value, (
        f"Live-lease job was incorrectly reclaimed: status={status}"
    )


@pytest.mark.asyncio
async def test_inflight_removed_after_reclaim(watcher_setup):
    """After reclaiming an orphan, the job must be removed from the inflight ZSET."""
    watcher, redis, config, logger = watcher_setup
    model = "test-watcher-model"

    job_id = "orphan-job-004"
    await redis.hset(
        job_key(job_id),
        mapping={
            "status": JobStatus.DISPATCHED.value,
            "model": model,
            "caller": "test",
            "request_id": "req-004",
            "priority": "5",
        },
    )
    stale_ts = time.time() - 999
    await redis.zadd(inflight_key(model), {job_id: stale_ts})
    await redis.set(slots_in_use_key(model), 1)

    await watcher._scan_model(model)

    rank = await redis.zrank(inflight_key(model), job_id)
    assert rank is None, f"Job {job_id} still in inflight ZSET after reclaim"
