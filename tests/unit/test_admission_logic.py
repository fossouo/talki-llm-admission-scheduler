"""Unit tests for AdmissionController logic.

Uses fakeredis (or real Redis via SCHEDULER_REAL_REDIS). Tests:
  - First two requests are admitted/queued (HSET fields set)
  - Third request at max_queue_depth=3 is queued (queue not full)
  - Sixth request with depth=3 is rejected (queue full)
  - Queue-full returns AdmissionResult.REJECTED with correct fields
  - Idempotency: same key returns same job_id
"""

from __future__ import annotations

import asyncio

import pytest
import pytest_asyncio

from app.config import AppConfig
from app.core.admission import AdmissionController
from app.core.uuid7 import uuid7
from app.observability.jsonl_logger import JSONLLogger
from app.state import AdmissionResult, JobStatus
from app.storage.redis_keys import job_key, queue_key, slots_capacity_key, slots_in_use_key
from app.storage.redis_lua import LuaScripts


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def small_config(tmp_path) -> AppConfig:
    """Config with max_concurrency=2, max_queue_depth=3 for 'test-model'."""
    raw = {
        "scheduler": {
            "redis_url": "redis://localhost",
            "backend_litellm_url": "http://fake",
            "worker_poll_timeout_seconds": 1,
            "lease_watcher_interval_seconds": 1,
            "integrity_check_interval_seconds": 60,
            "log_dir": str(tmp_path / "logs"),
        },
        "defaults": {
            "max_concurrency": 2,
            "max_queue_depth": 3,
            "max_wait_seconds": 5,
            "sync_timeout_seconds": 5,
            "priority": 5,
        },
        "models": {},
    }
    return AppConfig.model_validate(raw)


@pytest_asyncio.fixture
async def logger(tmp_path) -> JSONLLogger:
    log = JSONLLogger(log_dir=str(tmp_path / "logs"))
    await log.start()
    yield log
    await log.stop()


@pytest_asyncio.fixture
async def admission_ctrl(redis_client, small_config, logger, lua_scripts):
    """AdmissionController wired to fakeredis and small config."""
    # Initialize capacity key
    await redis_client.set(slots_capacity_key("test-model"), 2)
    await redis_client.setnx(slots_in_use_key("test-model"), 0)

    ctrl = AdmissionController(
        redis=redis_client,
        config=small_config,
        lua=lua_scripts,
        logger=logger,
    )
    return ctrl


# ---------------------------------------------------------------------------
# Payload helper
# ---------------------------------------------------------------------------

_PAYLOAD = {
    "model": "test-model",
    "messages": [{"role": "user", "content": "hi"}],
}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_first_request_is_queued(admission_ctrl, redis_client):
    """First request must be enqueued with status=queued."""
    outcome, job_req = await admission_ctrl.admit(
        model="test-model",
        payload=_PAYLOAD,
        priority=None,
        caller="tester",
        caller_metadata=None,
        idempotency_key=None,
    )
    assert outcome.result == AdmissionResult.QUEUED
    assert outcome.job_id is not None

    # Job hash must be present in Redis with correct status
    data = await redis_client.hgetall(job_key(outcome.job_id))
    assert data  # must not be empty
    status = data.get(b"status", b"").decode()
    assert status == JobStatus.QUEUED.value


@pytest.mark.asyncio
async def test_second_request_also_queued(admission_ctrl, redis_client):
    """Two concurrent requests must both be queued (queue not full at depth=3)."""
    o1, _ = await admission_ctrl.admit(
        model="test-model", payload=_PAYLOAD, priority=None,
        caller="t1", caller_metadata=None, idempotency_key=None,
    )
    o2, _ = await admission_ctrl.admit(
        model="test-model", payload=_PAYLOAD, priority=None,
        caller="t2", caller_metadata=None, idempotency_key=None,
    )
    assert o1.result == AdmissionResult.QUEUED
    assert o2.result == AdmissionResult.QUEUED
    assert o1.job_id != o2.job_id


@pytest.mark.asyncio
async def test_third_request_queued_depth_not_exceeded(admission_ctrl, redis_client):
    """Third request (max_queue_depth=3) must still be queued."""
    for _ in range(3):
        o, _ = await admission_ctrl.admit(
            model="test-model", payload=_PAYLOAD, priority=None,
            caller="t", caller_metadata=None, idempotency_key=None,
        )
        assert o.result == AdmissionResult.QUEUED

    depth = await redis_client.zcard(queue_key("test-model"))
    assert depth == 3


@pytest.mark.asyncio
async def test_sixth_request_rejected_when_queue_full(admission_ctrl, redis_client):
    """After 3 queued jobs, the 4th request must be rejected (max_queue_depth=3)."""
    for _ in range(3):
        o, _ = await admission_ctrl.admit(
            model="test-model", payload=_PAYLOAD, priority=None,
            caller="t", caller_metadata=None, idempotency_key=None,
        )
        assert o.result == AdmissionResult.QUEUED

    # Now the queue is full — next must be rejected
    o_reject, _ = await admission_ctrl.admit(
        model="test-model", payload=_PAYLOAD, priority=None,
        caller="overflow", caller_metadata=None, idempotency_key=None,
    )
    assert o_reject.result == AdmissionResult.REJECTED
    assert o_reject.status == JobStatus.REJECTED
    assert o_reject.estimated_drain_s is not None and o_reject.estimated_drain_s > 0


@pytest.mark.asyncio
async def test_rejected_job_has_correct_fields(admission_ctrl, redis_client):
    """On rejection, the job_id must be present and result is REJECTED."""
    for _ in range(3):
        await admission_ctrl.admit(
            model="test-model", payload=_PAYLOAD, priority=None,
            caller="t", caller_metadata=None, idempotency_key=None,
        )
    o, _ = await admission_ctrl.admit(
        model="test-model", payload=_PAYLOAD, priority=None,
        caller="overflow", caller_metadata=None, idempotency_key=None,
    )
    assert o.result == AdmissionResult.REJECTED
    assert isinstance(o.job_id, str) and len(o.job_id) > 0


@pytest.mark.asyncio
async def test_idempotency_same_key_returns_same_job_id(admission_ctrl, redis_client):
    """Two requests with the same idempotency key must return the same job_id."""
    key = "test-idem-key-1"
    o1, _ = await admission_ctrl.admit(
        model="test-model", payload=_PAYLOAD, priority=None,
        caller="t", caller_metadata=None, idempotency_key=key,
    )
    assert o1.result == AdmissionResult.QUEUED

    o2, _ = await admission_ctrl.admit(
        model="test-model", payload=_PAYLOAD, priority=None,
        caller="t", caller_metadata=None, idempotency_key=key,
    )
    assert o2.result == AdmissionResult.IDEMPOTENT_HIT
    assert o2.existing_job_id == o1.job_id


@pytest.mark.asyncio
async def test_different_idempotency_keys_different_jobs(admission_ctrl, redis_client):
    """Different idempotency keys must produce different job IDs."""
    o1, _ = await admission_ctrl.admit(
        model="test-model", payload=_PAYLOAD, priority=None,
        caller="t", caller_metadata=None, idempotency_key="key-aaa",
    )
    o2, _ = await admission_ctrl.admit(
        model="test-model", payload=_PAYLOAD, priority=None,
        caller="t", caller_metadata=None, idempotency_key="key-bbb",
    )
    assert o1.job_id != o2.job_id


@pytest.mark.asyncio
async def test_job_hash_fields_set_on_enqueue(admission_ctrl, redis_client):
    """After admission, the job hash must contain model, status, priority, caller."""
    o, _ = await admission_ctrl.admit(
        model="test-model", payload=_PAYLOAD, priority=3,
        caller="test-caller", caller_metadata=None, idempotency_key=None,
    )
    data = await redis_client.hgetall(job_key(o.job_id))

    def _f(k: str) -> str:
        return data.get(k.encode(), b"").decode()

    assert _f("model") == "test-model"
    assert _f("status") == JobStatus.QUEUED.value
    assert _f("priority") == "3"
    assert _f("caller") == "test-caller"
    assert _f("enqueue_ts")  # non-empty timestamp


@pytest.mark.asyncio
async def test_no_idempotency_key_no_idem_record(admission_ctrl, redis_client):
    """Without an idempotency key, no idem: record should be stored."""
    o, _ = await admission_ctrl.admit(
        model="test-model", payload=_PAYLOAD, priority=None,
        caller="t", caller_metadata=None, idempotency_key=None,
    )
    # Scan all idem: keys — there should be none
    keys = await redis_client.keys("idem:*")
    assert len(keys) == 0


@pytest.mark.asyncio
async def test_queue_depth_is_accurate(admission_ctrl, redis_client):
    """Queue ZCARD must track exactly how many jobs have been admitted."""
    for i in range(3):
        await admission_ctrl.admit(
            model="test-model", payload=_PAYLOAD, priority=None,
            caller="t", caller_metadata=None, idempotency_key=None,
        )
    assert await redis_client.zcard(queue_key("test-model")) == 3
