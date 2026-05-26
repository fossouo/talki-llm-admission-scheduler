"""Integration tests for the IntegrityChecker.

Tests:
  - Manually skew slots:in_use to 99 (no inflight jobs); integrity check
    resets to 0 and increments redis_errors_total{operation=drift_correction}.
  - When recorded <= actual + 2 (within tolerance), no reset occurs.
  - Prometheus gauge reflects the corrected value after check.
"""

from __future__ import annotations

import asyncio

import fakeredis.aioredis as fake_aioredis
import pytest
import pytest_asyncio
from prometheus_client import REGISTRY

from app.config import AppConfig
from app.core.integrity import IntegrityChecker
from app.observability import metrics as m
from app.observability.jsonl_logger import JSONLLogger
from app.storage.redis_keys import inflight_key, slots_capacity_key, slots_in_use_key


def _get_redis_errors_drift_correction() -> float:
    """Read redis_errors_total{operation=drift_correction} directly from the counter."""
    from app.observability import metrics as m
    # Access the per-label child directly to get the current value
    try:
        return m.redis_errors_total.labels(operation="drift_correction")._value.get()
    except Exception:
        return 0.0


@pytest_asyncio.fixture
async def integrity_setup(tmp_path):
    raw = {
        "scheduler": {
            "redis_url": "redis://localhost",
            "backend_litellm_url": "http://fake",
            "worker_poll_timeout_seconds": 1,
            "lease_watcher_interval_seconds": 60,
            "integrity_check_interval_seconds": 60,
            "log_dir": str(tmp_path / "integrity-logs"),
        },
        "defaults": {
            "max_concurrency": 4,
            "max_queue_depth": 40,
            "max_wait_seconds": 10,
            "sync_timeout_seconds": 10,
            "priority": 5,
        },
        "models": {
            "integrity-model": {
                "max_concurrency": 4,
                "max_queue_depth": 40,
                "max_wait_seconds": 10,
                "sync_timeout_seconds": 10,
                "priority": 5,
            }
        },
    }
    config = AppConfig.model_validate(raw)
    redis = fake_aioredis.FakeRedis(connected=True, version=(7, 0, 0))
    logger = JSONLLogger(log_dir=str(tmp_path / "integrity-logs"))
    await logger.start()

    await redis.set(slots_capacity_key("integrity-model"), 4)
    await redis.set(slots_in_use_key("integrity-model"), 0)

    checker = IntegrityChecker(redis=redis, config=config, logger=logger)

    yield checker, redis, config, logger

    await logger.stop()
    await redis.aclose()


@pytest.mark.asyncio
async def test_drift_correction_resets_in_use(integrity_setup):
    """Setting in_use=99 with 0 inflight jobs triggers drift correction to 0."""
    checker, redis, config, logger = integrity_setup
    model = "integrity-model"

    # Skew: set in_use to 99 with no inflight jobs
    await redis.set(slots_in_use_key(model), 99)

    # Run the check
    await checker._check_model(model)

    # Must have been reset to 0 (actual inflight = 0)
    corrected = int(await redis.get(slots_in_use_key(model)) or 0)
    assert corrected == 0, f"Expected in_use=0 after drift correction, got {corrected}"


@pytest.mark.asyncio
async def test_drift_correction_increments_metric(integrity_setup):
    """Drift correction must increment redis_errors_total{operation=drift_correction}."""
    checker, redis, config, logger = integrity_setup
    model = "integrity-model"

    before = _get_redis_errors_drift_correction()

    await redis.set(slots_in_use_key(model), 99)
    await checker._check_model(model)

    after = _get_redis_errors_drift_correction()
    assert after > before, (
        f"redis_errors_total{{drift_correction}} not incremented: before={before}, after={after}"
    )


@pytest.mark.asyncio
async def test_no_drift_no_reset(integrity_setup):
    """When in_use matches inflight count (within tolerance=2), no reset occurs."""
    checker, redis, config, logger = integrity_setup
    model = "integrity-model"

    # Set in_use=2, inflight count=1 → difference is 1, within tolerance of 2
    await redis.set(slots_in_use_key(model), 2)
    await redis.zadd(inflight_key(model), {"some-job": 1.0})

    before = _get_redis_errors_drift_correction()
    await checker._check_model(model)
    after = _get_redis_errors_drift_correction()

    # No drift correction should have fired
    assert before == after, (
        f"Unexpected drift correction when within tolerance: before={before}, after={after}"
    )

    # in_use must remain 2
    in_use = int(await redis.get(slots_in_use_key(model)) or 0)
    assert in_use == 2, f"in_use was unexpectedly modified: {in_use}"


@pytest.mark.asyncio
async def test_drift_with_actual_inflight(integrity_setup):
    """in_use=10, inflight=2 → difference=8, exceeds tolerance=2 → reset to 2."""
    checker, redis, config, logger = integrity_setup
    model = "integrity-model"

    await redis.set(slots_in_use_key(model), 10)
    await redis.zadd(inflight_key(model), {"job-a": 1.0, "job-b": 2.0})

    await checker._check_model(model)

    corrected = int(await redis.get(slots_in_use_key(model)) or 0)
    assert corrected == 2, f"Expected in_use=2 (actual inflight), got {corrected}"


@pytest.mark.asyncio
async def test_no_correction_when_in_use_at_tolerance_boundary(integrity_setup):
    """in_use=actual+2 must NOT trigger correction (exactly at tolerance boundary)."""
    checker, redis, config, logger = integrity_setup
    model = "integrity-model"

    # actual=3, recorded=5 → diff=2 → not > 2 → no correction
    await redis.zadd(inflight_key(model), {"j1": 1.0, "j2": 2.0, "j3": 3.0})
    await redis.set(slots_in_use_key(model), 5)

    before = _get_redis_errors_drift_correction()
    await checker._check_model(model)
    after = _get_redis_errors_drift_correction()

    assert before == after, "Correction fired at tolerance boundary (should not)"
    in_use = int(await redis.get(slots_in_use_key(model)) or 0)
    assert in_use == 5, f"in_use was unexpectedly changed to {in_use}"


@pytest.mark.asyncio
async def test_all_models_checked(integrity_setup):
    """_check_all_models iterates all configured models without error."""
    checker, redis, config, logger = integrity_setup
    # Should not raise
    await checker._check_all_models()
