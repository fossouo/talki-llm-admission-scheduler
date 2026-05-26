"""Real-Redis integration tests for Lua scripts.

These tests are skipped unless REDIS_URL is set in the environment.
They exercise the actual Redis EVALSHA path to catch bugs that are invisible
to fakeredis's Lua interpreter (different Lua versions, NUL pattern handling,
global `unpack` availability, etc.).

To run locally:
    REDIS_URL=redis://localhost:6379/15 pytest tests/integration/test_real_redis.py -m real_redis -v

The tests flush DB 15 (isolated index) before/after each test case — never
touch DB 0 or any other production DB index.

Background — why this file exists:
    The production ENQUEUE_JOB_SCRIPT uses NUL bytes as field separators in a
    Lua string.gmatch character-class pattern and calls the global unpack().
    fakeredis's Lua interpreter (LuaJIT / lua5.1 subset) treats NUL in
    patterns as malformed and has no global unpack — both differences are
    silent in fakeredis but would crash on real Redis Lua 5.1.  The bench
    discovered this gap.  See conftest.py for the fakeredis compatibility shim
    and tests/README.md for the two-tier test policy.
"""

from __future__ import annotations

import asyncio
import os
import time

import pytest
import redis.asyncio as aioredis

# ---------------------------------------------------------------------------
# Skip entire module if REDIS_URL is not provided
# ---------------------------------------------------------------------------
pytestmark = pytest.mark.real_redis

REDIS_URL = os.environ.get("REDIS_URL", "")
REAL_REDIS_DB = 15  # dedicated isolation index — never touches DB 0


def _skip_if_no_redis() -> None:
    if not REDIS_URL:
        pytest.skip("REDIS_URL not set — skipping real-Redis tests")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def real_redis():
    """Connect to the real Redis at REDIS_URL, flush DB 15 around the test."""
    _skip_if_no_redis()
    # Force the DB index to 15 regardless of what the URL specifies
    url = REDIS_URL.rstrip("/")
    # Strip trailing /N if present and append /15
    import re
    url_base = re.sub(r"/\d+$", "", url)
    full_url = f"{url_base}/{REAL_REDIS_DB}"

    client = aioredis.from_url(
        full_url, encoding="utf-8", decode_responses=False
    )
    await client.flushdb()
    yield client
    await client.flushdb()
    await client.aclose()


@pytest.fixture
async def lua_scripts_real(real_redis):
    """Load the *production* Lua scripts (NUL separator) into real Redis."""
    # Import AFTER conftest has a chance to patch for fakeredis, but we need
    # the original production scripts here.  We re-read them from source.
    from app.storage import redis_lua as _mod

    # Save patched versions installed by conftest (for fakeredis tests)
    orig_enqueue = _mod.ENQUEUE_JOB_SCRIPT

    # Temporarily restore production script to load into real Redis
    from app.storage.redis_lua import (
        CANCEL_QUEUED_SCRIPT,
        CLAIM_SLOT_SCRIPT,
        LuaScripts,
    )

    # The production ENQUEUE_JOB_SCRIPT constant may have been replaced by
    # conftest.  Re-read the original from the module's __file__ to avoid
    # patched version.  We parse the source file to extract it.
    import ast
    import pathlib

    src = pathlib.Path(_mod.__file__).read_text()
    tree = ast.parse(src)
    prod_enqueue_script: str | None = None
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Assign)
            and any(
                isinstance(t, ast.Name) and t.id == "ENQUEUE_JOB_SCRIPT"
                for t in node.targets
            )
            and isinstance(node.value, ast.Constant)
        ):
            prod_enqueue_script = node.value.value
            break

    assert prod_enqueue_script is not None, "Could not extract ENQUEUE_JOB_SCRIPT from source"

    # Load all three scripts into real Redis
    claim_sha = await real_redis.script_load(CLAIM_SLOT_SCRIPT)
    cancel_sha = await real_redis.script_load(CANCEL_QUEUED_SCRIPT)
    enqueue_sha = await real_redis.script_load(prod_enqueue_script)

    lua = LuaScripts()
    # Bypass load_all so we can inject the pre-loaded SHAs
    lua._claim_slot_sha = claim_sha
    lua._cancel_queued_sha = cancel_sha
    lua._enqueue_job_sha = enqueue_sha

    # Also patch evalsha_enqueue_job to use NUL separator (production behaviour)
    async def _prod_evalsha_enqueue_job(
        self,
        redis,
        queue_k: str,
        job_k: str,
        job_id: str,
        score: float,
        max_depth: int,
        field_values: list,
    ) -> int:
        flat_str = "\0".join(str(v) for v in field_values)
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

    import types
    lua.evalsha_enqueue_job = types.MethodType(_prod_evalsha_enqueue_job, lua)

    return lua


# ---------------------------------------------------------------------------
# Test 1 — Lua scripts load without parse errors on real Redis
# ---------------------------------------------------------------------------


@pytest.mark.real_redis
async def test_lua_scripts_load_cleanly(real_redis):
    """SCRIPT LOAD must succeed for all three production Lua scripts.

    A malformed Lua pattern (e.g. NUL in character class on Redis < 6) would
    cause SCRIPT LOAD to raise a ResponseError at load time.  Passing this
    test means Redis accepted the script as syntactically valid Lua.
    """
    _skip_if_no_redis()
    import pathlib

    import app.storage.redis_lua as _mod

    src = pathlib.Path(_mod.__file__).read_text()
    import ast

    tree = ast.parse(src)
    scripts: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant):
            for t in node.targets:
                if isinstance(t, ast.Name) and t.id.endswith("_SCRIPT"):
                    scripts[t.id] = node.value.value

    assert len(scripts) == 3, f"Expected 3 Lua script constants, found {list(scripts.keys())}"

    for name, script in scripts.items():
        sha = await real_redis.script_load(script)
        assert sha, f"{name}: SCRIPT LOAD returned empty SHA"
        assert len(sha) == 40, f"{name}: SHA is not 40 hex chars: {sha!r}"

    # Verify all 3 scripts loaded successfully
    assert len(scripts) >= 3


# ---------------------------------------------------------------------------
# Test 2 — enqueue_job: ZADD score and HSET fields are correct
# ---------------------------------------------------------------------------


@pytest.mark.real_redis
async def test_enqueue_job_zadd_and_hset(real_redis, lua_scripts_real):
    """enqueue_job Lua on real Redis: verify ZADD score and HSET fields."""
    _skip_if_no_redis()
    lua = lua_scripts_real

    queue_k = "queue:chat-fast"
    job_id = "test-job-001"
    job_k = f"job:{job_id}"
    score = 5_000_000_000_000.0 + 1_700_000_000_000_000  # priority 5 + epoch ns
    max_depth = 40
    field_values = [
        "model", "chat-fast",
        "status", "queued",
        "priority", "5",
        "enqueue_ts", "2026-05-26T10:00:00+00:00",
        "caller", "test-suite",
    ]

    result = await lua.evalsha_enqueue_job(
        real_redis, queue_k, job_k, job_id, score, max_depth, field_values
    )

    # Assertion 1: script returned 1 (success)
    assert result == 1, f"enqueue_job returned {result}, expected 1"

    # Assertion 2: job_id is in the ZSET with the correct score
    actual_score = await real_redis.zscore(queue_k, job_id)
    assert actual_score is not None, "job_id not found in ZSET after enqueue"
    assert abs(float(actual_score) - score) < 1.0, (
        f"ZADD score mismatch: expected {score}, got {actual_score}"
    )

    # Assertion 3: HSET fields were written correctly
    job_hash = await real_redis.hgetall(job_k)
    assert job_hash, "job hash is empty after enqueue"

    def _f(key: str) -> str:
        k = key.encode() if isinstance(key, str) else key
        v = job_hash.get(k, b"")
        return v.decode() if isinstance(v, bytes) else v

    assert _f("model") == "chat-fast", f"model field mismatch: {_f('model')!r}"
    assert _f("status") == "queued", f"status field mismatch: {_f('status')!r}"
    assert _f("priority") == "5", f"priority field mismatch: {_f('priority')!r}"
    assert _f("caller") == "test-suite", f"caller field mismatch: {_f('caller')!r}"

    # Assertion 4: ZCARD == 1 (queue has exactly one entry)
    depth = await real_redis.zcard(queue_k)
    assert depth == 1, f"queue depth after single enqueue: expected 1, got {depth}"

    # Assertion 5: NUL-separator round-trip did not corrupt any field value
    # All decoded values must be non-empty and free of NUL bytes
    for k_bytes, v_bytes in job_hash.items():
        v_str = v_bytes.decode() if isinstance(v_bytes, bytes) else str(v_bytes)
        assert "\x00" not in v_str, (
            f"NUL byte found in field value for key {k_bytes!r}: {v_str!r}"
        )


# ---------------------------------------------------------------------------
# Test 3 — claim_slot: capacity respected and INCR is atomic
# ---------------------------------------------------------------------------


@pytest.mark.real_redis
async def test_claim_slot_capacity_respected(real_redis, lua_scripts_real):
    """claim_slot Lua on real Redis: slots:in_use never exceeds capacity."""
    _skip_if_no_redis()
    lua = lua_scripts_real

    model = "code"
    in_use_key = f"slots:{model}:in_use"
    cap_key = f"slots:{model}:capacity"

    capacity = 2
    await real_redis.set(in_use_key, 0)
    await real_redis.set(cap_key, capacity)

    ts_iso = "2026-05-26T10:00:00+00:00"

    # Claim 2 slots — both should succeed
    r1 = await lua.evalsha_claim_slot(real_redis, in_use_key, cap_key, "job-A", ts_iso)
    r2 = await lua.evalsha_claim_slot(real_redis, in_use_key, cap_key, "job-B", ts_iso)

    # Assertion 1 & 2: both claims succeeded
    assert r1 == 1, f"First claim returned {r1}, expected 1"
    assert r2 == 1, f"Second claim returned {r2}, expected 1"

    # Assertion 3: in_use equals capacity
    in_use = int(await real_redis.get(in_use_key))
    assert in_use == capacity, f"in_use={in_use} after 2 claims, expected {capacity}"

    # Attempt a third claim — must be rejected (at capacity)
    r3 = await lua.evalsha_claim_slot(real_redis, in_use_key, cap_key, "job-C", ts_iso)

    # Assertion 4: third claim returned 0 (rejected)
    assert r3 == 0, f"Third claim returned {r3}, expected 0 (at capacity)"

    # Assertion 5: in_use is still 2 (not incremented by rejected claim)
    in_use_after = int(await real_redis.get(in_use_key))
    assert in_use_after == capacity, (
        f"in_use={in_use_after} after rejected claim, expected {capacity}"
    )


# ---------------------------------------------------------------------------
# Test 4 — cancel_queued: removes from ZSET and updates hash
# ---------------------------------------------------------------------------


@pytest.mark.real_redis
async def test_cancel_queued(real_redis, lua_scripts_real):
    """cancel_queued Lua on real Redis: ZREM + HSET status=cancelled."""
    _skip_if_no_redis()
    lua = lua_scripts_real

    model = "chat-fast"
    queue_k = f"queue:{model}"
    job_id = "cancel-test-job"
    job_k = f"job:{job_id}"

    # Set up: enqueue the job first
    await real_redis.zadd(queue_k, {job_id: 5e12})
    await real_redis.hset(job_k, mapping={"status": "queued", "model": model})

    cancel_ts = "2026-05-26T10:05:00+00:00"
    result = await lua.evalsha_cancel_queued(
        real_redis, queue_k, job_k, job_id, cancel_ts
    )

    # Assertion 1: cancel returned 1 (found in queue)
    assert result == 1, f"cancel_queued returned {result}, expected 1"

    # Assertion 2: job no longer in ZSET
    score = await real_redis.zscore(queue_k, job_id)
    assert score is None, f"job still in queue ZSET after cancel: score={score}"

    # Assertion 3: status updated to 'cancelled'
    status = await real_redis.hget(job_k, "status")
    status_str = status.decode() if isinstance(status, bytes) else status
    assert status_str == "cancelled", f"status={status_str!r} after cancel, expected 'cancelled'"

    # Assertion 4: complete_ts was written
    complete_ts = await real_redis.hget(job_k, "complete_ts")
    assert complete_ts is not None, "complete_ts not written by cancel_queued"
    ts_str = complete_ts.decode() if isinstance(complete_ts, bytes) else complete_ts
    assert ts_str == cancel_ts, f"complete_ts={ts_str!r}, expected {cancel_ts!r}"

    # Assertion 5: cancelling a non-existent job returns 0
    result2 = await lua.evalsha_cancel_queued(
        real_redis, queue_k, job_k, "nonexistent-job", cancel_ts
    )
    assert result2 == 0, f"cancel of missing job returned {result2}, expected 0"


# ---------------------------------------------------------------------------
# Test 5 — rapid 100x concurrent enqueue: slots:in_use never exceeds capacity
# ---------------------------------------------------------------------------


@pytest.mark.real_redis
async def test_rapid_enqueue_claim_no_overflow(real_redis, lua_scripts_real):
    """100 concurrent enqueues + rapid claims must not overflow capacity.

    This is the regression test for the race condition that prompted the
    atomic Lua slot claim (Issue: LiteLLM #16011 equivalent for this stack).
    """
    _skip_if_no_redis()
    lua = lua_scripts_real

    model = "chat-fast"
    queue_k = f"queue:{model}"
    in_use_key = f"slots:{model}:in_use"
    cap_key = f"slots:{model}:capacity"
    capacity = 4  # matches default config

    await real_redis.set(in_use_key, 0)
    await real_redis.set(cap_key, capacity)

    max_depth = 40
    ts_iso = "2026-05-26T10:10:00+00:00"

    # Enqueue 100 jobs concurrently
    async def _enqueue(i: int) -> int:
        job_id = f"rapid-job-{i:04d}"
        job_k = f"job:{job_id}"
        score = 5e12 + i
        fv = [
            "model", model,
            "status", "queued",
            "priority", "5",
            "enqueue_ts", ts_iso,
            "caller", "stress-test",
        ]
        return await lua.evalsha_enqueue_job(
            real_redis, queue_k, job_k, job_id, score, max_depth, fv
        )

    enqueue_results = await asyncio.gather(*[_enqueue(i) for i in range(100)])

    # Assertion 1: no more than max_depth enqueues succeeded (queue bounded)
    n_enqueued = sum(enqueue_results)
    assert n_enqueued <= max_depth, (
        f"Enqueued {n_enqueued} jobs but max_depth={max_depth}"
    )

    # Assertion 2: ZCARD matches the count of successful enqueues
    zcard = await real_redis.zcard(queue_k)
    assert zcard == n_enqueued, (
        f"ZCARD={zcard} does not match successful enqueues={n_enqueued}"
    )

    # Now claim slots as fast as possible — dispatcher simulation
    async def _claim(i: int) -> int:
        job_id = f"rapid-job-{i:04d}"
        return await lua.evalsha_claim_slot(
            real_redis, in_use_key, cap_key, job_id, ts_iso
        )

    claim_results = await asyncio.gather(*[_claim(i) for i in range(100)])

    # Assertion 3: successful claims never exceed capacity
    n_claimed = sum(claim_results)
    assert n_claimed <= capacity, (
        f"Claimed {n_claimed} slots but capacity={capacity} — slot overflow detected!"
    )

    # Assertion 4: in_use counter == number of successful claims
    in_use_val = int(await real_redis.get(in_use_key) or 0)
    assert in_use_val == n_claimed, (
        f"slots:in_use={in_use_val} but successful claims={n_claimed}"
    )

    # Assertion 5: in_use never exceeded capacity (key check)
    assert in_use_val <= capacity, (
        f"slots:in_use={in_use_val} exceeds capacity={capacity}"
    )
