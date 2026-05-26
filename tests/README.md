# Test Suite — Policy and Structure

## Two-Tier Test Policy

### Tier 1 — fakeredis (default, always runs in CI)

All tests in `tests/unit/` and most tests in `tests/integration/` use
[fakeredis](https://github.com/cunla/fakeredis-py) (`fakeredis[lua]>=2.23`).
fakeredis is sufficient for:

- Unit-level logic checks (admission decisions, priority scoring, idempotency)
- State machine transitions (queued → dispatched → completed)
- HTTP endpoint contracts (request/response shapes, status codes)
- Concurrency fixtures (multiple workers, cancellation races)

fakeredis is **NOT** sufficient for:

- Any code path that calls `EVAL` / `EVALSHA` with Lua features that differ
  between fakeredis's Lua interpreter and real Redis Lua 5.1.

Known divergences that have caused silent failures:

| Feature | fakeredis | real Redis Lua 5.1 |
|---|---|---|
| NUL byte (`\0`) in `string.gmatch` pattern | treated as malformed pattern | works (NUL terminates the pattern token) |
| Global `unpack()` | not available | available |
| `string.char(0)` in pattern class `[^%z]` | may error | works |

These differences are why `conftest.py` patches `ENQUEUE_JOB_SCRIPT` to use a
pipe (`|`) separator and explicit per-field HSET calls for all unit/integration
tests. The patch is semantically equivalent but avoids all three divergences.

### Tier 2 — real Redis (`@pytest.mark.real_redis`)

Tests decorated with `@pytest.mark.real_redis` require a real Redis instance.
They are **skipped by default in CI** and must be run explicitly by developers
before any change to the Lua scripts in `app/storage/redis_lua.py`.

These tests exercise the production Lua scripts unchanged — NUL separator,
`string.gmatch`, `unpack()` — against a real Redis 7 server to verify that
the scripts parse and execute correctly.

**When to run real-Redis tests**:

- Before any PR that modifies `app/storage/redis_lua.py`
- Before any PR that changes how `evalsha_enqueue_job`, `evalsha_claim_slot`,
  or `evalsha_cancel_queued` are called from the dispatcher or admission controller
- As a preflight before running the benchmark (done automatically by
  `scripts/run-local-bench.sh` when `REDIS_URL` is set)

**How to run**:

```bash
# Start Redis (Docker is easiest)
docker run -d --rm -p 6379:6379 redis:7-alpine

# Run only real-Redis tests against DB 15 (isolated, never touches DB 0)
REDIS_URL=redis://localhost:6379/15 pytest tests/integration/test_real_redis.py -m real_redis -v

# Or run as bench preflight (done automatically by scripts/run-local-bench.sh)
REDIS_URL=redis://localhost:6379/15 bash scripts/run-local-bench.sh
```

The tests flush DB 15 before and after each test case.  They never read or
write to any other database index.

## Directory Structure

```
tests/
├── conftest.py                      Shared fixtures; fakeredis compatibility shims
├── README.md                        This file
├── fakes/
│   └── fake_litellm.py              Fake LiteLLM FastAPI server for end-to-end tests
├── unit/
│   ├── test_admission_logic.py      AdmissionController unit tests
│   ├── test_config_load.py          Config parsing and validation
│   ├── test_jsonl_logger.py         JSONL logger unit tests
│   ├── test_metrics_registration.py Prometheus metric registration
│   ├── test_redis_keys.py           Redis key helper functions
│   └── test_uuid7.py                UUID v7 generation
├── integration/
│   ├── test_cancellation.py         Cancel API (queued and dispatched)
│   ├── test_concurrency.py          Concurrent request handling
│   ├── test_endpoints_async.py      Async submit + poll endpoints
│   ├── test_endpoints_health.py     Health endpoint
│   ├── test_endpoints_sync.py       Sync chat completion endpoint
│   ├── test_failure_modes.py        Backend failures, timeouts, network errors
│   ├── test_idempotency.py          Idempotency key deduplication
│   ├── test_integrity_check.py      Slot drift correction
│   ├── test_lease_watcher.py        Lease expiry and worker crash recovery
│   └── test_real_redis.py           ⚠️  Real Redis only (@pytest.mark.real_redis)
└── benchmark/
    └── run_bench.py                 Benchmark battery (not a pytest test file)
```

## Running the Full Suite

```bash
# All fakeredis tests (CI default)
pytest tests/ -x --tb=short

# All tests including real-Redis (requires REDIS_URL)
REDIS_URL=redis://localhost:6379/15 pytest tests/ --tb=short

# Only real-Redis tests
REDIS_URL=redis://localhost:6379/15 pytest -m real_redis -v

# With coverage
pytest tests/ --cov=app --cov-report=term-missing
```
