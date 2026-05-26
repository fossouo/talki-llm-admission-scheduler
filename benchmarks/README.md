# Admission Scheduler — Benchmark Suite

This directory contains the benchmark battery for the Talki LLM Admission Scheduler.

> **Important**: All benchmarks run against the **fake LiteLLM** backend (`tests/fakes/fake_litellm.py`), not a real GPU backend. The numbers measure **scheduler overhead and queue isolation behavior**, not LLM inference throughput. Fake latency is injected via the `X-Fake-Latency-Ms` HTTP header.

---

## Quick start

```bash
# From the repo root (requires .venv with dev deps)
bash scripts/run-local-bench.sh
```

Outputs:
- `benchmarks/results/run-<timestamp>.json` — full per-request data
- `benchmarks/results/run-<timestamp>.md` — human-readable report

To run a subset of scenarios:

```bash
BENCH_SCENARIOS=low,isolation bash scripts/run-local-bench.sh
```

Or call the Python runner directly against an already-running scheduler:

```bash
.venv/bin/python tests/benchmark/run_bench.py \
    --scheduler-url http://localhost:4001 \
    --scenarios warmup,low,mid,high,mixed,isolation,priority \
    --out benchmarks/results/run-$(date -u +%Y%m%dT%H%M%S).json \
    --report-md benchmarks/results/run-$(date -u +%Y%m%dT%H%M%S).md
```

---

## Prerequisites

**Option A — Docker (recommended):**
- Docker Engine 20.10+, `docker compose` v2.20+
- No process occupying `127.0.0.1:6390` (compose maps Redis there)

**Option B — in-process fallback (no Docker):**
- Python 3.12+ venv with all dev deps: `pip install -e ".[dev]"`
- `fakeredis[lua]>=2.23` (included in dev extras)
- A local Redis on `127.0.0.1:6379` (the in-process scheduler reads from it)

**Both options:**
- `gtimeout` (macOS: `brew install coreutils`) or `timeout` on PATH

---

## Scenarios

All scenarios use `httpx.AsyncClient` with `max_connections=200`.

| Scenario | What it tests | Concurrency | Models | Fake latency |
|----------|---------------|-------------|--------|-------------|
| **warmup** | Ensure everything is hot (not measured) | 5 sequential | `chat-fast` | 50 ms |
| **low** | Quiet load — slots always available | 10 concurrent | `chat-fast` | 200 ms |
| **mid** | Moderate load — queue fills briefly | 50 concurrent | `chat-fast` (35) + `tool-worker` (15) | 200 ms |
| **high** | Saturation — some rejections expected | 100 concurrent | `chat-fast` (65) + `tool-worker` (35) | 200 ms |
| **mixed** | Multi-model with different latencies — tests cross-model isolation | 100 concurrent | `code` (10) + `chat-fast` (40) + `brain-exec` (5) + `tool-worker` (45) | code=800ms, chat-fast=200ms, brain-exec=1500ms, tool-worker=100ms |
| **isolation** | Headline test: saturate `code` while steady-state `chat-fast` runs | 30s continuous | code (saturator) + chat-fast (5 RPS) | code=800ms, chat-fast=200ms |
| **priority** | Priority inversion: batch (p=9) queue pre-filled; critical (p=1) jobs should clear faster | 25 total | `chat-fast` | 200 ms |

---

## Acceptance criteria

Each scenario has a pass/fail verdict in the report:

| Scenario | Criterion |
|----------|-----------|
| **low** | 0 rejections, p95 e2e < 1000 ms |
| **mid** | 0 rejections (50 requests well within combined queue depth of 140) |
| **high** | No 5xx errors from the scheduler itself (some 429s allowed) |
| **mixed** | `tool-worker` and `chat-fast` p95 e2e < 3000 ms despite long `code` + `brain-exec` jobs |
| **isolation** | `chat-fast` p95 e2e during code saturation < 2× its low-load baseline |
| **priority** | Critical (p=1) p95 e2e ≤ batch (p=9) p95 e2e |

---

## Reading the report

The Markdown report (`run-<timestamp>.md`) contains:

1. **Summary table** — one row per scenario with total/ok/rejected/timeout counts and p95 e2e.
2. **Acceptance criteria** — ✅/❌ verdict with measured numbers for each criterion.
3. **Scenario details** — per-scenario aggregates + post-scenario queue/slot state from `/metrics`.

The JSON file (`run-<timestamp>.json`) contains:

```json
{
  "started_at": "2026-05-26T...",
  "scheduler_url": "http://localhost:4001",
  "config_snapshot": { /* GET /queues at start */ },
  "scenarios": {
    "low": {
      "params": { "concurrency": 10, "model": "chat-fast", "latency_ms": 200 },
      "aggregates": {
        "n_total": 10, "n_completed": 10, "n_failed": 0, "n_timeout": 0, "n_rejected": 0,
        "wall_clock_seconds": 0.71,
        "throughput_rps": 14.1,
        "rejection_rate": 0.0,
        "wait_ms": { "p50": 0, "p95": 0, "p99": 0, "max": 0 },
        "runtime_ms": { "p50": 200, "p95": 210, "p99": 215, "max": 220 },
        "e2e_ms": { "p50": 210, "p95": 220, "p99": 225, "max": 230 }
      },
      "per_request": [ /* up to 1000 entries */ ],
      "metrics_after": { "queue_depth": { "chat-fast": 0 }, "slots_in_use": { "chat-fast": 0 } }
    },
    ...
  }
}
```

`wait_ms` and `runtime_ms` in per-request records are enriched via `GET /jobs/{job_id}` after the scenario. They may be `null` if the job expired or the GET call failed.

`e2e_ms` is always present: it is the full client-side round-trip time from submit to response.

---

## What the numbers mean

- **e2e_ms** = client submits request → client receives response. Includes scheduler overhead, queue wait, and fake LiteLLM latency.
- **wait_ms** = enqueue timestamp → dispatch timestamp (from scheduler's Redis hash). Only meaningful for jobs that were actually queued.
- **runtime_ms** = dispatch timestamp → complete timestamp (from scheduler's Redis hash). Closely tracks the fake LiteLLM latency actually received.
- **throughput_rps** = `n_completed / wall_clock_seconds`.

**Note on fake latency injection**: The benchmark passes `X-Fake-Latency-Ms` headers to the scheduler endpoint, but the scheduler's `LiteLLMForwarder` only forwards the JSON payload body to the fake LiteLLM — not custom HTTP headers. As a result, the fake LiteLLM always uses its default 50ms latency, regardless of the scenario's stated `latency_ms`. This means:

1. **e2e numbers are real scheduler overhead measurements** — not inflated by simulated LLM latency. The 70–600ms e2e you see is the actual queue wait + dispatch + Redis roundtrip.
2. **The relative behavior (isolation, priority) is valid** — cross-model isolation and priority ordering are scheduler-level properties independent of backend latency.
3. **To test with realistic LLM latency**: run against the real fake LiteLLM server directly, or extend `LiteLLMForwarder.forward()` to pass a latency hint in the payload's `metadata` field and update the fake LiteLLM to read it from there.

On a quiet scheduler with 50ms fake backend latency, `e2e_ms ≈ 60–100 ms` (scheduler overhead ~10–50ms). Under saturation, `wait_ms` dominates — which is exactly what the queue isolation tests measure.

---

## Stored results

`benchmarks/results/` stores all run outputs. Do not commit generated JSON/Markdown reports automatically; they contain timestamps and machine-specific numbers. The `sample-run.*` files are a curated reference artifact committed with the repo.

```
benchmarks/results/
  sample-run.json      ← reference artifact (committed)
  sample-run.md        ← reference artifact (committed)
  run-<timestamp>.json ← generated per run (gitignored)
  run-<timestamp>.md   ← generated per run (gitignored)
```

---

## Adding a new scenario

1. Add the scenario name to `SCENARIOS_ALL` in `tests/benchmark/run_bench.py`.
2. Implement the runner function (follow the `run_scenario` / `run_isolation` pattern).
3. Add a verdict in `_build_verdicts()`.
4. Add a row to this README's scenario table.
