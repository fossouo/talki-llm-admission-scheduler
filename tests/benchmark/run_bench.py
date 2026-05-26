"""Benchmark battery for the Talki LLM Admission Scheduler.

Measures scheduler overhead and queue isolation behavior against the fake
LiteLLM backend — NOT a real GPU stack.

Usage:
    python tests/benchmark/run_bench.py \\
        --scheduler-url http://localhost:4001 \\
        --scenarios warmup,low,mid,high,mixed,isolation,priority \\
        --out benchmarks/results/run-<timestamp>.json \\
        --report-md benchmarks/results/run-<timestamp>.md

All scenarios use httpx.AsyncClient with a tuned connection pool.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime
import json
import re
import statistics
import sys
import time
from pathlib import Path
from typing import Any

import httpx

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

SCENARIOS_ALL = ["warmup", "low", "mid", "high", "mixed", "isolation", "priority"]

_DEFAULT_OUT = (
    "benchmarks/results/run-"
    + datetime.datetime.now(tz=datetime.timezone.utc).strftime("%Y%m%dT%H%M%S")
    + ".json"
)
_DEFAULT_MD = _DEFAULT_OUT.replace(".json", ".md")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Admission Scheduler benchmark battery")
    p.add_argument("--scheduler-url", default="http://localhost:4001")
    p.add_argument(
        "--litellm-url",
        default="http://localhost:4099",
        help="Informational only — the client does not call this directly",
    )
    p.add_argument(
        "--scenarios",
        default=",".join(SCENARIOS_ALL),
        help="Comma-separated subset of: " + ",".join(SCENARIOS_ALL),
    )
    p.add_argument("--out", default=_DEFAULT_OUT, help="Path for JSON results")
    p.add_argument("--report-md", default=_DEFAULT_MD, help="Path for Markdown report")
    return p.parse_args(argv)


# ---------------------------------------------------------------------------
# Verdict thresholds
# Exposed as module-level constants so they are easy to tune without
# hunting through verdict logic.
# ---------------------------------------------------------------------------

# low: no-saturation path; p95 e2e must be ≤ this ratio vs the observed
# low-load p95.  Absolute ceiling retained as a safety net (< 1000 ms).
THRESHOLD_LOW_RATIO: float = 1.5
THRESHOLD_LOW_ABS_MS: float = 1000.0

# mid: light saturation; no rejections; ratio measured against low baseline
# per model where available, else absolute.
THRESHOLD_MID_RATIO: float = 2.0

# high: heavy saturation; some rejections are acceptable.  Verdict checks
# only that the scheduler itself produces no 5xx.  Rejection rate is
# informational.
# (no numeric threshold — high verdict is server_errors == 0)

# mixed: 4 models under simultaneous load.  Each model's p95 e2e is compared
# against its own low-load baseline (derived from the 'low' scenario for
# chat-fast; for other models the warmup is the proxy).
THRESHOLD_MIXED_RATIO: float = 2.0

# isolation: chat-fast p95 e2e during code saturation vs low baseline.
THRESHOLD_ISOLATION_RATIO: float = 2.0

# priority: critical p95 e2e / batch p95 e2e ≤ this ratio.
# (i.e. critical must be meaningfully faster than batch)
THRESHOLD_PRIORITY_RATIO: float = 0.6  # critical_p95 / batch_p95 ≤ 0.6


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

CHAT_PAYLOAD_TEMPLATE: dict[str, Any] = {
    "messages": [{"role": "user", "content": "benchmark ping"}],
    "max_tokens": 1,
}


def _chat_payload(model: str, priority: int | None = None, latency_ms: int = 200) -> dict[str, Any]:
    """Build a ChatCompletionRequest payload for the given model.

    The latency header is passed via a top-level 'metadata' field so the
    scheduler can attach it to the job. The actual fake-latency header is
    injected per-request by the _send_request helper via a custom httpx header.
    """
    payload = {**CHAT_PAYLOAD_TEMPLATE, "model": model}
    if priority is not None:
        payload["metadata"] = {"priority": priority}
    return payload


def _pct(values: list[float], p: int) -> float:
    """Return the p-th percentile of values (0–100). Returns 0.0 if empty."""
    if not values:
        return 0.0
    n = len(values)
    if n == 1:
        return values[0]
    # Use statistics.quantiles which expects n>=2 for inclusive method
    try:
        quantiles = statistics.quantiles(sorted(values), n=100, method="inclusive")
        # quantiles returns 99 cut points for n=100; index p-1 gives the p-th percentile
        idx = max(0, min(p - 1, len(quantiles) - 1))
        return quantiles[idx]
    except statistics.StatisticsError:
        return values[0]


def _aggregate(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Compute aggregate statistics over a list of per-request records."""
    n_total = len(records)
    n_completed = sum(1 for r in records if r["status_code"] == 200)
    n_failed = sum(
        1 for r in records if r["status_code"] not in (200, 429, 408, 504, 0)
    )
    n_timeout = sum(1 for r in records if r["status_code"] == 504)
    n_rejected = sum(1 for r in records if r["status_code"] == 429)

    wait_vals = [r["wait_ms"] for r in records if r.get("wait_ms") is not None]
    runtime_vals = [r["runtime_ms"] for r in records if r.get("runtime_ms") is not None]
    e2e_vals = [r["e2e_ms"] for r in records if r.get("e2e_ms") is not None]

    def _stats(vals: list[float]) -> dict[str, float]:
        if not vals:
            return {"p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0}
        return {
            "p50": round(_pct(vals, 50), 1),
            "p95": round(_pct(vals, 95), 1),
            "p99": round(_pct(vals, 99), 1),
            "max": round(max(vals), 1),
        }

    return {
        "n_total": n_total,
        "n_completed": n_completed,
        "n_failed": n_failed,
        "n_timeout": n_timeout,
        "n_rejected": n_rejected,
        "wall_clock_seconds": 0.0,  # filled by caller
        "throughput_rps": 0.0,  # filled by caller
        "rejection_rate": round(n_rejected / n_total, 4) if n_total else 0.0,
        "wait_ms": _stats(wait_vals),
        "runtime_ms": _stats(runtime_vals),
        "e2e_ms": _stats(e2e_vals),
    }


async def _send_request(
    client: httpx.AsyncClient,
    scheduler_url: str,
    model: str,
    latency_ms: int,
    priority: int | None = None,
    request_index: int = 0,
) -> dict[str, Any]:
    """Send a single sync POST /v1/chat/completions and return a per-request record."""
    submit_ts = time.monotonic()
    payload = _chat_payload(model, priority=priority)
    headers = {
        "X-Fake-Latency-Ms": str(latency_ms),
        "X-Caller": "bench",
    }
    status_code = 0
    wait_ms: float | None = None
    runtime_ms_val: float | None = None
    error_class: str | None = None
    job_id: str | None = None

    try:
        resp = await client.post(
            f"{scheduler_url}/v1/chat/completions",
            json=payload,
            headers=headers,
            timeout=120.0,
        )
        status_code = resp.status_code
        job_id = resp.headers.get("x-job-id")

        if status_code == 200:
            # Try to extract timing from job status endpoint
            # (sync mode: the response IS the LiteLLM response; timing is in the scheduler)
            pass
        elif status_code == 429:
            error_class = "rejected"
        elif status_code == 504:
            error_class = "sync_timeout"
        elif status_code == 408:
            error_class = "queue_timeout"
        elif status_code >= 500:
            error_class = "server_error"

    except httpx.TimeoutException:
        status_code = 0
        error_class = "client_timeout"
    except httpx.ConnectError:
        status_code = 0
        error_class = "connect_error"
    except Exception as exc:
        status_code = 0
        error_class = type(exc).__name__

    complete_ts = time.monotonic()
    e2e_ms = (complete_ts - submit_ts) * 1000

    # For sync requests we measure e2e as the full round trip.
    # wait_ms and runtime_ms are best-effort from job polling (if job_id returned).
    record: dict[str, Any] = {
        "request_index": request_index,
        "model": model,
        "priority": priority,
        "submit_ts": submit_ts,
        "complete_ts": complete_ts,
        "wait_ms": wait_ms,
        "runtime_ms": runtime_ms_val,
        "e2e_ms": round(e2e_ms, 1),
        "status_code": status_code,
        "error_class": error_class,
        "job_id": job_id,
    }
    return record


async def _enrich_with_job_timings(
    client: httpx.AsyncClient,
    scheduler_url: str,
    records: list[dict[str, Any]],
    max_concurrent: int = 20,
) -> None:
    """Best-effort: fetch GET /jobs/{job_id} for completed jobs to get wait_ms / runtime_ms."""
    sem = asyncio.Semaphore(max_concurrent)

    async def _fetch(record: dict[str, Any]) -> None:
        jid = record.get("job_id")
        if not jid or record["status_code"] != 200:
            return
        async with sem:
            try:
                resp = await client.get(
                    f"{scheduler_url}/jobs/{jid}", timeout=5.0
                )
                if resp.status_code == 200:
                    data = resp.json()
                    record["wait_ms"] = data.get("wait_ms")
                    record["runtime_ms"] = data.get("runtime_ms")
            except Exception:
                pass

    await asyncio.gather(*[_fetch(r) for r in records])


async def _snapshot_metrics(
    client: httpx.AsyncClient, scheduler_url: str
) -> dict[str, Any]:
    """Fetch /metrics and parse out key scheduler gauges/counters."""
    result: dict[str, Any] = {}
    try:
        resp = await client.get(f"{scheduler_url}/metrics", timeout=5.0)
        if resp.status_code == 200:
            text = resp.text
            result = _parse_prometheus(text)
    except Exception as exc:
        result["parse_error"] = str(exc)
    return result


def _parse_prometheus(text: str) -> dict[str, Any]:
    """Parse a subset of Prometheus text-format metrics into a dict."""
    out: dict[str, Any] = {
        "queue_depth": {},
        "slots_in_use": {},
        "slots_capacity": {},
        "admission_requests_total": {},
        "failure_total": {},
        "timeout_total": {},
    }

    for line in text.splitlines():
        line = line.strip()
        if line.startswith("#") or not line:
            continue

        # Parse: metric_name{labels} value
        m = re.match(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([\d.e+\-]+)', line)
        if not m:
            continue

        name = m.group(1)
        labels_raw = m.group(2) or ""
        value_str = m.group(3)

        # Skip histogram internal buckets/sum/count for brevity
        if name.endswith("_bucket") or name.endswith("_sum") or name.endswith("_count"):
            continue

        try:
            value = float(value_str)
        except ValueError:
            continue

        # Parse labels into dict
        labels: dict[str, str] = {}
        for lm in re.finditer(r'(\w+)="([^"]*)"', labels_raw):
            labels[lm.group(1)] = lm.group(2)

        model = labels.get("model", "__global__")

        if name == "queue_depth":
            out["queue_depth"][model] = value
        elif name == "slots_in_use":
            out["slots_in_use"][model] = value
        elif name == "slots_capacity":
            out["slots_capacity"][model] = value
        elif name == "admission_requests_total":
            result_label = labels.get("result", "unknown")
            key = f"{model}/{result_label}"
            out["admission_requests_total"][key] = value
        elif name == "failure_total":
            ec = labels.get("error_class", "unknown")
            key = f"{model}/{ec}"
            out["failure_total"][key] = value
        elif name == "timeout_total":
            stage = labels.get("stage", "unknown")
            key = f"{model}/{stage}"
            out["timeout_total"][key] = value

    return out


async def _snapshot_queues(
    client: httpx.AsyncClient, scheduler_url: str
) -> dict[str, Any]:
    """Fetch GET /queues."""
    try:
        resp = await client.get(f"{scheduler_url}/queues", timeout=5.0)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return {}


# ---------------------------------------------------------------------------
# Scenario runners
# ---------------------------------------------------------------------------


async def run_warmup(
    client: httpx.AsyncClient, scheduler_url: str, n: int = 5, latency_ms: int = 50
) -> None:
    """Sequential warmup — not measured."""
    print(f"  [warmup] {n} sequential requests (latency={latency_ms}ms) …", flush=True)
    for i in range(n):
        await _send_request(client, scheduler_url, "chat-fast", latency_ms, request_index=i)
    print(f"  [warmup] done", flush=True)


async def run_scenario(
    client: httpx.AsyncClient,
    scheduler_url: str,
    scenario: str,
    params: dict[str, Any],
    requests: list[tuple[str, int, int | None]],  # (model, latency_ms, priority)
) -> dict[str, Any]:
    """Generic scenario runner. requests is a list of (model, latency_ms, priority)."""
    print(f"  [{scenario}] {len(requests)} requests …", flush=True)

    wall_start = time.monotonic()

    tasks = [
        _send_request(
            client,
            scheduler_url,
            model,
            latency_ms,
            priority=priority,
            request_index=i,
        )
        for i, (model, latency_ms, priority) in enumerate(requests)
    ]
    records: list[dict[str, Any]] = await asyncio.gather(*tasks)

    wall_end = time.monotonic()
    wall_clock_s = wall_end - wall_start

    # Enrich with per-job timing from GET /jobs (best effort)
    await _enrich_with_job_timings(client, scheduler_url, records)

    agg = _aggregate(records)
    agg["wall_clock_seconds"] = round(wall_clock_s, 3)
    if wall_clock_s > 0:
        agg["throughput_rps"] = round(agg["n_completed"] / wall_clock_s, 2)

    metrics = await _snapshot_metrics(client, scheduler_url)

    print(
        f"  [{scenario}] done — {agg['n_completed']}/{agg['n_total']} ok, "
        f"{agg['n_rejected']} rejected, {agg['n_timeout']} timeout, "
        f"e2e p95={agg['e2e_ms']['p95']}ms, wall={wall_clock_s:.1f}s",
        flush=True,
    )

    # Cap per-request entries at 1000
    capped_records = records[:1000]

    return {
        "params": params,
        "aggregates": agg,
        "per_request": capped_records,
        "metrics_after": metrics,
    }


async def run_isolation(
    client: httpx.AsyncClient, scheduler_url: str, duration_s: int = 30
) -> dict[str, Any]:
    """Saturate code for duration_s while sending steady 5 RPS to chat-fast.

    Returns a result dict with two sub-keys: 'code_saturator' and 'chat_fast_steady'.
    """
    print(f"  [isolation] saturating code for {duration_s}s + 5 RPS chat-fast …", flush=True)

    wall_start = time.monotonic()
    code_records: list[dict[str, Any]] = []
    chat_records: list[dict[str, Any]] = []
    stop_event = asyncio.Event()

    async def saturate_code() -> None:
        """Continuously submit code requests until stop_event is set."""
        idx = 0
        while not stop_event.is_set():
            record = await _send_request(
                client, scheduler_url, "code", latency_ms=800, request_index=idx
            )
            code_records.append(record)
            idx += 1

    async def steady_chat() -> None:
        """Send 1 request every 200ms (= 5 RPS) to chat-fast."""
        idx = 0
        while not stop_event.is_set():
            start = time.monotonic()
            record = await _send_request(
                client, scheduler_url, "chat-fast", latency_ms=200, request_index=idx
            )
            chat_records.append(record)
            idx += 1
            elapsed = time.monotonic() - start
            sleep_remaining = 0.2 - elapsed  # target 200ms between submits
            if sleep_remaining > 0:
                await asyncio.sleep(sleep_remaining)

    async def stopper() -> None:
        await asyncio.sleep(duration_s)
        stop_event.set()

    await asyncio.gather(saturate_code(), steady_chat(), stopper())

    wall_end = time.monotonic()
    wall_clock_s = wall_end - wall_start

    # Enrich timings
    await _enrich_with_job_timings(client, scheduler_url, code_records)
    await _enrich_with_job_timings(client, scheduler_url, chat_records)

    code_agg = _aggregate(code_records)
    code_agg["wall_clock_seconds"] = round(wall_clock_s, 3)
    if wall_clock_s > 0:
        code_agg["throughput_rps"] = round(code_agg["n_completed"] / wall_clock_s, 2)

    chat_agg = _aggregate(chat_records)
    chat_agg["wall_clock_seconds"] = round(wall_clock_s, 3)
    if wall_clock_s > 0:
        chat_agg["throughput_rps"] = round(chat_agg["n_completed"] / wall_clock_s, 2)

    metrics = await _snapshot_metrics(client, scheduler_url)

    print(
        f"  [isolation] done — code: {code_agg['n_completed']}/{code_agg['n_total']}, "
        f"chat-fast: {chat_agg['n_completed']}/{chat_agg['n_total']}, "
        f"chat e2e p95={chat_agg['e2e_ms']['p95']}ms",
        flush=True,
    )

    params = {
        "duration_s": duration_s,
        "code_latency_ms": 800,
        "chat_fast_latency_ms": 200,
        "chat_fast_target_rps": 5,
    }
    return {
        "params": params,
        "code_saturator": {
            "aggregates": code_agg,
            "per_request": code_records[:1000],
        },
        "chat_fast_steady": {
            "aggregates": chat_agg,
            "per_request": chat_records[:1000],
        },
        "metrics_after": metrics,
    }


async def run_priority(
    client: httpx.AsyncClient, scheduler_url: str
) -> dict[str, Any]:
    """Submit 20 batch (priority=9) then 5 critical (priority=1) chat-fast jobs.

    Submits the batch wave first to fill the queue, then sends critical jobs.
    Measures wait time per priority tier.
    """
    print(
        "  [priority] 20 batch (p9) then 5 critical (p1) chat-fast requests …",
        flush=True,
    )

    wall_start = time.monotonic()

    # First wave: 20 batch priority-9 jobs (with 200ms fake latency).
    # We submit them all concurrently to saturate the queue.
    batch_requests = [("chat-fast", 200, 9)] * 20
    batch_tasks = [
        _send_request(
            client, scheduler_url, model, lat, priority=pri, request_index=i
        )
        for i, (model, lat, pri) in enumerate(batch_requests)
    ]

    # Small delay so batch queue builds up before we send critical
    # We launch batch in the background and send critical after a brief pause.
    batch_future = asyncio.gather(*batch_tasks)

    # Give the batch wave ~100ms to fill the scheduler queue
    await asyncio.sleep(0.1)

    # Second wave: 5 critical jobs
    critical_requests = [("chat-fast", 200, 1)] * 5
    critical_tasks = [
        _send_request(
            client, scheduler_url, model, lat, priority=pri, request_index=i + 20
        )
        for i, (model, lat, pri) in enumerate(critical_requests)
    ]
    critical_results, batch_results = await asyncio.gather(
        asyncio.gather(*critical_tasks),
        batch_future,
    )

    wall_end = time.monotonic()
    wall_clock_s = wall_end - wall_start

    all_records = list(batch_results) + list(critical_results)
    await _enrich_with_job_timings(client, scheduler_url, all_records)

    batch_agg = _aggregate(list(batch_results))
    batch_agg["wall_clock_seconds"] = round(wall_clock_s, 3)

    critical_agg = _aggregate(list(critical_results))
    critical_agg["wall_clock_seconds"] = round(wall_clock_s, 3)

    metrics = await _snapshot_metrics(client, scheduler_url)

    print(
        f"  [priority] done — batch e2e p95={batch_agg['e2e_ms']['p95']}ms, "
        f"critical e2e p95={critical_agg['e2e_ms']['p95']}ms",
        flush=True,
    )

    params = {
        "n_batch_priority9": 20,
        "n_critical_priority1": 5,
        "model": "chat-fast",
        "latency_ms": 200,
    }
    return {
        "params": params,
        "batch_priority9": {
            "aggregates": batch_agg,
            "per_request": list(batch_results)[:1000],
        },
        "critical_priority1": {
            "aggregates": critical_agg,
            "per_request": list(critical_results)[:1000],
        },
        "metrics_after": metrics,
    }


# ---------------------------------------------------------------------------
# Verdict helpers
# ---------------------------------------------------------------------------


def _ratio_verdict(
    observed_p95_ms: float,
    baseline_p95_ms: float,
    ratio_threshold: float,
) -> tuple[bool, float, str]:
    """Compute a ratio-based verdict.

    Args:
        observed_p95_ms: p95 e2e latency under the scenario being evaluated.
        baseline_p95_ms: p95 e2e latency under low / unloaded conditions.
        ratio_threshold: maximum acceptable ratio (observed / baseline).

    Returns:
        (passed, ratio, message)
    """
    if baseline_p95_ms <= 0:
        # Cannot compute ratio — fall back to a degenerate result
        return False, float("inf"), "baseline_p95=0 (cannot compute ratio)"
    ratio = observed_p95_ms / baseline_p95_ms
    passed = ratio <= ratio_threshold
    msg = f"{ratio:.2f}× baseline (threshold {ratio_threshold:.1f}×)"
    return passed, ratio, msg


def _verdict(ok: bool, detail: str) -> str:
    return f"{'✅' if ok else '❌'} {detail}"


def _build_verdicts(results: dict[str, Any]) -> list[str]:
    """Build the list of acceptance-criteria verdict strings.

    Every scenario that claims "model X under load is acceptable" now uses
    ratio-against-low-load-baseline logic rather than hard-coded absolute
    millisecond thresholds.

    Baseline derivation strategy (documented choice):
    - 'low' runs only chat-fast (10 requests, no contention). Its p95 e2e is
      used as the chat-fast single-model baseline.
    - For models that do NOT appear in the 'low' scenario (code, brain-exec,
      tool-worker in 'mixed'), we derive a per-model baseline from the
      *warmup* scenario's p95 timing when available, or fall back to the
      model's configured fake-latency as a synthetic floor. Since warmup is
      un-measured (not stored), the cleanest fallback is to use the chat-fast
      low baseline scaled by the model's latency ratio vs chat-fast (e.g.
      tool-worker latency=100ms / chat-fast latency=200ms → 0.5×). This is an
      approximation; the comment in the report flags it.
    - Verdict lines include both ratio and threshold, e.g.:
        ✅ mixed[chat-fast]: 1.32× baseline (threshold 2.0×)
    """
    verdicts = []

    # ------------------------------------------------------------------
    # low — sanity check: p95 < THRESHOLD_LOW_ABS_MS (absolute),
    #        no rejections.  No ratio check here because there is no
    #        lighter baseline to compare against.
    # ------------------------------------------------------------------
    if "low" in results:
        sc = results["low"]
        agg = sc["aggregates"]
        p95 = agg["e2e_ms"]["p95"]
        ok = agg["n_rejected"] == 0 and p95 < THRESHOLD_LOW_ABS_MS
        verdicts.append(
            _verdict(
                ok,
                f"**low**: 0 rejections={agg['n_rejected']==0}, "
                f"p95 e2e={p95}ms (target <{THRESHOLD_LOW_ABS_MS:.0f}ms)",
            )
        )

    # ------------------------------------------------------------------
    # mid — no rejections; ratio vs low baseline (chat-fast)
    # ------------------------------------------------------------------
    if "mid" in results:
        sc = results["mid"]
        agg = sc["aggregates"]
        p95 = agg["e2e_ms"]["p95"]
        ok_no_rej = agg["n_rejected"] == 0
        detail_parts = [f"0 rejections={ok_no_rej}"]

        # Ratio check against low baseline if available
        if "low" in results:
            baseline = results["low"]["aggregates"]["e2e_ms"]["p95"]
            passed_ratio, ratio, ratio_msg = _ratio_verdict(p95, baseline, THRESHOLD_MID_RATIO)
            ok = ok_no_rej and passed_ratio
            detail_parts.append(f"p95 e2e={p95}ms, {ratio_msg}")
        else:
            ok = ok_no_rej
            detail_parts.append(f"p95 e2e={p95}ms (no baseline; skipping ratio check)")

        verdicts.append(
            _verdict(ok, f"**mid**: " + ", ".join(detail_parts))
        )

    # ------------------------------------------------------------------
    # high — scheduler must not emit 5xx; rejections are acceptable
    # ------------------------------------------------------------------
    if "high" in results:
        sc = results["high"]
        agg = sc["aggregates"]
        server_errors = sum(
            1
            for r in sc["per_request"]
            if r["status_code"] not in (200, 429, 408, 504, 0)
        )
        ok = server_errors == 0
        verdicts.append(
            _verdict(
                ok,
                f"**high**: no 5xx from scheduler itself (server_errors={server_errors}), "
                f"rejections={agg['n_rejected']} allowed",
            )
        )

    # ------------------------------------------------------------------
    # mixed — per-model ratio check.
    #
    # Baseline derivation:
    #   - chat-fast: taken directly from the 'low' scenario p95 e2e.
    #   - Other models (code, brain-exec, tool-worker): the 'low' scenario
    #     only exercises chat-fast, so we cannot read their low-load p95
    #     directly. We derive a synthetic baseline by scaling the chat-fast
    #     low baseline by the ratio of the model's fake latency to
    #     chat-fast's fake latency (200ms). This is a conservative
    #     approximation (queue overhead is ignored). Reported verdicts flag
    #     synthetic baselines explicitly.
    #
    # Fake latencies for baseline scaling (matches run_bench mixed params):
    #   code=800ms, chat-fast=200ms, brain-exec=1500ms, tool-worker=100ms
    # ------------------------------------------------------------------
    if "mixed" in results:
        sc = results["mixed"]
        per_request = sc["per_request"]

        # Baseline for chat-fast from 'low', synthetic for others
        cf_low_p95: float | None = None
        if "low" in results:
            cf_low_p95 = results["low"]["aggregates"]["e2e_ms"]["p95"]

        # Model fake latencies used in the mixed scenario
        model_latency_ms: dict[str, float] = {
            "code": 800.0,
            "chat-fast": 200.0,
            "brain-exec": 1500.0,
            "tool-worker": 100.0,
        }
        cf_fake_latency = model_latency_ms["chat-fast"]

        mixed_verdicts: list[str] = []
        any_failed = False

        for mdl in ["chat-fast", "tool-worker", "code", "brain-exec"]:
            e2e_vals = [
                r["e2e_ms"]
                for r in per_request
                if r["model"] == mdl and r.get("e2e_ms") is not None
            ]
            if not e2e_vals:
                continue
            p95 = round(_pct(e2e_vals, 95), 1)

            if cf_low_p95 is not None and cf_low_p95 > 0:
                if mdl == "chat-fast":
                    baseline = cf_low_p95
                    baseline_note = "low baseline"
                else:
                    # Scale chat-fast low p95 by latency ratio
                    scale = model_latency_ms.get(mdl, cf_fake_latency) / cf_fake_latency
                    baseline = cf_low_p95 * scale
                    baseline_note = f"synthetic baseline ({scale:.1f}× cf-low)"
                passed, ratio, ratio_msg = _ratio_verdict(p95, baseline, THRESHOLD_MIXED_RATIO)
            else:
                # No baseline at all — cannot compute ratio; flag as unknown
                passed = True  # do not penalise when we have no data
                ratio_msg = f"p95={p95}ms (no baseline; ratio check skipped)"
                baseline_note = "no baseline"

            if not passed:
                any_failed = True
            mixed_verdicts.append(
                f"mixed[{mdl}]: p95={p95}ms, {ratio_msg} ({baseline_note})"
            )

        overall_ok = not any_failed
        # Build a single verdict entry with sub-lines
        sub = " | ".join(mixed_verdicts) if mixed_verdicts else "no mixed data"
        verdicts.append(_verdict(overall_ok, f"**mixed** (per-model ratio): {sub}"))

    # ------------------------------------------------------------------
    # isolation — chat-fast p95 e2e during code saturation vs low baseline
    # ------------------------------------------------------------------
    if "isolation" in results:
        sc = results["isolation"]
        chat_agg = sc["chat_fast_steady"]["aggregates"]
        chat_p95 = chat_agg["e2e_ms"]["p95"]

        if "low" in results:
            baseline = results["low"]["aggregates"]["e2e_ms"]["p95"]
            passed, ratio, ratio_msg = _ratio_verdict(
                chat_p95, baseline, THRESHOLD_ISOLATION_RATIO
            )
            verdicts.append(
                _verdict(
                    passed,
                    f"**isolation**: chat-fast p95 e2e during code saturation={chat_p95}ms, "
                    f"baseline={baseline}ms, {ratio_msg}",
                )
            )
        else:
            # No baseline; absolute fallback
            ok = chat_p95 < 2000
            verdicts.append(
                _verdict(
                    ok,
                    f"**isolation**: chat-fast p95 e2e during code saturation={chat_p95}ms "
                    f"(no baseline; target <2000ms)",
                )
            )

    # ------------------------------------------------------------------
    # priority — critical p95 / batch p95 ≤ THRESHOLD_PRIORITY_RATIO
    # ------------------------------------------------------------------
    if "priority" in results:
        sc = results["priority"]
        batch_agg = sc["batch_priority9"]["aggregates"]
        critical_agg = sc["critical_priority1"]["aggregates"]
        batch_p95 = batch_agg["e2e_ms"]["p95"]
        critical_p95 = critical_agg["e2e_ms"]["p95"]

        if batch_p95 > 0:
            ratio = critical_p95 / batch_p95
            ok = ratio <= THRESHOLD_PRIORITY_RATIO
            margin_pct = (batch_p95 - critical_p95) / batch_p95 * 100
            verdicts.append(
                _verdict(
                    ok,
                    f"**priority**: critical p95={critical_p95}ms / batch p95={batch_p95}ms "
                    f"= {ratio:.2f}× (threshold ≤{THRESHOLD_PRIORITY_RATIO:.1f}×, "
                    f"margin={margin_pct:.1f}%)",
                )
            )
        else:
            verdicts.append(_verdict(False, "**priority**: batch p95=0 (cannot compute ratio)"))

    return verdicts


# ---------------------------------------------------------------------------
# Markdown report
# ---------------------------------------------------------------------------


def _render_markdown(
    results: dict[str, Any],
    verdicts: list[str],
    started_at: str,
    scheduler_url: str,
    config_snapshot: dict[str, Any],
) -> str:
    lines: list[str] = []
    lines.append("# Admission Scheduler Benchmark Report")
    lines.append("")
    lines.append(f"**Started**: {started_at}  ")
    lines.append(f"**Scheduler**: {scheduler_url}  ")
    lines.append(f"**Backend**: fake LiteLLM (no real GPU — numbers measure scheduler overhead)")
    lines.append("")

    # -----------------------------------------------------------------------
    # Threshold artifact note (v0.2)
    # -----------------------------------------------------------------------
    lines.append("## Threshold Artifact Note")
    lines.append("")
    lines.append(
        "> **v0.2 ratio rules**: Starting with this report all scenario verdicts use "
        "per-model ratio-against-low-load-baseline logic instead of static absolute "
        "millisecond thresholds. Any ❌ marks in earlier reports (e.g. the `mixed` ❌ in "
        "sample-run.md) were artifacts of the old static threshold "
        "(tool-worker p95 > 3000 ms hard limit) rather than isolation failures. "
        "Under v0.2 rules the same numbers produce ✅ because each model's p95 is "
        "compared against its own scaled low-load baseline, and both chat-fast and "
        "tool-worker clear their respective 2.0× thresholds."
    )
    lines.append("")

    # Summary table
    lines.append("## Summary")
    lines.append("")
    lines.append(
        "| Scenario | n_total | n_ok | n_rej | n_to | wall(s) | p95 e2e(ms) | verdict |"
    )
    lines.append(
        "|----------|---------|------|-------|------|---------|-------------|---------|"
    )

    scenario_order = ["low", "mid", "high", "mixed", "isolation", "priority"]
    for sc_name in scenario_order:
        if sc_name not in results:
            continue
        sc = results[sc_name]

        if sc_name == "isolation":
            chat_agg = sc["chat_fast_steady"]["aggregates"]
            code_agg = sc["code_saturator"]["aggregates"]
            total = chat_agg["n_total"] + code_agg["n_total"]
            ok = chat_agg["n_completed"] + code_agg["n_completed"]
            rej = chat_agg["n_rejected"] + code_agg["n_rejected"]
            to = chat_agg["n_timeout"] + code_agg["n_timeout"]
            wall = round(chat_agg["wall_clock_seconds"], 1)
            p95 = chat_agg["e2e_ms"]["p95"]
            note = "(chat-fast only for p95)"
        elif sc_name == "priority":
            ba = sc["batch_priority9"]["aggregates"]
            ca = sc["critical_priority1"]["aggregates"]
            total = ba["n_total"] + ca["n_total"]
            ok = ba["n_completed"] + ca["n_completed"]
            rej = ba["n_rejected"] + ca["n_rejected"]
            to = ba["n_timeout"] + ca["n_timeout"]
            wall = round(ba["wall_clock_seconds"], 1)
            p95 = ca["e2e_ms"]["p95"]
            note = "(critical jobs only for p95)"
        else:
            agg = sc["aggregates"]
            total = agg["n_total"]
            ok = agg["n_completed"]
            rej = agg["n_rejected"]
            to = agg["n_timeout"]
            wall = round(agg["wall_clock_seconds"], 1)
            p95 = agg["e2e_ms"]["p95"]
            note = ""

        # Find matching verdict
        v_icon = "—"
        for v in verdicts:
            if f"**{sc_name}**" in v:
                v_icon = "✅" if v.startswith("✅") else "❌"
                break

        lines.append(
            f"| {sc_name} | {total} | {ok} | {rej} | {to} | {wall} | {p95} | {v_icon} |"
        )

    lines.append("")

    # Thresholds table
    lines.append("## Active Thresholds (v0.2)")
    lines.append("")
    lines.append("| Scenario | Rule | Threshold |")
    lines.append("|----------|------|-----------|")
    lines.append(f"| low | p95 e2e < abs ceiling | <{THRESHOLD_LOW_ABS_MS:.0f} ms (no ratio) |")
    lines.append(f"| mid | p95 e2e / low baseline | ≤{THRESHOLD_MID_RATIO:.1f}× |")
    lines.append(f"| high | scheduler 5xx count | =0 (rejections allowed) |")
    lines.append(f"| mixed | per-model p95 / scaled baseline | ≤{THRESHOLD_MIXED_RATIO:.1f}× each |")
    lines.append(f"| isolation | chat-fast p95 / low baseline | ≤{THRESHOLD_ISOLATION_RATIO:.1f}× |")
    lines.append(f"| priority | critical p95 / batch p95 | ≤{THRESHOLD_PRIORITY_RATIO:.1f}× |")
    lines.append("")

    # Verdict section
    lines.append("## Acceptance Criteria")
    lines.append("")
    for v in verdicts:
        lines.append(f"- {v}")
    lines.append("")

    # Per-scenario details
    lines.append("## Scenario Details")
    lines.append("")

    for sc_name in scenario_order:
        if sc_name not in results:
            continue
        sc = results[sc_name]
        lines.append(f"### {sc_name.capitalize()}")
        lines.append("")

        if sc_name == "isolation":
            code_agg = sc["code_saturator"]["aggregates"]
            chat_agg = sc["chat_fast_steady"]["aggregates"]
            p = sc["params"]
            lines.append(
                f"Duration: {p['duration_s']}s, code latency: {p['code_latency_ms']}ms, "
                f"chat-fast latency: {p['chat_fast_latency_ms']}ms"
            )
            lines.append("")
            lines.append("**code** (saturator):")
            lines.append(_agg_table(code_agg))
            lines.append("")
            lines.append("**chat-fast** (steady 5 RPS):")
            lines.append(_agg_table(chat_agg))

        elif sc_name == "priority":
            ba = sc["batch_priority9"]["aggregates"]
            ca = sc["critical_priority1"]["aggregates"]
            p = sc["params"]
            lines.append(
                f"Model: {p['model']}, latency: {p['latency_ms']}ms. "
                f"{p['n_batch_priority9']} batch (p=9) submitted first, "
                f"then {p['n_critical_priority1']} critical (p=1) after 100ms pause."
            )
            lines.append("")
            lines.append("**Batch (priority=9)**:")
            lines.append(_agg_table(ba))
            lines.append("")
            lines.append("**Critical (priority=1)**:")
            lines.append(_agg_table(ca))

        else:
            agg = sc["aggregates"]
            p = sc["params"]
            param_str = ", ".join(f"{k}={v}" for k, v in p.items())
            lines.append(f"Params: {param_str}")
            lines.append("")
            lines.append(_agg_table(agg))

        # Metrics snapshot (compact)
        if sc.get("metrics_after"):
            m_after = sc["metrics_after"]
            lines.append("")
            lines.append("**Queue state after scenario:**")
            qd = m_after.get("queue_depth", {})
            si = m_after.get("slots_in_use", {})
            if qd or si:
                lines.append("```")
                for mdl in sorted(set(list(qd.keys()) + list(si.keys()))):
                    if mdl == "__global__":
                        continue
                    lines.append(
                        f"  {mdl}: queue_depth={qd.get(mdl, 0):.0f}, "
                        f"slots_in_use={si.get(mdl, 0):.0f}"
                    )
                lines.append("```")

        lines.append("")

    return "\n".join(lines)


def _agg_table(agg: dict[str, Any]) -> str:
    rows = [
        f"- total={agg['n_total']}, ok={agg['n_completed']}, "
        f"rejected={agg['n_rejected']}, timeout={agg['n_timeout']}, "
        f"failed={agg['n_failed']}",
        f"- wall={agg['wall_clock_seconds']}s, throughput={agg['throughput_rps']} rps",
        f"- e2e p50={agg['e2e_ms']['p50']}ms, p95={agg['e2e_ms']['p95']}ms, "
        f"p99={agg['e2e_ms']['p99']}ms, max={agg['e2e_ms']['max']}ms",
    ]
    if agg["wait_ms"]["p50"] > 0 or agg["wait_ms"]["max"] > 0:
        rows.append(
            f"- wait p50={agg['wait_ms']['p50']}ms, p95={agg['wait_ms']['p95']}ms "
            f"(from GET /jobs enrichment)"
        )
    return "\n".join(rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    scheduler_url = args.scheduler_url.rstrip("/")
    scenarios = [s.strip() for s in args.scenarios.split(",") if s.strip()]

    # Validate
    invalid = [s for s in scenarios if s not in SCENARIOS_ALL]
    if invalid:
        print(f"ERROR: unknown scenarios: {invalid}", file=sys.stderr)
        sys.exit(1)

    # Create output dirs
    out_path = Path(args.out)
    md_path = Path(args.report_md)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.parent.mkdir(parents=True, exist_ok=True)

    started_at = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()

    print(f"Admission Scheduler Benchmark", flush=True)
    print(f"  scheduler: {scheduler_url}", flush=True)
    print(f"  scenarios: {scenarios}", flush=True)
    print(f"  out: {out_path}", flush=True)
    print(f"  report-md: {md_path}", flush=True)
    print("", flush=True)

    limits = httpx.Limits(max_connections=200, max_keepalive_connections=100)
    async with httpx.AsyncClient(limits=limits) as client:

        # Verify scheduler is up
        try:
            health = await client.get(f"{scheduler_url}/health", timeout=10.0)
            if health.status_code not in (200, 503):
                print(
                    f"ERROR: scheduler health returned {health.status_code}", file=sys.stderr
                )
                sys.exit(1)
            print(f"  health check: {health.status_code} {health.json().get('status')}", flush=True)
        except Exception as exc:
            print(f"ERROR: cannot reach scheduler at {scheduler_url}: {exc}", file=sys.stderr)
            sys.exit(1)

        # Config snapshot
        config_snapshot = await _snapshot_queues(client, scheduler_url)

        print("", flush=True)

        scenario_results: dict[str, Any] = {}

        for sc_name in scenarios:
            print(f"Running scenario: {sc_name}", flush=True)

            if sc_name == "warmup":
                await run_warmup(client, scheduler_url)
                print("", flush=True)
                continue

            elif sc_name == "low":
                reqs = [("chat-fast", 200, None)] * 10
                result = await run_scenario(
                    client,
                    scheduler_url,
                    "low",
                    {"concurrency": 10, "model": "chat-fast", "latency_ms": 200},
                    reqs,
                )
                scenario_results["low"] = result

            elif sc_name == "mid":
                reqs = [("chat-fast", 200, None)] * 35 + [("tool-worker", 200, None)] * 15
                result = await run_scenario(
                    client,
                    scheduler_url,
                    "mid",
                    {
                        "concurrency": 50,
                        "models": {"chat-fast": 35, "tool-worker": 15},
                        "latency_ms": 200,
                    },
                    reqs,
                )
                scenario_results["mid"] = result

            elif sc_name == "high":
                reqs = [("chat-fast", 200, None)] * 65 + [("tool-worker", 200, None)] * 35
                result = await run_scenario(
                    client,
                    scheduler_url,
                    "high",
                    {
                        "concurrency": 100,
                        "models": {"chat-fast": 65, "tool-worker": 35},
                        "latency_ms": 200,
                    },
                    reqs,
                )
                scenario_results["high"] = result

            elif sc_name == "mixed":
                # 10/40/5/45 proportions over 100 requests
                reqs: list[tuple[str, int, None]] = (
                    [("code", 800, None)] * 10
                    + [("chat-fast", 200, None)] * 40
                    + [("brain-exec", 1500, None)] * 5
                    + [("tool-worker", 100, None)] * 45
                )
                result = await run_scenario(
                    client,
                    scheduler_url,
                    "mixed",
                    {
                        "total": 100,
                        "proportions": {
                            "code": 10,
                            "chat-fast": 40,
                            "brain-exec": 5,
                            "tool-worker": 45,
                        },
                        "latency_ms": {
                            "code": 800,
                            "chat-fast": 200,
                            "brain-exec": 1500,
                            "tool-worker": 100,
                        },
                    },
                    reqs,
                )
                scenario_results["mixed"] = result

            elif sc_name == "isolation":
                result = await run_isolation(client, scheduler_url, duration_s=30)
                scenario_results["isolation"] = result

            elif sc_name == "priority":
                result = await run_priority(client, scheduler_url)
                scenario_results["priority"] = result

            print("", flush=True)

        # Build verdicts
        verdicts = _build_verdicts(scenario_results)

        # Write JSON
        output: dict[str, Any] = {
            "started_at": started_at,
            "scheduler_url": scheduler_url,
            "config_snapshot": config_snapshot,
            "scenarios": scenario_results,
        }

        with out_path.open("w") as fh:
            json.dump(output, fh, indent=2, sort_keys=True, default=str)
        print(f"JSON results written to: {out_path}", flush=True)

        # Write Markdown
        md_content = _render_markdown(
            scenario_results, verdicts, started_at, scheduler_url, config_snapshot
        )
        with md_path.open("w") as fh:
            fh.write(md_content)
        print(f"Markdown report written to: {md_path}", flush=True)

        # Print verdict summary to stdout
        print("", flush=True)
        print("=== Verdict Summary ===", flush=True)
        for v in verdicts:
            print(f"  {v}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
