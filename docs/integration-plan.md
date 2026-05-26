# Integration Plan — LLM Admission Scheduler → Talki LiteLLM Stack

> **This document is design-only. No integration is being executed yet.**
> The phased rollout described below is a plan for when the operator decides
> to proceed. Nothing in this document modifies any running service.
> The operator decides when Phase 0 starts.

**Date**: 2026-05-26
**Related docs**: [README.md](../README.md) · [docs/architecture.md](architecture.md) · [deploy/xeon-runbook.md](../deploy/xeon-runbook.md) · [docs/litellm-audit.md](litellm-audit.md)

---

## Current state

### Where LiteLLM lives

LiteLLM proxy runs on `xeon.tail9beaa9.ts.net:4000`. It is the single upstream LLM gateway for the entire Talki agent fleet. All callers send `POST /v1/chat/completions` directly to `http://xeon:4000`.

### Named callers (from `.claude/CLAUDE.md` and memory)

| Caller | Cadence | Primary model aliases | Latency tolerance |
|--------|---------|----------------------|-------------------|
| `dev-agent` | 15-min ticks | `code`, `chat-fast` | Low-medium (timed tasks) |
| `brain-pr-merger` | 15-min ticks | `brain-exec`, `chat-fast` | Medium |
| `signal-ia` weekly cycle | Once/week (Tue 18:00) | `chat-fast` | High (async-friendly) |
| `hermes-agent` | 30-min ticks | `chat-fast` | High |
| `Pilou newsletter` (n8n) | Weekly | `chat-fast` | High |
| `Academy chatbot` (Lambda) | On-demand user sessions | `chat-fast` | Low (real-time UX) |
| `codex-review-shim` | On-demand | `code` | Medium |
| `tool-worker` callers | Various | `tool-worker` | Low |

### Why direct integration is risky today

Three compounding reasons not to cut callers over before validating the scheduler:

1. **No baseline observability into per-model backend load.** We do not know typical queue depth, p95 wait, or slot saturation for each model alias in production. The `litellm_in_flight_requests` gauge is global. Without a baseline we cannot distinguish scheduler-induced regressions from pre-existing LiteLLM issues.

2. **The scheduler is a new SPOF.** LiteLLM is already running and trusted. Any bug in the scheduler (slot leak, worker deadlock, Redis key collision) would surface as a timeout storm across all callers if we cut them all over at once.

3. **Prototype status.** The scheduler has not been exercised against real LiteLLM traffic. The integration tests in `tests/` use a mock forwarder. Real network and real backend response shapes may reveal edge cases in the dispatcher loop, the pub/sub sync wait, or the gzip response blob path.

---

## Phase 0 — Shadow mode (week 1)

**Goal**: deploy the scheduler on Xeon and validate it against real LiteLLM without routing any production caller through it.

**What to do**:

```bash
# On xeon — from the repo root
sudo mkdir -p /mnt/talki-logs/admission-scheduler
sudo chown 10001:10001 /mnt/talki-logs/admission-scheduler
docker compose up -d --build

# Verify both containers healthy
docker compose ps
curl -fsS http://localhost:4001/health | jq .
```

Run `scripts/smoke-test.sh` to validate the local round-trip, then drive synthetic traffic against the scheduler while it proxies to the real LiteLLM proxy:

```bash
# Synthetic load — 10 sequential sync requests to chat-fast
for i in $(seq 1 10); do
  curl -s -X POST http://xeon:4001/v1/chat/completions \
    -H "Content-Type: application/json" \
    -H "X-Caller: shadow-test" \
    -d '{"model":"chat-fast","messages":[{"role":"user","content":"shadow test '\'$i\''"]},"max_tokens":16}' \
    | jq -r '"\(.choices[0].message.content // .error)"'
done

# Check JSONL log
tail -20 /mnt/talki-logs/admission-scheduler/events-$(date -u +%Y-%m-%d).jsonl | jq .

# Check Prometheus metrics
curl -fsS http://xeon:4001/metrics | grep -E '^(admission|queue|slots|failure|request_duration)'
```

**Acceptance criteria for Phase 0 exit**:
- `GET /health` returns `{"status":"ok","redis":"ok","litellm":"ok"}` for 24 h with no manual intervention.
- 100 synthetic `chat-fast` requests complete with no scheduler-attributable errors (`failure_total{error_class="network_error"}` = 0).
- JSONL file is non-empty and every event parses as valid JSON (`jq . events-*.jsonl >/dev/null`).
- `slots_in_use` gauge never exceeds `slots_capacity` for any model during the synthetic load.
- No Redis key leaks: after synthetic load completes, `KEYS slots:*:in_use` values are all `0`.

**Observability checklist (Phase 0 JSONL validation)**:

The v0.2 schema adds two fields that are the basis for the future GPU-aware
roadmap. Verify their presence in the Phase 0 corpus:

```bash
# Check backend_target field appears in completed events
jq 'select(.event=="completed") | .backend_target' events-*.jsonl | head -10

# Expect: null for most events during Phase 0 (LiteLLM header not yet verified)
# Expect: "dgx:8004" or similar once x-litellm-deployment header is confirmed

# Check kv_pressure_hint field appears in dispatched events
jq 'select(.event=="dispatched") | .kv_pressure_hint' events-*.jsonl | head -5
# Expect: null for all Phase 0 events (v0.3 probe not yet wired)

# Confirm no events are missing the fields entirely (field must be present, even if null)
jq 'select(.event=="completed") | has("backend_target")' events-*.jsonl | sort | uniq -c
# Expect: all "true"
```

These fields are captured from day 1 so the Phase 0 shadow corpus is usable
as calibration data for future GPU-aware features without re-instrumentation.
`backend_target` will be non-null once the LiteLLM proxy on Xeon is verified
to emit `x-litellm-deployment` response headers (check with
`curl -sI http://xeon:4000/v1/chat/completions` after a real request).

**No production caller is reconfigured in this phase.** LiteLLM continues to serve all production traffic on `:4000`.

---

## Phase 1 — Canary one caller (week 2)

**Goal**: route one low-risk production caller through the scheduler and observe for 7 days.

**Chosen caller**: `signal-ia` weekly cycle.

Rationale:
- Fires once per week (Tuesday 18:00 via launchd).
- Latency-tolerant: the TTS pipeline that follows takes 2–3 h, so a few extra seconds of queue wait is irrelevant.
- Async-friendly: `signal-ia`'s `make-episode` script can trivially use the async path (`POST /jobs` + poll).
- Low blast radius: one misfired weekly request is recoverable (rerun script manually).

**Change required**: update the `LITELLM_URL` env var (or equivalent config variable) in the signal-ia cycle script from `http://xeon:4000` to `http://xeon:4001`. No code change needed — the scheduler exposes the same OpenAI-compatible API.

```bash
# In ~/Documents/GitHub/talki-infra/mac/podcast/signal-ia/agents/cycle.sh
# Find the line that sets LITELLM_URL (or BACKEND_URL / chat-fast endpoint)
# Change: http://xeon:4000  →  http://xeon:4001
```

**Monitor for 7 days**:

```bash
# JSONL: confirm signal-ia requests reach completed state
tail -F /mnt/talki-logs/admission-scheduler/events-$(date -u +%Y-%m-%d).jsonl \
  | jq 'select(.caller=="signal-ia")'

# Prometheus: watch queue_wait for chat-fast
curl -fsS http://xeon:4001/metrics | grep queue_wait_seconds
```

**Acceptance criteria for Phase 1 exit**:
- The weekly cycle fires and `event=completed` appears in the JSONL log with `caller=signal-ia`.
- `queue_wait_seconds{model="chat-fast"}` p50 < 1 s (no saturation from a single caller).
- No `failure_total{error_class=~"network_error|worker_crash"}` events attributable to the signal-ia session.
- `GET /health` remains `ok` throughout the 7-day observation window.

**Rollback**: revert `LITELLM_URL` in `cycle.sh` back to `http://xeon:4000`.

---

## Phase 2 — Promote dev-side callers (weeks 3–4)

**Goal**: route the two highest-saturation callers — `dev-agent` and `brain-pr-merger` — through the scheduler. These fire every 15 minutes and are responsible for most concurrent `code` and `brain-exec` requests.

**Why these callers show the biggest gain**: their 15-min cadence means they frequently overlap. Without admission control, multiple dev-agent instances can submit `code` requests simultaneously, causing the backend to receive more concurrent requests than LiteLLM's (unreliable) `max_parallel_requests` cap allows. The scheduler replaces that unreliable cap with atomic Lua slot enforcement.

**Change required**: in the dev-agent and brain-pr-merger configuration (launchd plist `EnvironmentVariables` or the agent's config file), update the LiteLLM URL from `:4000` to `:4001`.

```bash
# Example plist update (do NOT commit API keys — see Q-012):
# <key>LITELLM_URL</key>
# <string>http://xeon:4001</string>
```

**Observe for 14 days**:
- Watch `queue_depth{model="code"}` and `queue_depth{model="brain-exec"}` during business hours.
- Watch `failure_total{model="code",error_class="backend_429"}` — should drop vs. the baseline measured in Phase 0.
- Watch `queue_wait_seconds{model="code"}` p95 — target < 30 s.

**Acceptance criteria for Phase 2 exit**:
- `failure_total{error_class="backend_429"}` for `code` and `brain-exec` is lower than the baseline (measured via LiteLLM's own metrics during Phase 0).
- `queue_wait_seconds{model="code"}` p95 < 30 s over the 14-day window.
- `slots_in_use{model="code"}` never exceeds `slots_capacity{model="code"}` (drift = 0).
- No `worker_crash_total` events.
- `GET /health` remains `ok` throughout.

**Rollback**: revert the two plists to `:4000`, `launchctl kickstart` both agents.

---

## Phase 3 — All callers (week 5+)

**Goal**: cut the entire Talki agent fleet over to `:4001`. LiteLLM at `:4000` continues to run unchanged — the scheduler proxies to it; callers simply change the port they target.

**Callers to migrate**:
- `hermes-agent` (`chat-fast`, every 30 min)
- `Pilou newsletter` n8n workflow (`chat-fast`, weekly)
- `Academy chatbot` Lambda (`chat-fast`, on-demand — this is a real-user path, update Lambda env var `LITELLM_URL`)
- `codex-review-shim` (`code`, on-demand)
- Any remaining tool-worker callers

**Academy chatbot note**: this is the only caller with a real-time UX contract. The scheduler adds < 5 ms overhead on an unqueued request (the common case for a non-saturated `chat-fast` model). During saturation a user query may queue briefly. The `max_wait_seconds: 60` for `chat-fast` ensures a 408 is returned rather than an indefinite hang — callers should handle 408 and surface a friendly retry message.

**DNS alias option** (fleet-wide cut): rather than updating each caller individually, create a DNS alias `litellm.internal` (or update `/etc/hosts` on Xeon) that points to `xeon:4001`. Callers that already use `http://litellm.internal:4000` or `http://xeon:4000` can be cut over by changing the alias, not each script.

```bash
# Simple /etc/hosts approach on callers that resolve via the host file
# Add: <xeon-tailnet-ip>  litellm-sched.internal
# Update callers to use: http://litellm-sched.internal:4001
```

**Rollback path**: per-caller (revert env var) or fleet-wide (point DNS alias back to `xeon:4000`). The scheduler being stateless beyond Redis means any in-flight jobs survive a restart; already-dispatched jobs complete normally.

---

## Risks and mitigations

### SPOF: scheduler container crashes

**Risk**: if the scheduler container goes down, all callers that have been migrated fail with connection refused.

**Mitigations**:
- `restart: unless-stopped` in `docker-compose.yml` + Docker healthcheck restarts the container within ~30 s of a crash.
- The scheduler is stateless beyond Redis. All queued jobs survive a restart (Redis volume is persisted; dispatcher re-subscribes to its queue ZSETs on startup).
- For multi-instance resilience: the architecture is designed for N scheduler instances behind a load balancer (Redis coordinates slot counters and queue atomically). Implement this before routing production real-user traffic through it.
- Rollback path is always available: revert env vars to point directly at `:4000`.

### Latency tax

**Risk**: every request incurs additional round-trip overhead (admission check, Redis ops, pub/sub wait).

**Expected**: < 5 ms per request on the unqueued path (single Redis EVAL + publish). Measure during Phase 0 by comparing `backend_duration_seconds` against the wall-clock time seen by the same caller hitting `:4000` directly.

**If observed overhead > 10 ms**: profile the Redis round-trip time between the Xeon scheduler container and the Redis container (both on the same host, same Docker bridge). Sub-millisecond round-trips are expected.

### Misconfigured `max_concurrency`

**Risk**: a model is over-subscribed (too many slots → backend gets hammered) or starved (too few slots → queue grows unboundedly).

**Mitigation**: start with the values in `config/default.yaml` — chosen conservatively based on known backend specs (2 concurrent for `code` / DGX llama.cpp, 4 for `chat-fast` / Qwen3.6 MoE on DGX, 1 for `brain-exec`, 6 for `tool-worker` / Qwen3-8B vLLM). Iterate based on `queue_wait_seconds` p95 and `backend_duration_seconds` p95 observed in JSONL and Prometheus.

If `queue_wait_seconds{model="chat-fast"}` p95 creeps above 5 s during Phase 1, increment `max_concurrency` for `chat-fast` by 1 and watch backend latency. If `backend_duration_seconds` degrades in parallel, the model is compute-bound and the slot count was correct.

### Redis volume loss

**Risk**: Redis data volume is wiped (`docker compose down -v`) while jobs are in-flight.

**Mitigation**: never run `docker compose down -v` in production. The documented maintenance procedure is `docker compose restart` (preserves volume). If a volume wipe does occur, in-flight jobs are lost, slot counters reset to 0, and the scheduler returns to a clean state. Callers receive 502/504 for the in-flight requests — normal retry logic handles this.

---

## Observability checklist

### Prometheus scrape config

Add to `prometheus.yml` on the Prometheus instance (or wherever the Talki fleet scrapes metrics):

```yaml
scrape_configs:
  - job_name: 'admission-scheduler'
    scrape_interval: 15s
    scrape_timeout: 10s
    static_configs:
      - targets: ['xeon.tail9beaa9.ts.net:4001']
    metrics_path: '/metrics'
```

### Suggested Grafana panels

| Panel | Metric | Visualization |
|-------|--------|---------------|
| Queue depth per model | `queue_depth{model=~".*"}` | Time-series line, one series per model |
| Slots in use vs capacity | `slots_in_use / slots_capacity` | Heatmap or gauge per model |
| Queue wait p95 per model | `histogram_quantile(0.95, rate(queue_wait_seconds_bucket[5m]))` | Time-series line |
| Request failure rate | `rate(failure_total[5m])` by `error_class` | Stacked bar |
| Admission outcomes | `rate(admission_requests_total[5m])` by `result` | Stacked bar |
| Backend duration p95 | `histogram_quantile(0.95, rate(backend_duration_seconds_bucket[5m]))` | Line per model |
| Worker crash rate | `rate(worker_crash_total[1h])` | Stat / alert threshold |
| DLQ depth | `dlq_depth` | Gauge per model; alert if > 0 |

### Alerting recommendations

- Alert if `queue_depth{model="code"}` > 15 for > 5 min (queue filling faster than draining).
- Alert if `worker_crash_total` rate > 0 per hour.
- Alert if `GET /health` returns non-200 for > 60 s.
- Alert if `failure_total{error_class="backend_429"}` spikes (means LiteLLM is still rate-limiting despite the slot count — slots may need to be reduced).

---

## Acceptance criteria for full rollout

All of the following must hold for **14 consecutive days** before the integration is considered stable:

- Zero scheduler-attributable timeouts (`failure_total{error_class="network_error"}` from the scheduler's perspective = 0).
- `queue_wait_seconds{model="chat-fast"}` p95 < 2 s.
- `queue_wait_seconds{model="tool-worker"}` p95 < 2 s.
- `queue_wait_seconds{model="code"}` p95 < 30 s.
- `queue_wait_seconds{model="brain-exec"}` p95 < 30 s.
- No slot drift events (`redis_errors_total{operation="drift_correction"}` = 0).
- No `worker_crash_total` events.
- `GET /health` returns `{"status":"ok"}` with 99.9% availability over the 14-day window.

---

## Summary

| Phase | Duration | Callers migrated | Risk | Rollback effort |
|-------|----------|-----------------|------|----------------|
| 0 — Shadow mode | Week 1 | None (synthetic only) | Zero | n/a |
| 1 — Canary | Week 2 | `signal-ia` | Very low | 1 env var |
| 2 — Dev-side | Weeks 3–4 | `dev-agent`, `brain-pr-merger` | Low | 2 env vars |
| 3 — All callers | Week 5+ | Remaining fleet | Medium | Env vars or DNS alias |

The entire plan is reversible at any stage. LiteLLM at `:4000` continues to run throughout — the scheduler is an additive layer, not a replacement.
