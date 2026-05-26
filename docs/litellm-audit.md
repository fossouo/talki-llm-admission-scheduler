# LiteLLM Feature Audit — Admission Scheduler Side-Car

**Date:** 2026-05-26
**Scope:** LiteLLM proxy v1.77–v1.82 (OSS). Target deployment: `xeon:4000` in front of vLLM + llama.cpp GPU backends.
**Purpose:** Determine what LiteLLM already provides so the admission scheduler side-car builds only the missing layer.

**See also**: [README.md](../README.md) · [docs/architecture.md](architecture.md) · [docs/integration-plan.md](integration-plan.md) · [deploy/xeon-runbook.md](../deploy/xeon-runbook.md)

---

## 1. Request Scheduler / Priority Queue

**Does LiteLLM have it?** Yes — **[BETA]**, explicitly marked "Use for testing only."

The feature lives at `/docs/scheduler` and is called **Request Prioritization**. It works by:

1. Caller adds `priority=<int>` to the request (lower integer = higher priority, e.g. `priority=0` beats `priority=2000`).
2. Requests are pushed onto a Redis-backed priority queue.
3. A polling loop (`polling_interval`, default ~3 ms) checks whether a healthy deployment is available or whether the request has reached the front of the queue.

**Configuration (SDK/Router level, not proxy config.yaml):**

```python
router = Router(
    model_list=model_list,
    routing_strategy="simple-shuffle",
    redis_host=..., redis_port=..., redis_password=...,
)
scheduler_config = SchedulerConfig(polling_interval=0.03, timeout=30)
response = await router.schedule_acompletion(messages=..., priority=0, scheduler_config=scheduler_config)
```

**Supported endpoints only:** `acompletion` (→ `/v1/chat/completions`) and `atext_completion` (→ `/v1/completions`). Embeddings and other endpoints are not supported.

**Redis:** Required for multi-instance. In single-process deployment, in-memory queue is used but is not surfaced at the HTTP layer.

**Critical gap — polling bug ([Issue #6867](https://github.com/BerriAI/litellm/issues/6867), closed as "not planned"):** The current polling logic immediately removes requests from the queue if *any* healthy deployment exists, regardless of their priority order. This means "if there is a healthy deployment, no request ever waits for the `polling_interval`." The queue never actually fills during normal load, making priority ordering illusory when there is capacity. The bug was closed without a fix.

**v1.77.3 addition — Priority Quota Reservation:** A separate, orthogonal feature. Proxy admins can reserve percentages of per-model TPM/RPM capacity for API keys tagged with priority levels (e.g. `"prod": 0.9, "dev": 0.1`). This is a static capacity split, not a dynamic queue.

---

## 2. Rate Limiting (TPM, RPM, Max Parallel Requests)

LiteLLM enforces at four scopes: per-deployment (`rpm`, `tpm` in `litellm_params`), per-key, per-user, per-team.

**Behavior when exceeded:** Hard 429 response, no queuing. Caller must handle backoff.

**`max_parallel_requests` known issues:**
- [#16011](https://github.com/BerriAI/litellm/issues/16011): With 200 concurrent connections and `max_parallel_requests=5`, backend received 112–200 simultaneous requests. Enforcement inconsistent under burst.
- [#17323](https://github.com/BerriAI/litellm/issues/17323): On `/v1/messages` with Redis cache, permanently rate-limits keys until flush.
- [#17824](https://github.com/BerriAI/litellm/issues/17824): Feature request open for proxy-level `max_parallel_requests`.

---

## 3. Queueing and Backpressure

**Does LiteLLM queue when saturated?** **No** — not for LLM requests. Hard 429 only. The only internal queue is for DB transaction logging (PR #9759). The BETA scheduler exists but is bypassed when any healthy deployment is available (the polling bug, §1), and is SDK-only.

**Conclusion:** There is no proxy-level queue that a caller hitting `POST /v1/chat/completions` would benefit from.

---

## 4. Redis — When Required

| Feature | Redis Required? |
|---|---|
| Rate limits (single instance) | No |
| Rate limits (multi-instance) | Yes |
| Scheduler / priority queue | Yes |
| Usage-based routing | Yes |
| Least-busy routing | Optional |
| Circuit breaker for Redis itself | Built-in (v1.82+) |

---

## 5. Fallbacks

Three production-ready types: `fallbacks`, `context_window_fallbacks`, `content_policy_fallbacks`. Plus `enable_pre_call_checks`, runtime management endpoints (`/fallback/new`, etc.), and per-request `disable_fallbacks: true`.

---

## 6. Routing Strategies

Five built-in: `simple-shuffle` (recommended for prod), `least-busy`, `usage-based-routing` (not recommended for prod), `latency-based-routing`, `cost-based-routing`. Plus custom plugin via `CustomRoutingStrategyBase`.

---

## 7. Async / Submit + Poll Patterns

**Does LiteLLM expose async submit-poll at HTTP layer?** **No.** Only sync OpenAI-compatible endpoints.

- `acompletion`/`abatch_completion` are client-side SDK patterns.
- Managed Batches BETA = pass-through to OpenAI Batch API (not local backends).

---

## 8. Request Timeouts

Three levels: `router_settings.timeout` (global), per-deployment `timeout`/`stream_timeout`, per-request override via body.

---

## 9. Cooldowns & Circuit Breakers

`allowed_fails`, `cooldown_time`, automatic 429 → instant cooldown, `order=` fallback promotion, `GET /health/deployments`. Redis circuit breaker added v1.82.0 (fast-fail after 5 consecutive Redis failures).

---

## 10. Model Field Pass-Through (BUG)

[Issue #22709](https://github.com/BerriAI/litellm/issues/22709) (open, March 2026): response `model` field flip-flopped after PR #19943 — now returns client-supplied alias unconditionally, even when observability needs the backend name. **Side-car implication:** key all accounting on the alias the side-car SENDS, not the alias parsed from any response body.

---

## 11. Logging and Callbacks

`StandardLoggingPayload` with `log_pre_api_call`, `log_post_api_call`, `async_log_success_event`, `async_log_failure_event` hooks. Built-in destinations: Langfuse, OTel, Datadog, S3, GCS, Azure Blob, SQS, DynamoDB, ~15 more.

**Missing:** No built-in "write JSONL to local file" callback. Requires custom Python callback module.

---

## 12. Prometheus Metrics

Endpoint `GET /metrics`. Exposes: `litellm_in_flight_requests`, `litellm_request_total_latency_metric`, `litellm_llm_api_latency_metric`, TTFT, overhead latency, total/failed requests, remaining rate-limit, spend, deployment state, Redis health, DB queue.

**Gaps for GPU admission control:**
- No queue depth per model alias.
- No queue wait latency.
- No GPU backend concurrency slots-occupied vs capacity.
- `litellm_in_flight_requests` is global, not per-model.

---

## 13. Gap Analysis

| Capability | LiteLLM has? | Sufficient? | Side-car must add? |
|---|---|---|---|
| Priority queue for LLM | BETA broken, SDK-only | No | **Yes** — HTTP-layer FIFO/priority |
| Max concurrency per alias | Partial, buggy enforcement | No | **Yes** — atomic slot counter |
| Request queuing on saturation | No — 429 only | No | **Yes** — bounded queue |
| Max queue depth + overflow 429 | No | No | **Yes** |
| Per-entry wait TTL | No | No | **Yes** |
| Sync + async submit modes | Sync only | No | **Yes** |
| JSONL lifecycle log | No native | No | **Yes** |
| Queue depth / wait metrics | No | No | **Yes** |
| Rate limits (TPM/RPM) | Yes | Yes | No |
| Fallbacks | Yes | Yes | No |
| Cooldowns | Yes | Yes | No |
| Routing strategies | Yes | Yes | No |
| Request timeouts | Yes | Yes | No |
| Prometheus latency/throughput | Yes | Yes | No (extend only) |
| Redis circuit breaker | Yes (v1.82+) | Yes | No |
| Priority capacity reservation | Yes (v1.77.3) | Partial | No (configure in LiteLLM) |

---

## 14. What the Side-Car Must Own — 5 Bullets

1. **Per-model admission gate with atomic concurrency slots** (Redis `INCR`/`DECR`) since LiteLLM's `max_parallel_requests` has documented enforcement failures.
2. **Bounded FIFO/priority queue with configurable depth and wait timeout** since LiteLLM has no HTTP-layer queue.
3. **JSONL lifecycle event log** (queued / dispatched / completed / failed / timeout / rejected) since LiteLLM has no native file callback.
4. **Queue-aware Prometheus metrics**: `sidecar_queue_depth{model}`, `sidecar_slots_in_use{model}`, `sidecar_slots_capacity{model}`, `sidecar_queue_wait_seconds{model}`, `sidecar_rejected_total{model,reason}`.
5. **Deterministic alias keying** (use the alias the side-car sends, never the response.model field, due to Issue #22709).

---

## References

- [LiteLLM Request Prioritization (BETA)](https://docs.litellm.ai/docs/scheduler)
- [LiteLLM Router Load Balancing](https://docs.litellm.ai/docs/routing)
- [LiteLLM Proxy Load Balancing](https://docs.litellm.ai/docs/proxy/load_balancing)
- [LiteLLM Fallbacks / Reliability](https://docs.litellm.ai/docs/proxy/reliability)
- [LiteLLM Timeouts](https://docs.litellm.ai/docs/proxy/timeout)
- [LiteLLM Prometheus Metrics](https://docs.litellm.ai/docs/proxy/prometheus)
- [LiteLLM Logging Callbacks](https://docs.litellm.ai/docs/proxy/logging)
- [Redis Circuit Breaker Blog](https://docs.litellm.ai/blog/redis-circuit-breaker)
- [Issue #6867 — Scheduler polling bug (closed, not planned)](https://github.com/BerriAI/litellm/issues/6867)
- [Issue #16011 — max_parallel_requests inconsistent (open)](https://github.com/BerriAI/litellm/issues/16011)
- [Issue #17323 — max_parallel_requests permanent limit](https://github.com/BerriAI/litellm/issues/17323)
- [Issue #17824 — Proxy-level max_parallel_requests](https://github.com/BerriAI/litellm/issues/17824)
- [Issue #22709 — model field returns alias not deployment](https://github.com/BerriAI/litellm/issues/22709)
- [PR #9759 — DB transaction queue (v1.65.4)](https://github.com/BerriAI/litellm/pull/9759)
