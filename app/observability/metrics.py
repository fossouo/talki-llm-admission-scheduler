"""Prometheus metrics declarations for the admission scheduler.

All metrics are instantiated at module level so they are available
as singletons. Convenience wrappers record the most common observations.
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

# Histogram buckets aligned with architecture §11
_DURATION_BUCKETS = [0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600]

# ---------------------------------------------------------------------------
# Counters
# ---------------------------------------------------------------------------

admission_requests_total = Counter(
    "admission_requests_total",
    "Total admission requests by outcome.",
    ["model", "result"],
)

failure_total = Counter(
    "failure_total",
    "Total failed jobs by error class.",
    ["model", "error_class"],
)

timeout_total = Counter(
    "timeout_total",
    "Total timed-out requests by stage (queue or sync).",
    ["model", "stage"],
)

cancel_total = Counter(
    "cancel_total",
    "Total cancelled jobs.",
    ["model"],
)

worker_crash_total = Counter(
    "worker_crash_total",
    "Total worker crash events detected by lease watcher.",
    ["model"],
)

log_write_errors_total = Counter(
    "log_write_errors_total",
    "Total JSONL log write errors (disk full, permissions, etc.).",
    [],
)

redis_errors_total = Counter(
    "redis_errors_total",
    "Total Redis operation errors.",
    ["operation"],
)

# ---------------------------------------------------------------------------
# Gauges
# ---------------------------------------------------------------------------

queue_depth = Gauge(
    "queue_depth",
    "Current number of jobs waiting in the priority queue.",
    ["model"],
)

slots_in_use = Gauge(
    "slots_in_use",
    "Current number of concurrency slots occupied.",
    ["model"],
)

slots_capacity = Gauge(
    "slots_capacity",
    "Configured maximum concurrency slots.",
    ["model"],
)

dlq_depth = Gauge(
    "dlq_depth",
    "Number of jobs in the dead-letter queue.",
    ["model"],
)

# ---------------------------------------------------------------------------
# Histograms
# ---------------------------------------------------------------------------

queue_wait_seconds = Histogram(
    "queue_wait_seconds",
    "Time spent waiting in queue before dispatch (seconds).",
    ["model"],
    buckets=_DURATION_BUCKETS,
)

request_duration_seconds = Histogram(
    "request_duration_seconds",
    "Total end-to-end request duration from admission to terminal state (seconds).",
    ["model"],
    buckets=_DURATION_BUCKETS,
)

backend_duration_seconds = Histogram(
    "backend_duration_seconds",
    "Time spent waiting for LiteLLM backend response (seconds).",
    ["model"],
    buckets=_DURATION_BUCKETS,
)

# ---------------------------------------------------------------------------
# Convenience helpers
# ---------------------------------------------------------------------------


def record_admission(model: str, result: str) -> None:
    """Increment admission_requests_total for the given model and result."""
    admission_requests_total.labels(model=model, result=result).inc()


def record_completion(model: str, wait_s: float, runtime_s: float, backend_s: float) -> None:
    """Observe wait and duration histograms on successful completion."""
    queue_wait_seconds.labels(model=model).observe(wait_s)
    request_duration_seconds.labels(model=model).observe(wait_s + runtime_s)
    backend_duration_seconds.labels(model=model).observe(backend_s)


def record_failure(model: str, error_class: str) -> None:
    """Increment failure_total for the given model and error class."""
    failure_total.labels(model=model, error_class=error_class).inc()


def record_timeout(model: str, stage: str) -> None:
    """Increment timeout_total for the given model and stage."""
    timeout_total.labels(model=model, stage=stage).inc()


def record_cancel(model: str) -> None:
    """Increment cancel_total for the given model."""
    cancel_total.labels(model=model).inc()


def record_worker_crash(model: str) -> None:
    """Increment worker_crash_total for the given model."""
    worker_crash_total.labels(model=model).inc()


def record_redis_error(operation: str) -> None:
    """Increment redis_errors_total for the given operation label."""
    redis_errors_total.labels(operation=operation).inc()


def update_slot_gauges(model: str, in_use: int, capacity: int) -> None:
    """Update slots_in_use and slots_capacity gauges."""
    slots_in_use.labels(model=model).set(in_use)
    slots_capacity.labels(model=model).set(capacity)


def update_queue_depth(model: str, depth: int) -> None:
    """Update queue_depth gauge for the given model."""
    queue_depth.labels(model=model).set(depth)


def update_dlq_depth(model: str, depth: int) -> None:
    """Update dlq_depth gauge for the given model."""
    dlq_depth.labels(model=model).set(depth)
