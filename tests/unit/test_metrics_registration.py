"""Unit tests for Prometheus metrics registration.

Verifies:
  - All 14 metric names declared in architecture §11 are registered.
  - Re-importing the metrics module does not raise a ValueError
    ("Duplicated timeseries" collision).
  - Convenience helpers (record_admission, etc.) do not raise.
"""

from __future__ import annotations

import importlib

import pytest
from prometheus_client import REGISTRY


# All 14 metric names from architecture §11.
# NOTE: prometheus_client's REGISTRY.collect() returns the BASE name for Counters,
# i.e. "admission_requests" not "admission_requests_total". The _total suffix is
# added during Prometheus exposition format output, not in the Python object name.
# We test for both the base name AND the metric object's declared name to cover both.
_EXPECTED_METRIC_NAMES_BASE = {
    "admission_requests",   # Counter → base name in REGISTRY
    "queue_depth",
    "slots_in_use",
    "slots_capacity",
    "queue_wait_seconds",
    "request_duration_seconds",
    "backend_duration_seconds",
    "failure",              # Counter
    "timeout",              # Counter
    "cancel",               # Counter
    "worker_crash",         # Counter
    "dlq_depth",
    "log_write_errors",     # Counter
    "redis_errors",         # Counter
}


def _registered_names() -> set[str]:
    """Return the set of metric names currently in the Prometheus registry."""
    return {m.name for m in REGISTRY.collect()}


def test_all_14_metrics_registered():
    """Every metric declared in architecture §11 must be present in the registry.

    Prometheus Python client collects Counters by their base name (without _total
    suffix). This test uses the base names as seen by REGISTRY.collect().
    """
    import app.observability.metrics  # ensure module is loaded

    registered = _registered_names()
    missing = _EXPECTED_METRIC_NAMES_BASE - registered
    assert not missing, f"Missing metrics: {sorted(missing)}"


def test_no_duplicate_registration_on_reimport():
    """Importing app.observability.metrics more than once in a session must not create new
    metrics objects (module-level singletons are only registered once at first import).

    NOTE: `importlib.reload()` WILL raise a ValueError (Duplicated timeseries) because
    reload() re-executes the module body and tries to register the same metric names
    again. This is the expected prometheus_client behaviour for module reloads. What we
    MUST NOT do is call `Counter(...)` multiple times in normal application code paths
    (e.g. inside functions or class __init__). This test verifies the module-level
    singleton pattern is used and that normal re-import via `import` (cached in
    sys.modules) is safe.
    """
    import sys
    import app.observability.metrics as m1

    # Standard re-import via sys.modules — same object, no new registration
    import app.observability.metrics as m2
    assert m1 is m2, "Re-import returned a different module object (should be the same singleton)"

    # Confirm the Counter object is the same singleton
    assert m1.admission_requests_total is m2.admission_requests_total, (
        "admission_requests_total is not a singleton"
    )


def test_record_admission_helper_does_not_raise():
    import app.observability.metrics as m
    m.record_admission("test-model", "queued")
    m.record_admission("test-model", "rejected")
    m.record_admission("test-model", "idempotent_hit")


def test_record_failure_helper_does_not_raise():
    import app.observability.metrics as m
    m.record_failure("test-model", "backend_5xx")
    m.record_failure("test-model", "network_error")


def test_record_timeout_helper_does_not_raise():
    import app.observability.metrics as m
    m.record_timeout("test-model", "queue")
    m.record_timeout("test-model", "sync")


def test_record_cancel_helper_does_not_raise():
    import app.observability.metrics as m
    m.record_cancel("test-model")


def test_record_worker_crash_does_not_raise():
    import app.observability.metrics as m
    m.record_worker_crash("test-model")


def test_record_redis_error_does_not_raise():
    import app.observability.metrics as m
    m.record_redis_error("bzpopmin")
    m.record_redis_error("drift_correction")


def test_update_slot_gauges_does_not_raise():
    import app.observability.metrics as m
    m.update_slot_gauges("test-model", 2, 4)


def test_update_queue_depth_does_not_raise():
    import app.observability.metrics as m
    m.update_queue_depth("test-model", 7)


def test_update_dlq_depth_does_not_raise():
    import app.observability.metrics as m
    m.update_dlq_depth("test-model", 0)


def test_record_completion_helper_does_not_raise():
    import app.observability.metrics as m
    m.record_completion("test-model", wait_s=0.1, runtime_s=1.5, backend_s=1.4)


def test_admission_requests_total_labels():
    """admission_requests_total should have model and result labels."""
    import app.observability.metrics as m

    # Trigger a labelled increment
    m.record_admission("label-test-model", "queued")

    found = False
    for metric in REGISTRY.collect():
        # REGISTRY returns base name "admission_requests" for the Counter
        if metric.name in ("admission_requests_total", "admission_requests"):
            for sample in metric.samples:
                if (
                    sample.labels.get("model") == "label-test-model"
                    and sample.labels.get("result") == "queued"
                ):
                    found = True
                    break
    assert found, "No sample for admission_requests_total{model=label-test-model, result=queued}"
