"""Centralized Redis key naming helpers.

All Redis keys used by the scheduler flow through these pure functions so
that key names are consistent and can be changed in one place.
"""

from __future__ import annotations


def slots_in_use_key(model: str) -> str:
    """STRING key: atomic INCR/DECR slot counter for in-use slots."""
    return f"slots:{model}:in_use"


def slots_capacity_key(model: str) -> str:
    """STRING key: max-concurrency capacity (set at boot, read-only at runtime)."""
    return f"slots:{model}:capacity"


def queue_key(model: str) -> str:
    """ZSET key: priority queue for pending jobs, score = composite priority."""
    return f"queue:{model}"


def job_key(job_id: str) -> str:
    """HASH key: full job state, response blob, timestamps."""
    return f"job:{job_id}"


def idem_key(sha256_hash: str) -> str:
    """STRING key: idempotency record mapping SHA256(idempotency-key) → job_id."""
    return f"idem:{sha256_hash}"


def lease_key(job_id: str) -> str:
    """STRING key with TTL: dispatcher worker holds this while the job is in-flight."""
    return f"lease:{job_id}"


def inflight_key(model: str) -> str:
    """ZSET key: in-flight jobs with score = dispatch epoch (for lease watcher)."""
    return f"inflight:{model}"


def dlq_key(model: str) -> str:
    """SET key: dead-letter queue for jobs that failed due to worker crash."""
    return f"dlq:{model}"


def seq_key(model: str) -> str:
    """STRING key: monotonic sequence counter to break priority score ties."""
    return f"seq:{model}"


def job_done_channel(job_id: str) -> str:
    """Pub/Sub channel name: published when a job reaches a terminal state."""
    return f"job_done:{job_id}"
