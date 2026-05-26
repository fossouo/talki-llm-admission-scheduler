"""Enums and dataclasses for job lifecycle state."""

from __future__ import annotations

import dataclasses
import time
from enum import Enum
from typing import Any


class JobStatus(str, Enum):
    """Possible terminal and transient states for a job."""

    QUEUED = "queued"
    DISPATCHED = "dispatched"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    CANCELLING = "cancelling"


class EventType(str, Enum):
    """JSONL log event types, one per lifecycle transition."""

    ADMITTED = "admitted"
    QUEUED = "queued"
    DISPATCHED = "dispatched"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    WORKER_CRASH = "worker_crash"
    SLOT_DRIFT_CORRECTED = "slot_drift_corrected"


class ErrorClass(str, Enum):
    """Canonical error class labels for metrics and JSONL log."""

    QUEUE_TIMEOUT = "queue_timeout"
    SYNC_TIMEOUT = "sync_timeout"
    BACKEND_5XX = "backend_5xx"
    BACKEND_TIMEOUT = "backend_timeout"
    BACKEND_429 = "backend_429"
    NETWORK_ERROR = "network_error"
    WORKER_CRASH = "worker_crash"
    ADMISSION_REJECTED = "admission_rejected"


class AdmissionResult(str, Enum):
    """Outcome of an admission attempt."""

    ADMITTED = "admitted"
    QUEUED = "queued"
    REJECTED = "rejected"
    IDEMPOTENT_HIT = "idempotent_hit"


@dataclasses.dataclass
class JobRequest:
    """Parsed and validated parameters for a new job submission."""

    job_id: str
    request_id: str
    model: str
    payload: dict[str, Any]
    priority: int
    caller: str
    caller_metadata: dict[str, Any]
    idempotency_key: str | None
    enqueue_ts: float = dataclasses.field(default_factory=time.time)


@dataclasses.dataclass
class AdmissionOutcome:
    """Result returned by AdmissionController.admit()."""

    result: AdmissionResult
    job_id: str
    status: JobStatus
    position_in_queue: int | None = None
    existing_job_id: str | None = None  # filled on idempotency hit
    estimated_drain_s: int | None = None  # filled on rejection
