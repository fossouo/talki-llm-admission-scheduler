"""Pydantic request/response models for all API endpoints."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Chat completion pass-through (OpenAI-compatible)
# ---------------------------------------------------------------------------


class ChatMessage(BaseModel):
    """Single message in an OpenAI chat completion request."""

    role: str
    content: str | list[Any]


class ChatCompletionRequest(BaseModel):
    """OpenAI-compatible chat completion request payload.

    Only `model` and `messages` are required; all other fields are forwarded
    as-is to LiteLLM.
    """

    model: str
    messages: list[ChatMessage]
    temperature: float | None = None
    max_tokens: int | None = None
    stream: bool = False
    metadata: dict[str, Any] | None = None

    model_config = {"extra": "allow"}  # forward unknown fields to LiteLLM


# ---------------------------------------------------------------------------
# Async job submission
# ---------------------------------------------------------------------------


class JobSubmitRequest(BaseModel):
    """Request body for POST /jobs (explicit async submission)."""

    model: str
    messages: list[ChatMessage]
    temperature: float | None = None
    max_tokens: int | None = None
    metadata: dict[str, Any] | None = None
    priority: int | None = Field(default=None, description="Priority tier: 1=critical … 9=batch")

    model_config = {"extra": "allow"}


class JobSubmitResponse(BaseModel):
    """202 response for POST /jobs."""

    job_id: str
    status: str
    position_in_queue: int | None = None


# ---------------------------------------------------------------------------
# Job status / result
# ---------------------------------------------------------------------------


class JobStatusResponse(BaseModel):
    """Response for GET /jobs/{job_id}."""

    job_id: str
    status: str
    model: str | None = None
    priority: int | None = None
    enqueue_ts: str | None = None
    dispatch_ts: str | None = None
    complete_ts: str | None = None
    wait_ms: int | None = None
    runtime_ms: int | None = None
    error_class: str | None = None
    error_message: str | None = None
    http_status: int | None = None
    result: dict[str, Any] | None = None


# ---------------------------------------------------------------------------
# Queue snapshot
# ---------------------------------------------------------------------------


class ModelQueueSnapshot(BaseModel):
    """Per-model queue state for GET /queues."""

    depth: int
    in_use: int
    capacity: int
    oldest_enqueue_ms: int | None = None


class QueuesResponse(BaseModel):
    """Response for GET /queues — snapshot per model."""

    queues: dict[str, ModelQueueSnapshot]


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


class WorkerStatus(BaseModel):
    """Liveness status of a single dispatcher worker."""

    alive: bool


class HealthResponse(BaseModel):
    """Response for GET /health."""

    status: str  # "ok" | "degraded" | "down"
    redis: str  # "ok" | "down"
    litellm: str  # "ok" | "degraded" | "down"
    workers: dict[str, WorkerStatus]


# ---------------------------------------------------------------------------
# Cancellation
# ---------------------------------------------------------------------------


class CancelResponse(BaseModel):
    """Response for POST /jobs/{job_id}/cancel."""

    job_id: str
    status: str  # "cancelled" | "cancelling" | current terminal status
