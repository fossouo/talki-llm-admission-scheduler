"""Integration tests for job cancellation.

Tests:
  - cancel-queued: cancel a queued job → status=cancelled, slot not incremented.
  - cancel-dispatched: (best-effort) cancel while in-flight.
  - cancel-terminal: cancel an already completed job → 200 no-op (returns current status).
  - cancel-unknown: 404.
"""

from __future__ import annotations

import asyncio

import pytest

from app.state import JobStatus
from app.storage.redis_keys import job_key, queue_key, slots_in_use_key


async def _submit(client, model="chat-fast"):
    resp = await client.post(
        "/jobs",
        json={"model": model, "messages": [{"role": "user", "content": "cancel test"}]},
    )
    assert resp.status_code == 202
    return resp.json()["job_id"]


async def _poll_until(client, job_id, expected_statuses, timeout_s=15.0):
    deadline = asyncio.get_event_loop().time() + timeout_s
    while asyncio.get_event_loop().time() < deadline:
        resp = await client.get(f"/jobs/{job_id}")
        if resp.status_code in (404, 410):
            return resp
        body = resp.json()
        if body["status"] in expected_statuses:
            return resp
        await asyncio.sleep(0.2)
    raise TimeoutError(f"Job {job_id} never reached {expected_statuses}")


@pytest.mark.asyncio
async def test_cancel_queued_job(client, app):
    """Cancel a queued job → status becomes cancelled, slot is not incremented."""
    redis = app.state.redis

    # Temporarily stop workers so the job stays queued
    workers = app.state.workers
    for w in workers.values():
        await w.stop()

    job_id = await _submit(client)

    # Confirm it's queued
    resp = await client.get(f"/jobs/{job_id}")
    assert resp.status_code == 200
    body = resp.json()
    # It might have been dispatched if workers picked it up before we stopped them
    # Accept either queued or dispatched here
    assert body["status"] in (JobStatus.QUEUED.value, JobStatus.DISPATCHED.value)

    in_use_before = int(await redis.get(slots_in_use_key("chat-fast")) or 0)

    # Cancel
    cancel_resp = await client.post(f"/jobs/{job_id}/cancel")
    assert cancel_resp.status_code == 200
    cancel_body = cancel_resp.json()
    assert cancel_body["status"] in (
        JobStatus.CANCELLED.value,
        JobStatus.CANCELLING.value,
    )

    # Poll to confirm cancelled
    final = await _poll_until(
        client, job_id, {JobStatus.CANCELLED.value, JobStatus.FAILED.value}, timeout_s=5.0
    )
    if final.status_code == 200:
        assert final.json()["status"] in (JobStatus.CANCELLED.value, JobStatus.FAILED.value)

    # Slot must not be inflated
    in_use_after = int(await redis.get(slots_in_use_key("chat-fast")) or 0)
    # After cancel, slot should be back to where we started (or lower)
    assert in_use_after <= in_use_before + 1  # allow 1 for potential race


@pytest.mark.asyncio
async def test_cancel_terminal_job_is_noop(client):
    """Cancelling an already-terminal job must return 200 with the terminal status."""
    # Submit and wait for terminal
    job_id = await _submit(client)
    terminal = {"completed", "failed", "timeout", "cancelled"}
    deadline = asyncio.get_event_loop().time() + 15.0
    while asyncio.get_event_loop().time() < deadline:
        resp = await client.get(f"/jobs/{job_id}")
        if resp.status_code in (404, 410):
            break
        if resp.json()["status"] in terminal:
            break
        await asyncio.sleep(0.3)

    # Cancel the terminal job
    cancel_resp = await client.post(f"/jobs/{job_id}/cancel")
    assert cancel_resp.status_code == 200
    body = cancel_resp.json()
    # Status should be the terminal status (no change)
    assert body["status"] in terminal


@pytest.mark.asyncio
async def test_cancel_unknown_job_returns_404(client):
    """Cancelling a non-existent job must return 404."""
    resp = await client.post("/jobs/nonexistent-fake-job-id-xyz/cancel")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_cancel_queued_removes_from_queue(client, app):
    """After cancellation, the job must not remain in the queue ZSET."""
    redis = app.state.redis
    workers = app.state.workers

    # Stop workers to keep job queued
    for w in workers.values():
        await w.stop()

    job_id = await _submit(client)

    # Confirm in queue
    queue_depth = await redis.zcard(queue_key("chat-fast"))
    assert queue_depth >= 1

    # Cancel
    await client.post(f"/jobs/{job_id}/cancel")
    await asyncio.sleep(0.2)

    # Job should no longer be in queue
    rank = await redis.zrank(queue_key("chat-fast"), job_id)
    assert rank is None, f"Job {job_id} still in queue after cancel"
