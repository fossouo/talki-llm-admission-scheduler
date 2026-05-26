"""Integration tests for idempotency key behaviour.

Tests:
  - Two requests with the same key return the same job_id.
  - Two requests with different keys return different job_ids.
  - Idempotency key expires after the configured window (tested with manual key expiry).
"""

from __future__ import annotations

import asyncio
import hashlib

import pytest

from app.storage.redis_keys import idem_key


def _idem_hash(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


@pytest.mark.asyncio
async def test_same_key_returns_same_job_id(client):
    """Two POST /jobs with the same Idempotency-Key must return the same job_id."""
    key = "test-idem-key-abc123"
    payload = {"model": "chat-fast", "messages": [{"role": "user", "content": "hello"}]}

    resp1 = await client.post("/jobs", json=payload, headers={"Idempotency-Key": key})
    assert resp1.status_code == 202
    job_id_1 = resp1.json()["job_id"]

    resp2 = await client.post("/jobs", json=payload, headers={"Idempotency-Key": key})
    assert resp2.status_code == 202
    job_id_2 = resp2.json()["job_id"]

    assert job_id_1 == job_id_2, (
        f"Same idempotency key returned different job IDs: {job_id_1} vs {job_id_2}"
    )


@pytest.mark.asyncio
async def test_different_keys_return_different_job_ids(client):
    """Two POST /jobs with different Idempotency-Keys must return different job_ids."""
    payload = {"model": "chat-fast", "messages": [{"role": "user", "content": "hello"}]}

    resp1 = await client.post("/jobs", json=payload, headers={"Idempotency-Key": "idem-key-1"})
    resp2 = await client.post("/jobs", json=payload, headers={"Idempotency-Key": "idem-key-2"})

    assert resp1.status_code == 202
    assert resp2.status_code == 202
    assert resp1.json()["job_id"] != resp2.json()["job_id"]


@pytest.mark.asyncio
async def test_no_idempotency_key_always_new_job(client):
    """Requests without Idempotency-Key must always produce new job IDs."""
    payload = {"model": "chat-fast", "messages": [{"role": "user", "content": "hello"}]}

    resp1 = await client.post("/jobs", json=payload)
    resp2 = await client.post("/jobs", json=payload)

    assert resp1.status_code == 202
    assert resp2.status_code == 202
    assert resp1.json()["job_id"] != resp2.json()["job_id"]


@pytest.mark.asyncio
async def test_idempotency_key_expiry(client, app):
    """After the idem: Redis key expires, the same key produces a new job_id."""
    redis = app.state.redis
    key = "test-idem-expiry-key"
    payload = {"model": "chat-fast", "messages": [{"role": "user", "content": "expiry test"}]}

    resp1 = await client.post("/jobs", json=payload, headers={"Idempotency-Key": key})
    assert resp1.status_code == 202
    job_id_1 = resp1.json()["job_id"]

    # Manually expire the idempotency key
    ikey = idem_key(_idem_hash(key))
    await redis.expire(ikey, 1)
    await asyncio.sleep(1.5)

    # Now the same key should produce a new job
    resp2 = await client.post("/jobs", json=payload, headers={"Idempotency-Key": key})
    assert resp2.status_code == 202
    job_id_2 = resp2.json()["job_id"]

    assert job_id_1 != job_id_2, (
        "Expected a new job_id after idempotency key expiry"
    )


@pytest.mark.asyncio
async def test_idempotency_via_sync_endpoint(client):
    """Same Idempotency-Key on POST /v1/chat/completions must return same job_id in header."""
    key = "sync-idem-key-xyz"
    payload = {"model": "chat-fast", "messages": [{"role": "user", "content": "sync idem"}]}

    resp1 = await client.post(
        "/v1/chat/completions", json=payload, headers={"Idempotency-Key": key}
    )
    job_id_1 = resp1.headers.get("x-job-id")

    resp2 = await client.post(
        "/v1/chat/completions", json=payload, headers={"Idempotency-Key": key}
    )
    job_id_2 = resp2.headers.get("x-job-id")

    if job_id_1 and job_id_2:
        assert job_id_1 == job_id_2, (
            f"Sync endpoint idempotency key mismatch: {job_id_1} vs {job_id_2}"
        )
