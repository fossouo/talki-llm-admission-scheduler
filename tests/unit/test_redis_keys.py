"""Unit tests for Redis key naming helpers.

Verifies that each helper returns the exact expected string for alphanumeric
and hyphenated model names.
"""

from __future__ import annotations

import pytest

from app.storage.redis_keys import (
    dlq_key,
    idem_key,
    inflight_key,
    job_done_channel,
    job_key,
    lease_key,
    queue_key,
    seq_key,
    slots_capacity_key,
    slots_in_use_key,
)


@pytest.mark.parametrize(
    "model, expected",
    [
        ("chat-fast", "slots:chat-fast:in_use"),
        ("code", "slots:code:in_use"),
        ("brain-exec", "slots:brain-exec:in_use"),
        ("tool-worker", "slots:tool-worker:in_use"),
        ("model123", "slots:model123:in_use"),
    ],
)
def test_slots_in_use_key(model: str, expected: str):
    assert slots_in_use_key(model) == expected


@pytest.mark.parametrize(
    "model, expected",
    [
        ("chat-fast", "slots:chat-fast:capacity"),
        ("code", "slots:code:capacity"),
    ],
)
def test_slots_capacity_key(model: str, expected: str):
    assert slots_capacity_key(model) == expected


@pytest.mark.parametrize(
    "model, expected",
    [
        ("chat-fast", "queue:chat-fast"),
        ("code", "queue:code"),
        ("brain-exec", "queue:brain-exec"),
    ],
)
def test_queue_key(model: str, expected: str):
    assert queue_key(model) == expected


@pytest.mark.parametrize(
    "job_id, expected",
    [
        ("abc123", "job:abc123"),
        ("01935c2e-dead-7abc-beef-112233445566", "job:01935c2e-dead-7abc-beef-112233445566"),
    ],
)
def test_job_key(job_id: str, expected: str):
    assert job_key(job_id) == expected


@pytest.mark.parametrize(
    "sha, expected",
    [
        ("abc", "idem:abc"),
        ("deadbeef" * 8, f"idem:{'deadbeef' * 8}"),
    ],
)
def test_idem_key(sha: str, expected: str):
    assert idem_key(sha) == expected


@pytest.mark.parametrize(
    "job_id, expected",
    [
        ("myjob", "lease:myjob"),
    ],
)
def test_lease_key(job_id: str, expected: str):
    assert lease_key(job_id) == expected


@pytest.mark.parametrize(
    "model, expected",
    [
        ("chat-fast", "inflight:chat-fast"),
        ("code", "inflight:code"),
    ],
)
def test_inflight_key(model: str, expected: str):
    assert inflight_key(model) == expected


@pytest.mark.parametrize(
    "model, expected",
    [
        ("chat-fast", "dlq:chat-fast"),
        ("code", "dlq:code"),
    ],
)
def test_dlq_key(model: str, expected: str):
    assert dlq_key(model) == expected


@pytest.mark.parametrize(
    "model, expected",
    [
        ("chat-fast", "seq:chat-fast"),
        ("code", "seq:code"),
    ],
)
def test_seq_key(model: str, expected: str):
    assert seq_key(model) == expected


@pytest.mark.parametrize(
    "job_id, expected",
    [
        ("myjob", "job_done:myjob"),
        ("01935c2e-dead-7abc-beef-112233445566", "job_done:01935c2e-dead-7abc-beef-112233445566"),
    ],
)
def test_job_done_channel(job_id: str, expected: str):
    assert job_done_channel(job_id) == expected


def test_all_keys_are_strings():
    """Sanity: every key function returns a str, not bytes."""
    from app.core.uuid7 import uuid7
    jid = uuid7()
    assert isinstance(slots_in_use_key("m"), str)
    assert isinstance(slots_capacity_key("m"), str)
    assert isinstance(queue_key("m"), str)
    assert isinstance(job_key(jid), str)
    assert isinstance(idem_key("hash"), str)
    assert isinstance(lease_key(jid), str)
    assert isinstance(inflight_key("m"), str)
    assert isinstance(dlq_key("m"), str)
    assert isinstance(seq_key("m"), str)
    assert isinstance(job_done_channel(jid), str)


def test_hyphenated_model_name_round_trips():
    """Hyphen in model name must be preserved verbatim in all keys."""
    model = "chat-fast"
    assert "chat-fast" in slots_in_use_key(model)
    assert "chat-fast" in queue_key(model)
    assert "chat-fast" in inflight_key(model)
    assert "chat-fast" in dlq_key(model)
