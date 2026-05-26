"""Integration tests for GET /health.

Tests:
  - Normal case: Redis up + workers alive → 200 {"status": "ok"|"degraded"}
  - Redis killed mid-test → 503
"""

from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_health_returns_200_when_redis_up(client):
    """GET /health must return 200 with status ok or degraded when Redis is up."""
    resp = await client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] in ("ok", "degraded"), f"Unexpected status: {body['status']}"
    assert body["redis"] == "ok"


@pytest.mark.asyncio
async def test_health_body_has_required_fields(client):
    """GET /health must include status, redis, litellm, workers fields."""
    resp = await client.get("/health")
    body = resp.json()
    assert "status" in body
    assert "redis" in body
    assert "litellm" in body
    assert "workers" in body
    assert isinstance(body["workers"], dict)


@pytest.mark.asyncio
async def test_health_workers_are_alive(client, app):
    """All model workers must report alive=true at startup."""
    resp = await client.get("/health")
    body = resp.json()
    for model, status in body["workers"].items():
        assert status["alive"] is True, f"Worker {model} not alive"


@pytest.mark.asyncio
async def test_health_returns_503_when_redis_down(app, fake_litellm_server, tmp_path):
    """GET /health must return 503 when Redis is unreachable."""
    import httpx
    from unittest.mock import AsyncMock, patch

    # Patch the Redis ping to simulate a down Redis
    original_redis = app.state.redis

    with patch.object(original_redis, "ping", new=AsyncMock(side_effect=Exception("Redis down"))):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://testserver",
            timeout=httpx.Timeout(10.0),
        ) as c:
            resp = await c.get("/health")

    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "down"
    assert body["redis"] == "down"
