"""httpx forwarding client for LiteLLM proxy.

A single shared AsyncClient is created at startup and reused across requests
for connection pooling. Each call uses a per-request timeout to avoid tying
up a dispatcher slot indefinitely.

v0.1 only supports non-streaming (stream=false). The `model` field in the
request payload is preserved exactly as-is — accounting is keyed on the alias
the scheduler sends, not on the alias returned in the response body (see
LiteLLM Issue #22709).
"""

from __future__ import annotations

from typing import Any

import httpx

from app.observability import metrics as m


class LiteLLMForwarder:
    """Thin async httpx wrapper for forwarding chat completion requests."""

    def __init__(self, backend_url: str) -> None:
        self._backend_url = backend_url.rstrip("/")
        self._client: httpx.AsyncClient | None = None

    async def start(self) -> None:
        """Create the shared AsyncClient. Call once at startup."""
        self._client = httpx.AsyncClient(
            limits=httpx.Limits(
                max_connections=100,
                max_keepalive_connections=50,
                keepalive_expiry=30.0,
            ),
            headers={"Content-Type": "application/json"},
        )

    async def stop(self) -> None:
        """Close the shared AsyncClient. Call on graceful shutdown."""
        if self._client is not None:
            await self._client.aclose()

    async def forward(
        self,
        payload: dict[str, Any],
        *,
        timeout_s: float,
    ) -> dict[str, Any]:
        """POST payload to LiteLLM /v1/chat/completions and return parsed JSON.

        The `model` field is forwarded unchanged. Streaming is disabled for v0.1.
        Raises httpx exceptions on network failure or non-2xx responses.
        """
        if self._client is None:
            raise RuntimeError("LiteLLMForwarder not started — call start() first")

        # Force non-streaming for v0.1
        safe_payload = dict(payload)
        safe_payload["stream"] = False

        url = f"{self._backend_url}/v1/chat/completions"
        timeout = httpx.Timeout(timeout_s)

        response = await self._client.post(url, json=safe_payload, timeout=timeout)
        response.raise_for_status()
        return response.json()

    def health_url(self) -> str:
        """Return the LiteLLM health check URL."""
        return f"{self._backend_url}/health"

    async def check_health(self) -> str:
        """Return 'ok', 'degraded', or 'down' based on LiteLLM health endpoint."""
        if self._client is None:
            return "down"
        try:
            resp = await self._client.get(
                self.health_url(),
                timeout=httpx.Timeout(5.0),
            )
            if resp.status_code == 200:
                return "ok"
            return "degraded"
        except httpx.HTTPStatusError:
            return "degraded"
        except (httpx.ConnectError, httpx.TimeoutException, Exception):
            m.record_redis_error("litellm_health_check")  # reuse redis_errors label
            return "down"
