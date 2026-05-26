"""httpx forwarding client for LiteLLM proxy.

A single shared AsyncClient is created at startup and reused across requests
for connection pooling. Each call uses a per-request timeout to avoid tying
up a dispatcher slot indefinitely.

v0.1 only supports non-streaming (stream=false). The `model` field in the
request payload is preserved exactly as-is — accounting is keyed on the alias
the scheduler sends, not on the alias returned in the response body (see
LiteLLM Issue #22709).

v0.2 adds backend_target extraction from response headers to support the new
JSONL schema fields. See extract_backend_target() for the resolution strategy.
"""

from __future__ import annotations

from typing import Any

import httpx

from app.observability import metrics as m


def extract_backend_target(response: httpx.Response) -> str | None:
    """Extract the physical backend target from a LiteLLM proxy response.

    Resolution order (per litellm-audit.md §10 and LiteLLM proxy conventions):
    1. x-litellm-deployment response header — set by LiteLLM proxy to the
       deployment name / backend string (e.g. 'dgx:8004/qwen36-a3b').
    2. x-litellm-model response header — the resolved model alias (may
       include the backend host for some LiteLLM versions).
    3. response body 'model' field — set by LiteLLM to the backend-returned
       model name; useful as a last resort but less precise.
    4. Returns None if none of the above are present or non-empty.

    Note: in Phase 0 shadow mode this will frequently return None because
    the actual Xeon LiteLLM proxy build may not set these headers.
    Downstream consumers MUST treat None as 'not measured'.
    """
    # Header names are case-insensitive in HTTP; httpx normalises to lower-case.
    for header in ("x-litellm-deployment", "x-litellm-model"):
        value = response.headers.get(header, "").strip()
        if value:
            return value

    # Fall back to response body 'model' field
    try:
        body = response.json()
        body_model = (body.get("model") or "").strip()
        if body_model:
            return body_model
    except Exception:
        pass

    return None


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
    ) -> tuple[dict[str, Any], str | None]:
        """POST payload to LiteLLM /v1/chat/completions.

        Returns:
            (response_body, backend_target)

            response_body: parsed JSON response dict.
            backend_target: physical backend string extracted from response
                headers (x-litellm-deployment > x-litellm-model > body.model),
                or None if not determinable. See extract_backend_target().

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
        backend_target = extract_backend_target(response)
        return response.json(), backend_target

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
