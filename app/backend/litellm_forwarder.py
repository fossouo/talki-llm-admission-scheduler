"""httpx forwarding client for LiteLLM proxy.

Supports authentication via Authorization header.
"""

from __future__ import annotations
from typing import Any
import httpx
from app.observability import metrics as m

def extract_backend_target(response: httpx.Response) -> str | None:
    for header in ("x-litellm-deployment", "x-litellm-model"):
        value = response.headers.get(header, "").strip()
        if value: return value
    try:
        body = response.json()
        body_model = (body.get("model") or "").strip()
        if body_model: return body_model
    except: pass
    return None

class LiteLLMForwarder:
    def __init__(self, backend_url: str, api_key: str | None = None) -> None:
        self._backend_url = backend_url.rstrip("/")
        self._api_key = api_key
        self._client: httpx.AsyncClient | None = None

    async def start(self) -> None:
        headers = {"Content-Type": "application/json"}
        if self._api_key:
            headers["Authorization"] = f"Bearer {self._api_key}"
            
        self._client = httpx.AsyncClient(
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=50, keepalive_expiry=30.0),
            headers=headers,
        )

    async def stop(self) -> None:
        if self._client is not None:
            await self._client.aclose()

    async def forward(self, payload: dict[str, Any], *, timeout_s: float) -> tuple[dict[str, Any], str | None]:
        if self._client is None: raise RuntimeError("LiteLLMForwarder not started")
        safe_payload = dict(payload)
        safe_payload["stream"] = False
        url = f"{self._backend_url}/v1/chat/completions"
        response = await self._client.post(url, json=safe_payload, timeout=httpx.Timeout(timeout_s))
        response.raise_for_status()
        return response.json(), extract_backend_target(response)

    def health_url(self) -> str:
        # LiteLLM /health performs a real inference against every configured
        # deployment. A liveness loop must never generate tokens or wake a
        # cold model; this endpoint checks only the proxy process.
        return f"{self._backend_url}/health/liveliness"

    async def check_health(self) -> str:
        if self._client is None: return "down"
        try:
            resp = await self._client.get(self.health_url(), timeout=httpx.Timeout(5.0))
            return "ok" if resp.status_code == 200 else "degraded"
        except: return "down"
