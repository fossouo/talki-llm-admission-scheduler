"""Fake LiteLLM server for integration tests.

Exposes a tiny FastAPI app that emulates LiteLLM's POST /v1/chat/completions.

Control headers (only honoured by the fake):
  X-Fake-Latency-Ms     Sleep this many ms before responding (default 50).
  X-Fake-Status         Respond with this HTTP status instead of 200.
  X-Fake-Body           Literal JSON string to return as body.
  X-Fake-Reset-Connection  If "1", raise an abrupt disconnect (close without response).

Health endpoint:
  GET /health  →  200 {"status": "ok"}
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import uvicorn
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

_CANNED_RESPONSE: dict[str, Any] = {
    "id": "chatcmpl-fake",
    "object": "chat.completion",
    "model": "fake-model",
    "choices": [
        {
            "index": 0,
            "message": {"role": "assistant", "content": "fake response"},
            "finish_reason": "stop",
        }
    ],
    "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
}


def make_fake_litellm_app() -> FastAPI:
    """Create and return the fake LiteLLM FastAPI app."""
    app = FastAPI(title="FakeLiteLLM")

    @app.post("/v1/chat/completions")
    async def chat_completions(request: Request) -> Response:
        # Read control headers
        latency_ms = int(request.headers.get("X-Fake-Latency-Ms", "50"))
        fake_status = request.headers.get("X-Fake-Status")
        fake_body = request.headers.get("X-Fake-Body")
        reset_connection = request.headers.get("X-Fake-Reset-Connection", "0")

        # Simulate network reset by closing the connection without a response.
        # We do this by raising an exception that will cause uvicorn to reset.
        if reset_connection == "1":
            raise RuntimeError("Simulated connection reset by fake server")

        if latency_ms > 0:
            await asyncio.sleep(latency_ms / 1000.0)

        if fake_status:
            status_code = int(fake_status)
            body: dict[str, Any] = {"error": f"fake error status {status_code}"}
            if fake_body:
                try:
                    body = json.loads(fake_body)
                except json.JSONDecodeError:
                    pass
            return JSONResponse(content=body, status_code=status_code)

        if fake_body:
            try:
                body_dict = json.loads(fake_body)
                return JSONResponse(content=body_dict, status_code=200)
            except json.JSONDecodeError:
                pass

        return JSONResponse(content=_CANNED_RESPONSE, status_code=200)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return app


class FakeLiteLLMServer:
    """Helper to run the fake LiteLLM app in a background thread for tests."""

    def __init__(self, host: str = "127.0.0.1", port: int = 0) -> None:
        self.host = host
        self._requested_port = port
        self.port: int = 0
        self._server: uvicorn.Server | None = None
        self._task: asyncio.Task[None] | None = None
        self.app = make_fake_litellm_app()

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    async def start(self) -> None:
        """Start the fake server on a free port."""
        import socket

        # Find a free port if 0 was requested
        if self._requested_port == 0:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind((self.host, 0))
                self.port = s.getsockname()[1]
        else:
            self.port = self._requested_port

        config = uvicorn.Config(
            self.app,
            host=self.host,
            port=self.port,
            log_level="warning",
            loop="asyncio",
            access_log=False,
        )
        self._server = uvicorn.Server(config)
        # Run server in background task
        self._task = asyncio.create_task(self._server.serve())
        # Wait until the server is actually accepting connections
        await self._wait_ready()

    async def _wait_ready(self, timeout: float = 5.0) -> None:
        """Poll until the server port is open."""
        import socket

        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            try:
                with socket.create_connection((self.host, self.port), timeout=0.1):
                    return
            except OSError:
                await asyncio.sleep(0.05)
        raise TimeoutError(f"Fake LiteLLM server did not start in {timeout}s")

    async def stop(self) -> None:
        """Stop the server gracefully."""
        if self._server is not None:
            self._server.should_exit = True
            if self._task is not None:
                try:
                    await asyncio.wait_for(self._task, timeout=3.0)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    self._task.cancel()
