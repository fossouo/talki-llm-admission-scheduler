"""Append-only JSONL event logger with UTC-midnight rotation.

Design:
- A single asyncio.Queue receives event dicts from the application.
- A single writer coroutine drains the queue, serializes to JSON, and
  appends to the current day's file. This keeps disk I/O off the hot path.
- On IOError (disk full, permissions) the event is dropped, the error is
  counted via the Prometheus counter, and the writer attempts to reopen the
  file handle on the next iteration.
- UTC midnight rotation is handled by checking the current UTC date before
  each write.
"""

from __future__ import annotations

import asyncio
import datetime
import json
import os
from pathlib import Path
from typing import Any

from app.observability import metrics as m


class JSONLLogger:
    """Non-blocking JSONL event logger with daily UTC rotation."""

    def __init__(self, log_dir: str, queue_size: int = 10_000) -> None:
        self._log_dir = Path(log_dir)
        self._queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue(maxsize=queue_size)
        self._writer_task: asyncio.Task[None] | None = None
        self._current_date: datetime.date | None = None
        self._fh: Any = None  # open file handle

    async def start(self) -> None:
        """Start the background writer coroutine. Call once at app startup."""
        self._log_dir.mkdir(parents=True, exist_ok=True)
        self._writer_task = asyncio.create_task(self._writer(), name="jsonl-logger")

    async def stop(self) -> None:
        """Flush pending events and stop the writer coroutine."""
        await self._queue.put(None)  # sentinel to unblock the writer
        if self._writer_task is not None:
            try:
                await asyncio.wait_for(self._writer_task, timeout=5.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._writer_task.cancel()
        if self._fh is not None:
            try:
                self._fh.flush()
                self._fh.close()
            except OSError:
                pass

    async def emit(self, event_dict: dict[str, Any]) -> None:
        """Non-blocking: enqueue an event dict for async writing.

        If the queue is full (backpressure from a slow disk), the event is
        dropped and the write-error counter is incremented.
        """
        try:
            self._queue.put_nowait(event_dict)
        except asyncio.QueueFull:
            m.log_write_errors_total.inc()

    # ------------------------------------------------------------------
    # Internal writer coroutine
    # ------------------------------------------------------------------

    async def _writer(self) -> None:
        """Drain the queue and write events to the current day's JSONL file."""
        while True:
            event = await self._queue.get()
            if event is None:  # sentinel: flush and exit
                self._queue.task_done()
                break
            await self._write_event(event)
            self._queue.task_done()

    async def _write_event(self, event: dict[str, Any]) -> None:
        """Serialize and append one event to the rotated JSONL file."""
        today = datetime.datetime.now(tz=datetime.timezone.utc).date()
        try:
            self._ensure_file_open(today)
            line = json.dumps(event, default=str) + "\n"
            self._fh.write(line)
            self._fh.flush()
        except OSError:
            # Disk full or permissions error — count and attempt recovery next call.
            m.log_write_errors_total.inc()
            self._fh = None  # force reopen on next call

    def _ensure_file_open(self, today: datetime.date) -> None:
        """Open (or rotate) the log file if date has changed or handle is closed."""
        if self._fh is not None and self._current_date == today:
            return
        # Close previous handle if rotating
        if self._fh is not None:
            try:
                self._fh.flush()
                self._fh.close()
            except OSError:
                pass
        self._current_date = today
        log_path = self._log_dir / f"events-{today.isoformat()}.jsonl"
        # Reopen in append mode; may raise OSError (disk full etc.) — caller handles
        self._fh = open(log_path, "a", encoding="utf-8", buffering=1)  # noqa: SIM115


def build_base_event(
    event: str,
    job_id: str,
    request_id: str,
    model: str,
    caller: str,
    caller_metadata: dict[str, Any] | None,
    priority: int,
) -> dict[str, Any]:
    """Build the base JSONL event dict with mandatory fields."""
    return {
        "ts": datetime.datetime.now(tz=datetime.timezone.utc).isoformat(),
        "request_id": request_id,
        "job_id": job_id,
        "model": model,
        "event": event,
        "caller": caller,
        "caller_metadata": caller_metadata or {},
        "priority": priority,
    }
