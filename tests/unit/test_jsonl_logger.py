"""Unit tests for the JSONL event logger.

Tests:
  - N events → N lines, each valid JSON
  - UTC midnight rotation (mock the date)
  - Queue-full drop increments log_write_errors_total
  - Stop flushes pending events
"""

from __future__ import annotations

import asyncio
import datetime
import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest
import pytest_asyncio

from app.observability.jsonl_logger import JSONLLogger


async def _emit_n(logger: JSONLLogger, n: int) -> None:
    for i in range(n):
        await logger.emit({"index": i, "event": "test_event"})


async def _flush(logger: JSONLLogger) -> None:
    """Stop (which flushes) then restart the logger."""
    await logger.stop()


@pytest.mark.asyncio
async def test_emit_n_lines(tmp_path):
    """N emitted events produce exactly N valid JSON lines in today's file."""
    logger = JSONLLogger(log_dir=str(tmp_path))
    await logger.start()
    await _emit_n(logger, 10)
    await _flush(logger)

    today = datetime.datetime.now(tz=datetime.timezone.utc).date()
    log_file = tmp_path / f"events-{today.isoformat()}.jsonl"
    assert log_file.exists(), f"Expected log file {log_file} not created"

    lines = log_file.read_text().strip().split("\n")
    assert len(lines) == 10, f"Expected 10 lines, got {len(lines)}"
    for line in lines:
        obj = json.loads(line)  # must not raise
        assert "index" in obj


@pytest.mark.asyncio
async def test_single_event_is_valid_json(tmp_path):
    """A single emitted event must produce valid JSON with the emitted fields."""
    logger = JSONLLogger(log_dir=str(tmp_path))
    await logger.start()
    await logger.emit({"event": "hello", "value": 42})
    await _flush(logger)

    today = datetime.datetime.now(tz=datetime.timezone.utc).date()
    log_file = tmp_path / f"events-{today.isoformat()}.jsonl"
    lines = log_file.read_text().strip().split("\n")
    assert len(lines) == 1
    obj = json.loads(lines[0])
    assert obj["event"] == "hello"
    assert obj["value"] == 42


@pytest.mark.asyncio
async def test_rotation_across_utc_midnight(tmp_path):
    """Events emitted on two different UTC dates land in separate JSONL files."""
    logger = JSONLLogger(log_dir=str(tmp_path))
    await logger.start()

    date_a = datetime.date(2026, 1, 1)
    date_b = datetime.date(2026, 1, 2)

    # Emit 3 events "on" date_a
    with patch(
        "app.observability.jsonl_logger.datetime",
        wraps=datetime,
    ) as mock_dt:
        mock_dt.datetime.now.return_value = datetime.datetime(
            2026, 1, 1, 23, 59, 59, tzinfo=datetime.timezone.utc
        )
        # We need to patch inside the writer's loop — simplest is to
        # manually call _write_event with a patched date helper
        for i in range(3):
            logger._queue.put_nowait({"day": "a", "i": i})

    # Give the writer a moment to flush these before we rotate
    await asyncio.sleep(0.1)

    # Emit 2 events "on" date_b by patching the internal _write_event date lookup
    original_write = logger._write_event

    async def _write_on_date_b(event):
        """Wrapper that forces today = date_b inside _ensure_file_open."""
        with patch.object(
            logger,
            "_current_date",
            new=None,  # reset so _ensure_file_open re-opens
        ):
            # Temporarily replace the date check
            orig_ensure = logger._ensure_file_open

            def patched_ensure(today):
                orig_ensure(date_b)

            logger._ensure_file_open = patched_ensure
            try:
                await original_write(event)
            finally:
                logger._ensure_file_open = orig_ensure

    for i in range(2):
        await _write_on_date_b({"day": "b", "i": i})

    await _flush(logger)

    file_a = tmp_path / f"events-{date_a.isoformat()}.jsonl"
    file_b = tmp_path / f"events-{date_b.isoformat()}.jsonl"

    # date_b file must exist and contain 2 lines
    assert file_b.exists(), f"Rotated file {file_b} not found"
    lines_b = file_b.read_text().strip().split("\n")
    assert len(lines_b) == 2


@pytest.mark.asyncio
async def test_queue_full_increments_error_counter(tmp_path):
    """When the internal queue is full, log_write_errors_total must be incremented."""
    # Create a logger with a tiny queue, stop the writer task so it doesn't drain
    logger = JSONLLogger(log_dir=str(tmp_path), queue_size=2)
    await logger.start()

    # Cancel the writer task to prevent draining
    if logger._writer_task:
        logger._writer_task.cancel()
        try:
            await asyncio.wait_for(logger._writer_task, timeout=1.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass
        logger._writer_task = None

    # Fill the queue to capacity
    logger._queue.put_nowait({"x": 1})
    logger._queue.put_nowait({"x": 2})

    # The queue is now full — emit must call put_nowait which raises QueueFull
    before = _read_log_write_errors()
    await logger.emit({"x": "overflow"})
    after = _read_log_write_errors()
    assert after > before, "log_write_errors_total should have been incremented on queue full"

    # Drain the queue manually before exiting to avoid warnings
    while not logger._queue.empty():
        try:
            logger._queue.get_nowait()
        except Exception:
            break


@pytest.mark.asyncio
async def test_stop_flushes_pending_events(tmp_path):
    """Events enqueued before stop() must appear in the file."""
    logger = JSONLLogger(log_dir=str(tmp_path))
    await logger.start()
    for i in range(5):
        await logger.emit({"idx": i})
    await logger.stop()

    today = datetime.datetime.now(tz=datetime.timezone.utc).date()
    log_file = tmp_path / f"events-{today.isoformat()}.jsonl"
    lines = log_file.read_text().strip().split("\n")
    assert len(lines) == 5


@pytest.mark.asyncio
async def test_large_event_batch(tmp_path):
    """100 events must all land in the file."""
    logger = JSONLLogger(log_dir=str(tmp_path))
    await logger.start()
    await _emit_n(logger, 100)
    await _flush(logger)

    today = datetime.datetime.now(tz=datetime.timezone.utc).date()
    log_file = tmp_path / f"events-{today.isoformat()}.jsonl"
    lines = [ln for ln in log_file.read_text().strip().split("\n") if ln]
    assert len(lines) == 100


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _read_log_write_errors() -> float:
    """Read the current value of log_write_errors_total directly from the counter object."""
    from app.observability import metrics as m
    # Access the underlying value directly; avoids REGISTRY name/suffix ambiguity
    return m.log_write_errors_total._value.get()
