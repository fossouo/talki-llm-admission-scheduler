"""Audit JSONL — chaque action significative du daemon est tracée.

Fichier rotaté quotidiennement : `logs/orchestrator-YYYY-MM-DD.jsonl`.
Chaque ligne = un événement {ts, role, action, payload}.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import json
from pathlib import Path

from .config import TEAM_ROOT

LOG_DIR = TEAM_ROOT / "logs"
_lock = asyncio.Lock()


async def log_event(role: str, action: str, **payload) -> None:
    """Append une ligne JSONL. Thread-safe via asyncio lock."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    day = dt.date.today().isoformat()
    f = LOG_DIR / f"orchestrator-{day}.jsonl"
    line = json.dumps(
        {
            "ts": dt.datetime.now(dt.timezone.utc).isoformat(),
            "role": role,
            "action": action,
            "payload": payload,
        },
        ensure_ascii=False,
        default=str,
    )
    async with _lock:
        with f.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
