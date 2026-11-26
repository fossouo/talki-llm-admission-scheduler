"""Audit JSONL — chaque action significative du daemon est tracée.

Fichier rotaté quotidiennement : logs/orchestrator-YYYY-MM-DD.jsonl.
Chaque ligne = un événement {ts, role, action, payload}.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import uuid
from pathlib import Path
from typing import Any

from .config import TEAM_ROOT

LOG_DIR = TEAM_ROOT / "logs"
_lock = asyncio.Lock()
RUN_ID = f"admission-scheduler-{uuid.uuid4().hex[:6]}"

async def log_event(role: str, action: str, **fields: Any) -> None:
    """Append une ligne JSONL. Thread-safe via asyncio lock."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    day = dt.date.today().isoformat()
    f = LOG_DIR / f"orchestrator-{day}.jsonl"
    
    payload = {
        "schema_version": 2,
        "project_id": "admission-scheduler",
        "repo": "fossouo/talki-llm-admission-scheduler",
        "run_id": RUN_ID,
        "ts": dt.datetime.now(dt.timezone.utc).isoformat(),
        "role": role,
        "action": action,
        "agent_name": role,
        "event_type": action,
        "payload": fields,
        "metadata": fields
    }
    
    line = json.dumps(payload, ensure_ascii=False, default=str)
    
    try:
        async with _lock:
            with f.open("a", encoding="utf-8") as fh:
                fh.write(line + "\n")
    except Exception:
        import logging
        logging.getLogger("orchestrator.audit").exception("audit-write-failed")
