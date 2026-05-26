"""Thin wrapper around Redis HASH operations for job state.

All reads and writes to job:{job_id} hashes go through this module so that
field names and serialization are defined in one place.
"""

from __future__ import annotations

import gzip
import json
import time
from typing import Any

import redis.asyncio as aioredis

from app.state import JobStatus
from app.storage.redis_keys import job_key

# TTL applied to job hashes once they reach a terminal state (seconds).
JOB_TERMINAL_TTL = 3600


class JobStore:
    """Thin async wrapper for job hash CRUD operations."""

    def __init__(self, redis: aioredis.Redis) -> None:
        self._redis = redis

    async def get_all(self, job_id: str) -> dict[str, str] | None:
        """Return all fields of the job hash, or None if the key does not exist."""
        data = await self._redis.hgetall(job_key(job_id))
        if not data:
            return None
        return {
            (k.decode() if isinstance(k, bytes) else k): (
                v.decode() if isinstance(v, bytes) else v
            )
            for k, v in data.items()
        }

    async def get_field(self, job_id: str, field: str) -> str | None:
        """Return a single field from the job hash."""
        val = await self._redis.hget(job_key(job_id), field)
        if val is None:
            return None
        return val.decode() if isinstance(val, bytes) else val

    async def set_fields(self, job_id: str, **fields: Any) -> None:
        """Set one or more fields on the job hash."""
        if not fields:
            return
        await self._redis.hset(job_key(job_id), mapping={k: str(v) for k, v in fields.items()})

    async def mark_terminal(
        self,
        job_id: str,
        status: JobStatus,
        complete_ts: str | None = None,
        **extra_fields: Any,
    ) -> None:
        """Set terminal status + complete_ts and apply TTL."""
        ts = complete_ts or _iso_now()
        mapping: dict[str, str] = {"status": status.value, "complete_ts": ts}
        mapping.update({k: str(v) for k, v in extra_fields.items()})
        key = job_key(job_id)
        pipe = self._redis.pipeline()
        pipe.hset(key, mapping=mapping)
        pipe.expire(key, JOB_TERMINAL_TTL)
        await pipe.execute()

    async def store_response_blob(self, job_id: str, response: dict[str, Any]) -> None:
        """Gzip-compress and store the LiteLLM response blob in the job hash."""
        blob = gzip.compress(json.dumps(response).encode())
        await self._redis.hset(job_key(job_id), "response_blob", blob)

    async def load_response_blob(self, job_id: str) -> dict[str, Any] | None:
        """Decompress and return the stored response blob, or None if absent."""
        raw = await self._redis.hget(job_key(job_id), "response_blob")
        if raw is None:
            return None
        try:
            return json.loads(gzip.decompress(raw))
        except Exception:
            return None

    async def exists(self, job_id: str) -> bool:
        """Return True if the job hash exists in Redis."""
        return bool(await self._redis.exists(job_key(job_id)))


def _iso_now() -> str:
    """Return current UTC time as an RFC3339 string with microsecond precision."""
    import datetime

    return datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
