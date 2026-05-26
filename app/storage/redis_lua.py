"""Lua scripts for atomic Redis operations.

All scripts are defined as module-level constants and loaded to Redis via
SCRIPT LOAD at startup. The SHA1 returned by SCRIPT LOAD is cached and
used via EVALSHA on every call.
"""

from __future__ import annotations

import redis.asyncio as aioredis


# ---------------------------------------------------------------------------
# Script: claim_slot
# Atomically check if a concurrency slot is available and claim it.
# Sets job status to "dispatched" and records dispatch_ts.
#
# KEYS[1] = slots:{model}:in_use
# KEYS[2] = slots:{model}:capacity
# ARGV[1] = job_id
# ARGV[2] = ts_iso (dispatch timestamp as ISO string)
# Returns 1 if claimed, 0 if at capacity.
# ---------------------------------------------------------------------------
CLAIM_SLOT_SCRIPT = """
local in_use   = tonumber(redis.call('GET', KEYS[1])) or 0
local capacity = tonumber(redis.call('GET', KEYS[2]))
if not capacity then return 0 end
if in_use < capacity then
  redis.call('INCR', KEYS[1])
  redis.call('HSET', 'job:' .. ARGV[1], 'status', 'dispatched', 'dispatch_ts', ARGV[2])
  return 1
end
return 0
"""

# ---------------------------------------------------------------------------
# Script: cancel_queued
# Atomically remove a job from the queue and mark it cancelled — only if the
# current status is still "queued". Returns 1 on success, 0 if job is not
# in the queue (already dispatched or terminal).
#
# KEYS[1] = queue:{model}
# KEYS[2] = job:{job_id}
# ARGV[1] = job_id
# ARGV[2] = cancel_ts (ISO string)
# Returns 1 if cancelled, 0 if not found in queue.
# ---------------------------------------------------------------------------
CANCEL_QUEUED_SCRIPT = """
local removed = redis.call('ZREM', KEYS[1], ARGV[1])
if removed == 1 then
  redis.call('HSET', KEYS[2], 'status', 'cancelled', 'complete_ts', ARGV[2])
  return 1
end
return 0
"""

# ---------------------------------------------------------------------------
# Script: enqueue_job
# Atomically check queue depth and enqueue the job if under max_queue_depth.
#
# KEYS[1] = queue:{model}
# KEYS[2] = job:{job_id}
# ARGV[1] = job_id
# ARGV[2] = score (composite float as string)
# ARGV[3] = max_queue_depth
# ARGV[4] = job_hash_json — a flat list of field,value pairs
#            encoded as "field1\0value1\0field2\0value2\0..." (NUL separated)
# Returns 1 on success, 0 if queue is full.
# ---------------------------------------------------------------------------
ENQUEUE_JOB_SCRIPT = """
local depth = redis.call('ZCARD', KEYS[1])
local max   = tonumber(ARGV[3])
if depth >= max then return 0 end
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
-- ARGV[4] is the JSON-serialised flat list: [field, value, field, value ...]
-- We pass it as a single string joined by NUL and split here.
local parts = {}
local s = ARGV[4]
local i = 1
for token in string.gmatch(s .. string.char(0), '([^%z]*)%z') do
  parts[i] = token
  i = i + 1
end
-- Pairs: parts[1]=field1, parts[2]=value1, ...
local hset_args = {KEYS[2]}
for j = 1, #parts do
  if parts[j] ~= '' then
    hset_args[#hset_args + 1] = parts[j]
  end
end
redis.call('HSET', unpack(hset_args))
return 1
"""


class LuaScripts:
    """Holds pre-loaded SHA1 handles for each Lua script."""

    def __init__(self) -> None:
        self._claim_slot_sha: str | None = None
        self._cancel_queued_sha: str | None = None
        self._enqueue_job_sha: str | None = None

    async def load_all(self, redis: aioredis.Redis) -> None:
        """SCRIPT LOAD all scripts into Redis and cache their SHA1s."""
        self._claim_slot_sha = await redis.script_load(CLAIM_SLOT_SCRIPT)
        self._cancel_queued_sha = await redis.script_load(CANCEL_QUEUED_SCRIPT)
        self._enqueue_job_sha = await redis.script_load(ENQUEUE_JOB_SCRIPT)

    @property
    def claim_slot_sha(self) -> str:
        """SHA1 of the claim_slot script."""
        if self._claim_slot_sha is None:
            raise RuntimeError("Lua scripts not loaded — call load_all() first")
        return self._claim_slot_sha

    @property
    def cancel_queued_sha(self) -> str:
        """SHA1 of the cancel_queued script."""
        if self._cancel_queued_sha is None:
            raise RuntimeError("Lua scripts not loaded — call load_all() first")
        return self._cancel_queued_sha

    @property
    def enqueue_job_sha(self) -> str:
        """SHA1 of the enqueue_job script."""
        if self._enqueue_job_sha is None:
            raise RuntimeError("Lua scripts not loaded — call load_all() first")
        return self._enqueue_job_sha

    async def evalsha_claim_slot(
        self,
        redis: aioredis.Redis,
        in_use_key: str,
        capacity_key: str,
        job_id: str,
        ts_iso: str,
    ) -> int:
        """Claim a concurrency slot atomically. Returns 1 on success, 0 on full."""
        return await redis.evalsha(
            self.claim_slot_sha,
            2,
            in_use_key,
            capacity_key,
            job_id,
            ts_iso,
        )

    async def evalsha_cancel_queued(
        self,
        redis: aioredis.Redis,
        queue_k: str,
        job_k: str,
        job_id: str,
        cancel_ts: str,
    ) -> int:
        """Remove job from queue and mark cancelled. Returns 1 on success."""
        return await redis.evalsha(
            self.cancel_queued_sha,
            2,
            queue_k,
            job_k,
            job_id,
            cancel_ts,
        )

    async def evalsha_enqueue_job(
        self,
        redis: aioredis.Redis,
        queue_k: str,
        job_k: str,
        job_id: str,
        score: float,
        max_depth: int,
        field_values: list[str],
    ) -> int:
        """Enqueue job atomically if queue depth allows. Returns 1 on success."""
        flat_str = "\0".join(field_values)
        return await redis.evalsha(
            self.enqueue_job_sha,
            2,
            queue_k,
            job_k,
            job_id,
            str(score),
            str(max_depth),
            flat_str,
        )
