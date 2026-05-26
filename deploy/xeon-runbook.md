# Xeon Deployment Runbook — LLM Admission Scheduler

**Target host**: `xeon.tail9beaa9.ts.net` (Linux, Docker 24+)
**Container port**: `4001`
**Stack**: `docker compose` (v2 syntax, no hyphen)

**See also**: [README.md](../README.md) · [docs/architecture.md](../docs/architecture.md) · [docs/litellm-audit.md](../docs/litellm-audit.md) · [docs/integration-plan.md](../docs/integration-plan.md)

---

## Prerequisites

### 1. Verify Docker is installed

```bash
docker version
# Must show Server running on linux/amd64
docker compose version
# Must be ≥ 2.20
```

### 2. Create the log mount and set ownership

The container runs as UID 10001 (`scheduler`).  The host directory must be
owned by that UID or the app will silently drop JSONL writes.

```bash
sudo mkdir -p /mnt/talki-logs/admission-scheduler
sudo chown 10001:10001 /mnt/talki-logs/admission-scheduler
```

Verify:

```bash
stat -c "%u %g %n" /mnt/talki-logs/admission-scheduler
# → 10001 10001 /mnt/talki-logs/admission-scheduler
```

### 3. Clone the repo

```bash
sudo mkdir -p /opt/admission-scheduler
sudo chown "$(id -u):$(id -g)" /opt/admission-scheduler
git clone git@github.com:fossouo/talki-app.git /opt/admission-scheduler
# Or if you already have it:
cd /opt/admission-scheduler && git pull
```

> The compose file references `./config/xeon.yaml` relative to itself, so
> always run `docker compose` from the repo root.

---

## Start

```bash
cd /opt/admission-scheduler
docker compose up -d --build
```

First run pulls Redis 7-alpine, builds the Python image (~90 s), then starts
both services.  Check startup:

```bash
docker compose ps
# Both services should show status "Up" and health "healthy" within 60 s.
```

---

## Stop

```bash
docker compose down
# Redis volume is preserved; in-flight jobs survive a restart.
```

---

## Restart (config change, no rebuild)

Edit `config/xeon.yaml`, then:

```bash
docker compose restart admission-scheduler
```

The new config is re-read at startup (the yaml is mounted read-only).

For a code change (new image):

```bash
docker compose up -d --build admission-scheduler
```

---

## Status

```bash
# Container health
docker compose ps

# Health endpoint
curl -fsS http://localhost:4001/health | jq .
# Expected: {"status": "ok", "redis": "ok", "ts": "..."}

# Queue depths + slot usage
curl -fsS http://localhost:4001/queues | jq .

# Prometheus text metrics (first 30 lines)
curl -fsS http://localhost:4001/metrics | head -30
```

---

## Logs

### Application stderr/stdout (structured)

```bash
docker compose logs -f admission-scheduler
```

### JSONL event log (lifecycle events)

```bash
# Today's events
tail -F /mnt/talki-logs/admission-scheduler/events-$(date -u +%Y-%m-%d).jsonl

# Pretty-print last 20 events
tail -20 /mnt/talki-logs/admission-scheduler/events-$(date -u +%Y-%m-%d).jsonl | jq .
```

### Redis traffic (very chatty — for debugging only)

```bash
docker exec admission-scheduler-redis redis-cli MONITOR
# Ctrl-C to stop — this logs EVERY Redis command
```

---

## Inspect Redis

```bash
# Open an interactive redis-cli shell inside the container
docker exec -it admission-scheduler-redis redis-cli

# Useful commands once inside:
KEYS slots:*                           # all slot counters
GET  slots:chat-fast:in_use            # current in-use count for a model
GET  slots:chat-fast:capacity          # configured capacity

ZRANGE queue:code 0 -1 WITHSCORES     # queued jobs for 'code' model (lowest score = highest priority)
ZCARD queue:chat-fast                  # queue depth

HGETALL job:<id>                       # full state for a specific job
KEYS idem:*                            # idempotency keys
SMEMBERS dlq:code                      # dead-letter jobs for 'code'
ZRANGE inflight:code 0 -1 WITHSCORES  # dispatched jobs + their lease timestamps
```

You can also reach Redis from the host on port 6390:

```bash
redis-cli -p 6390 INFO memory
```

---

## Rollback

### Config-only rollback (fast, no data loss)

```bash
git checkout config/xeon.yaml        # revert YAML to last committed state
docker compose restart admission-scheduler
```

### Code rollback (new image from previous commit)

```bash
git checkout <previous-sha>
docker compose up -d --build admission-scheduler
```

In-flight jobs survive as long as the Redis volume is intact.

---

## Cleanup (full wipe)

```bash
docker compose down -v
# Drops the Redis data volume — all queued/in-flight jobs are lost.
# Irreversible.
```

---

## Smoke test

### Quick health check

```bash
curl -fsS http://localhost:4001/health | jq .
```

### Sync chat-completions (OpenAI-compatible)

```bash
curl -X POST http://localhost:4001/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "chat-fast",
    "messages": [{"role": "user", "content": "ping"}],
    "max_tokens": 16
  }' | jq .choices[0].message.content
```

Expected: a short text reply (forwarded from LiteLLM).

### Async job submission + poll

```bash
JOB=$(curl -fsS -X POST http://localhost:4001/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "model": "chat-fast",
    "messages": [{"role": "user", "content": "ping async"}],
    "max_tokens": 16
  }')
echo "$JOB" | jq .

JOB_ID=$(echo "$JOB" | jq -r .job_id)

# Poll until terminal
for i in $(seq 1 30); do
  STATUS=$(curl -fsS "http://localhost:4001/jobs/$JOB_ID" | jq -r .status)
  echo "  [$i] $STATUS"
  [[ "$STATUS" == "completed" || "$STATUS" == "failed" || "$STATUS" == "timeout" ]] && break
  sleep 2
done

# Fetch result
curl -fsS "http://localhost:4001/jobs/$JOB_ID" | jq .
```

---

## Q-006 reminder

Any shell script that calls the scheduler with `curl` MUST wrap the call in a
bounded timeout (Quality Gate Q-006 from `.claude/rules/quality-gates.md`):

```bash
# Correct — applies gtimeout; falls back to 'timeout' if gtimeout absent
_TO=$(command -v gtimeout || command -v timeout)
$_TO 60 curl -X POST http://localhost:4001/v1/chat/completions ...

# Wrong — no timeout; a hung scheduler would block the caller indefinitely
curl -X POST http://localhost:4001/v1/chat/completions ...
```

The scheduler enforces its own `sync_timeout_seconds` per model, but the
client-side guard is still required for scripts that cannot rely on the
scheduler being reachable.

---

## Architecture notes

- Port **4001** is the scheduler.  Port **4000** is LiteLLM (unchanged).
- Redis is exposed on host port **6390** only (avoids clash with any existing Redis on 6379).
- `host.docker.internal` resolves to the Docker bridge gateway (Linux,
  requires Docker 20.10+ and the `extra_hosts: host-gateway` in compose).
  This is how the scheduler container reaches `xeon:4000` (LiteLLM).
- Single-worker uvicorn is intentional — see Dockerfile comment.
