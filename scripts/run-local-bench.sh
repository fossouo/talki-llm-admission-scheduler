#!/usr/bin/env bash
# run-local-bench.sh — Spin up fake LiteLLM + scheduler, run benchmark, tear down.
#
# Prerequisites:
#   Option A (Docker — preferred):
#     - Docker Engine 20.10+ with `docker compose` v2 plugin
#     - No pre-existing Redis on 127.0.0.1:6390 (docker-compose uses that port)
#
#   Option B (in-process fallback, no Docker):
#     - Python venv at .venv/ with all project dependencies installed:
#         pip install -e ".[dev]"
#     - fakeredis[lua] installed (included in dev deps)
#     - A local Redis accessible at 127.0.0.1:6379 OR fakeredis-server binary
#
#   Both options:
#     - Python 3.12+ in .venv/
#     - gtimeout (brew install coreutils) or timeout on PATH
#
# Usage:
#   bash scripts/run-local-bench.sh
#   bash scripts/run-local-bench.sh --scenarios low,mid,isolation
#
# Environment overrides:
#   BENCH_SCENARIOS    comma-separated (default: warmup,low,mid,high,mixed,isolation,priority)
#   BENCH_PORT         scheduler port (default: 4001)
#   FAKE_LITELLM_PORT  fake LiteLLM port (default: 4099)
#   COMPOSE_PROJECT    docker compose project name (default: bench)

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BENCH_SCENARIOS="${BENCH_SCENARIOS:-warmup,low,mid,high,mixed,isolation,priority}"
BENCH_PORT="${BENCH_PORT:-4001}"
FAKE_LITELLM_PORT="${FAKE_LITELLM_PORT:-4099}"
COMPOSE_PROJECT="${COMPOSE_PROJECT:-bench}"
PYTHON="${REPO_ROOT}/.venv/bin/python"
TIMESTAMP="$(date -u +%Y%m%dT%H%M%S)"
OUT_JSON="${REPO_ROOT}/benchmarks/results/run-${TIMESTAMP}.json"
OUT_MD="${REPO_ROOT}/benchmarks/results/run-${TIMESTAMP}.md"

# Pass extra args (e.g. --scenarios) to run_bench.py
EXTRA_ARGS=("$@")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
_timeout_cmd() {
    if command -v gtimeout &>/dev/null; then
        echo "gtimeout"
    elif command -v timeout &>/dev/null; then
        echo "timeout"
    else
        echo ""
    fi
}

TIMEOUT_CMD="$(_timeout_cmd)"

_wait_url() {
    local url="$1"
    local max_s="${2:-30}"
    local deadline=$(( $(date +%s) + max_s ))
    echo "  Waiting for ${url} (up to ${max_s}s) …"
    while [ "$(date +%s)" -lt "$deadline" ]; do
        if curl -fsS --max-time 2 "${url}" >/dev/null 2>&1; then
            echo "  ✓ ${url} is up"
            return 0
        fi
        sleep 1
    done
    echo "ERROR: ${url} did not become ready in ${max_s}s" >&2
    return 1
}

# PIDs to clean up
FAKE_LITELLM_PID=""
SCHEDULER_IN_PROC_PID=""
USING_DOCKER=false

# ---------------------------------------------------------------------------
# Cleanup trap
# ---------------------------------------------------------------------------
cleanup() {
    local exit_code=$?
    echo ""
    echo "--- Cleaning up ---"

    if [ -n "$FAKE_LITELLM_PID" ] && kill -0 "$FAKE_LITELLM_PID" 2>/dev/null; then
        echo "  Stopping fake LiteLLM (PID $FAKE_LITELLM_PID) …"
        kill "$FAKE_LITELLM_PID" 2>/dev/null || true
        wait "$FAKE_LITELLM_PID" 2>/dev/null || true
    fi

    if [ -n "$SCHEDULER_IN_PROC_PID" ] && kill -0 "$SCHEDULER_IN_PROC_PID" 2>/dev/null; then
        echo "  Stopping in-process scheduler (PID $SCHEDULER_IN_PROC_PID) …"
        kill "$SCHEDULER_IN_PROC_PID" 2>/dev/null || true
        wait "$SCHEDULER_IN_PROC_PID" 2>/dev/null || true
    fi

    if $USING_DOCKER; then
        echo "  Tearing down docker compose (project=${COMPOSE_PROJECT}) …"
        STANDALONE="${REPO_ROOT}/benchmarks/docker-compose.bench-standalone.yml"
        if [ -f "$STANDALONE" ]; then
            docker compose -p "$COMPOSE_PROJECT" \
                -f "$STANDALONE" \
                down --remove-orphans 2>/dev/null || true
        else
            docker compose -p "$COMPOSE_PROJECT" \
                down --remove-orphans 2>/dev/null || true
        fi
    fi

    exit $exit_code
}
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
# Check Python
# ---------------------------------------------------------------------------
if [ ! -f "$PYTHON" ]; then
    echo "ERROR: Python not found at ${PYTHON}" >&2
    echo "       Run:  python3 -m venv .venv && .venv/bin/pip install -e '.[dev]'" >&2
    exit 1
fi

echo "Talki LLM Admission Scheduler — Local Benchmark"
echo "================================================"
echo "  PYTHON:              $PYTHON"
echo "  BENCH_SCENARIOS:     $BENCH_SCENARIOS"
echo "  BENCH_PORT:          $BENCH_PORT"
echo "  FAKE_LITELLM_PORT:   $FAKE_LITELLM_PORT"
echo ""

mkdir -p "${REPO_ROOT}/benchmarks/results"

# ---------------------------------------------------------------------------
# Step 1: Start fake LiteLLM on the host
# ---------------------------------------------------------------------------
echo "Step 1: Starting fake LiteLLM on port ${FAKE_LITELLM_PORT} …"

# Build a tiny wrapper that exports the app so uvicorn can import it
FAKE_LITELLM_LOG="${REPO_ROOT}/benchmarks/results/fake-litellm-${TIMESTAMP}.log"

"$PYTHON" -m uvicorn \
    "tests.fakes.fake_litellm:make_fake_litellm_app" \
    --factory \
    --host "0.0.0.0" \
    --port "${FAKE_LITELLM_PORT}" \
    --log-level warning \
    >"$FAKE_LITELLM_LOG" 2>&1 &
FAKE_LITELLM_PID=$!
echo "  fake LiteLLM PID: $FAKE_LITELLM_PID (log: $FAKE_LITELLM_LOG)"

_wait_url "http://127.0.0.1:${FAKE_LITELLM_PORT}/health" 15

# ---------------------------------------------------------------------------
# Step 2: Start the scheduler
# ---------------------------------------------------------------------------
echo ""
echo "Step 2: Starting scheduler …"

SCHEDULER_LOG="${REPO_ROOT}/benchmarks/results/scheduler-${TIMESTAMP}.log"

# Try Docker first; fall back to in-process
if docker compose version &>/dev/null 2>&1; then
    echo "  Using docker compose (project=${COMPOSE_PROJECT})"
    USING_DOCKER=true

    # Override the LiteLLM URL in xeon.yaml to point at host fake
    # We do this by writing a bench-specific config overlay
    BENCH_CONFIG="${REPO_ROOT}/benchmarks/bench-config.yaml"
    cat >"$BENCH_CONFIG" <<YAML
scheduler:
  redis_url: "redis://admission-scheduler-redis:6379/0"
  backend_litellm_url: "http://host.docker.internal:${FAKE_LITELLM_PORT}"
  log_dir: "/tmp/bench-scheduler-logs"

defaults:
  max_concurrency: 3
  max_queue_depth: 50
  max_wait_seconds: 120
  sync_timeout_seconds: 120
  priority: 5

models:
  code:
    max_concurrency: 2
    max_queue_depth: 20
    max_wait_seconds: 180
    sync_timeout_seconds: 300
    priority: 3
  chat-fast:
    max_concurrency: 4
    max_queue_depth: 40
    max_wait_seconds: 60
    sync_timeout_seconds: 120
    priority: 5
  brain-exec:
    max_concurrency: 1
    max_queue_depth: 5
    max_wait_seconds: 300
    sync_timeout_seconds: 600
    priority: 3
  tool-worker:
    max_concurrency: 6
    max_queue_depth: 100
    max_wait_seconds: 30
    sync_timeout_seconds: 60
    priority: 5
YAML

    # Write the standalone bench compose (avoids /mnt/talki-logs host-path bind conflict)
    BENCH_STANDALONE_COMPOSE="${REPO_ROOT}/benchmarks/docker-compose.bench-standalone.yml"
    cat >"$BENCH_STANDALONE_COMPOSE" <<COMPOSE
services:
  admission-scheduler-redis:
    image: redis:7-alpine
    restart: "no"
    networks:
      - bench-net
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 5
      start_period: 5s

  admission-scheduler:
    build:
      context: ${REPO_ROOT}
      dockerfile: Dockerfile
    restart: "no"
    ports:
      - "127.0.0.1:${BENCH_PORT}:4001"
    depends_on:
      admission-scheduler-redis:
        condition: service_healthy
    environment:
      SCHEDULER_CONFIG: /app/config/bench.yaml
      SCHEDULER_LOG_DIR: /tmp/scheduler-logs
    volumes:
      - ${BENCH_CONFIG}:/app/config/bench.yaml:ro
    extra_hosts:
      - "host.docker.internal:host-gateway"
    networks:
      - bench-net

networks:
  bench-net:
    driver: bridge
COMPOSE

    docker compose -p "$COMPOSE_PROJECT" \
        -f "$BENCH_STANDALONE_COMPOSE" \
        up -d --build \
        >"$SCHEDULER_LOG" 2>&1

    SCHEDULER_URL="http://localhost:${BENCH_PORT}"
    _wait_url "${SCHEDULER_URL}/health" 60

else
    echo "  docker compose not available — falling back to in-process scheduler"
    USING_DOCKER=false

    # Write a bench-specific config pointing at local fake LiteLLM
    BENCH_CONFIG="${REPO_ROOT}/benchmarks/bench-config.yaml"
    cat >"$BENCH_CONFIG" <<YAML
scheduler:
  redis_url: "redis://127.0.0.1:6379/0"
  backend_litellm_url: "http://127.0.0.1:${FAKE_LITELLM_PORT}"
  log_dir: "/tmp/bench-scheduler-logs"

defaults:
  max_concurrency: 3
  max_queue_depth: 50
  max_wait_seconds: 120
  sync_timeout_seconds: 120
  priority: 5

models:
  code:
    max_concurrency: 2
    max_queue_depth: 20
    max_wait_seconds: 180
    sync_timeout_seconds: 300
    priority: 3
  chat-fast:
    max_concurrency: 4
    max_queue_depth: 40
    max_wait_seconds: 60
    sync_timeout_seconds: 120
    priority: 5
  brain-exec:
    max_concurrency: 1
    max_queue_depth: 5
    max_wait_seconds: 300
    sync_timeout_seconds: 600
    priority: 3
  tool-worker:
    max_concurrency: 6
    max_queue_depth: 100
    max_wait_seconds: 30
    sync_timeout_seconds: 60
    priority: 5
YAML

    SCHEDULER_CONFIG="$BENCH_CONFIG" \
    "$PYTHON" -m uvicorn \
        "app.main:create_app" \
        --factory \
        --host "127.0.0.1" \
        --port "${BENCH_PORT}" \
        --log-level warning \
        >"$SCHEDULER_LOG" 2>&1 &
    SCHEDULER_IN_PROC_PID=$!
    echo "  in-process scheduler PID: $SCHEDULER_IN_PROC_PID"

    SCHEDULER_URL="http://127.0.0.1:${BENCH_PORT}"
    _wait_url "${SCHEDULER_URL}/health" 30
fi

echo ""
echo "  Scheduler: ${SCHEDULER_URL}"
echo ""

# ---------------------------------------------------------------------------
# Step 3: Run the benchmark
# ---------------------------------------------------------------------------
echo "Step 3: Running benchmark …"
echo ""

BENCH_CMD=(
    "$PYTHON"
    "${REPO_ROOT}/tests/benchmark/run_bench.py"
    --scheduler-url "$SCHEDULER_URL"
    --litellm-url "http://127.0.0.1:${FAKE_LITELLM_PORT}"
    --scenarios "$BENCH_SCENARIOS"
    --out "$OUT_JSON"
    --report-md "$OUT_MD"
)

# Pass through any extra CLI args (e.g. --scenarios override)
if [ "${#EXTRA_ARGS[@]}" -gt 0 ]; then
    BENCH_CMD+=("${EXTRA_ARGS[@]}")
fi

cd "$REPO_ROOT"
"${BENCH_CMD[@]}"

# ---------------------------------------------------------------------------
# Step 4: Report path
# ---------------------------------------------------------------------------
echo ""
echo "================================================"
echo "Benchmark complete."
echo "  JSON:   $OUT_JSON"
echo "  Report: $OUT_MD"
echo ""
echo "Preview:"
if command -v head &>/dev/null; then
    head -40 "$OUT_MD" 2>/dev/null || true
fi
