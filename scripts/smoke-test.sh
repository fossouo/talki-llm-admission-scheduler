#!/usr/bin/env bash
# smoke-test.sh — End-to-end smoke test for the admission scheduler.
# Usage:  ./scripts/smoke-test.sh [BASE_URL]
# Default BASE_URL: http://localhost:4001
#
# Requires: curl, jq
# Q-006: every curl call is wrapped in gtimeout/timeout (≤60 s).

set -euo pipefail

BASE_URL="${1:-http://localhost:4001}"
PASS=0
FAIL=0

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TO() {
  # Return gtimeout if available, else timeout (coreutils on Linux)
  command -v gtimeout 2>/dev/null || command -v timeout 2>/dev/null
}
TO=$(_TO)

log()  { echo "[smoke] $*"; }
ok()   { echo "[PASS] $1"; (( PASS++ )); }
fail() { echo "[FAIL] $1"; (( FAIL++ )); }

curl_safe() {
  # Usage: curl_safe <timeout_s> <extra curl args…>
  local t=$1; shift
  $TO "$t" curl -fsS "$@"
}

assert_field() {
  # assert_field <json_string> <jq_expr> <expected_value>
  local json=$1 expr=$2 expected=$3
  local actual
  actual=$(echo "$json" | jq -r "$expr" 2>/dev/null || true)
  if [[ "$actual" == "$expected" ]]; then
    return 0
  else
    echo "    assert_field FAILED: $expr = '$actual' (expected '$expected')"
    return 1
  fi
}

# ---------------------------------------------------------------------------
# 1. GET /health — must return status=ok
# ---------------------------------------------------------------------------
log "--- TEST 1: GET /health ---"
HEALTH=$(curl_safe 10 "${BASE_URL}/health") || { fail "1 – /health unreachable"; HEALTH="{}"; }
if assert_field "$HEALTH" ".status" "ok"; then
  ok "1 – /health returns status=ok"
else
  fail "1 – /health.status != ok  (got: $HEALTH)"
fi

# ---------------------------------------------------------------------------
# 2. POST /v1/chat/completions (sync) — must return a choices array
# ---------------------------------------------------------------------------
log "--- TEST 2: POST /v1/chat/completions sync ---"
SYNC_RESP=$(curl_safe 60 -X POST "${BASE_URL}/v1/chat/completions" \
  -H "Content-Type: application/json" \
  -d '{"model":"chat-fast","messages":[{"role":"user","content":"ping"}],"max_tokens":16}') \
  || { fail "2 – sync request failed or timed out"; SYNC_RESP="{}"; }

if echo "$SYNC_RESP" | jq -e '.choices[0].message.content' > /dev/null 2>&1; then
  ok "2 – sync /v1/chat/completions returns choices[0].message.content"
else
  fail "2 – sync response missing choices[0].message.content  (got: $SYNC_RESP)"
fi

# ---------------------------------------------------------------------------
# 3. POST /jobs (async) — submit, poll until terminal, verify
# ---------------------------------------------------------------------------
log "--- TEST 3: POST /jobs async + poll ---"
ASYNC_SUBMIT=$(curl_safe 30 -X POST "${BASE_URL}/jobs" \
  -H "Content-Type: application/json" \
  -d '{"model":"chat-fast","messages":[{"role":"user","content":"ping async"}],"max_tokens":16}') \
  || { fail "3 – async submit failed"; ASYNC_SUBMIT="{}"; }

JOB_ID=$(echo "$ASYNC_SUBMIT" | jq -r '.job_id // empty' 2>/dev/null || true)
if [[ -z "$JOB_ID" ]]; then
  fail "3 – async submit did not return job_id  (got: $ASYNC_SUBMIT)"
else
  log "  job_id: $JOB_ID"
  TERMINAL=0
  for i in $(seq 1 30); do
    JOB_STATUS_RESP=$(curl_safe 10 "${BASE_URL}/jobs/${JOB_ID}") || { log "  poll $i: request error"; sleep 2; continue; }
    STATUS=$(echo "$JOB_STATUS_RESP" | jq -r '.status // empty' 2>/dev/null || true)
    log "  poll $i: $STATUS"
    case "$STATUS" in
      completed|failed|timeout|cancelled)
        TERMINAL=1
        break
        ;;
    esac
    sleep 2
  done
  if [[ $TERMINAL -eq 1 && "$STATUS" == "completed" ]]; then
    ok "3 – async job completed"
  elif [[ $TERMINAL -eq 1 ]]; then
    fail "3 – async job terminal but status=$STATUS (not completed)"
  else
    fail "3 – async job did not reach terminal state within 60 s"
  fi
fi

# ---------------------------------------------------------------------------
# 4. 10 concurrent sync requests — none must time out (expect all 200s)
# ---------------------------------------------------------------------------
log "--- TEST 4: 10 concurrent sync requests ---"
TMPDIR_CONC=$(mktemp -d)
trap 'rm -rf "$TMPDIR_CONC"' EXIT

for i in $(seq 1 10); do
  (
    OUT=$(curl_safe 60 -w "\n%{http_code}" -X POST "${BASE_URL}/v1/chat/completions" \
      -H "Content-Type: application/json" \
      -d "{\"model\":\"chat-fast\",\"messages\":[{\"role\":\"user\",\"content\":\"concurrent $i\"}],\"max_tokens\":8}" \
      2>/dev/null || echo "CURL_FAIL")
    HTTP_CODE=$(echo "$OUT" | tail -1)
    echo "$HTTP_CODE" > "${TMPDIR_CONC}/req_${i}.code"
  ) &
done
wait

CONC_PASS=0
CONC_FAIL=0
for i in $(seq 1 10); do
  CODE=$(cat "${TMPDIR_CONC}/req_${i}.code" 2>/dev/null || echo "missing")
  if [[ "$CODE" == "200" ]]; then
    (( CONC_PASS++ ))
  else
    (( CONC_FAIL++ ))
    log "  req $i: HTTP $CODE"
  fi
done

if [[ $CONC_FAIL -eq 0 ]]; then
  ok "4 – all 10 concurrent requests returned HTTP 200"
else
  fail "4 – $CONC_FAIL / 10 concurrent requests did not return 200"
fi

# ---------------------------------------------------------------------------
# 5. GET /queues and GET /metrics — dump to stdout
# ---------------------------------------------------------------------------
log "--- TEST 5: GET /queues ---"
QUEUES=$(curl_safe 10 "${BASE_URL}/queues") || { fail "5a – /queues unreachable"; QUEUES="{}"; }
if echo "$QUEUES" | jq . > /dev/null 2>&1; then
  ok "5a – /queues returns valid JSON"
  echo "$QUEUES" | jq .
else
  fail "5a – /queues returned non-JSON"
fi

log "--- TEST 5b: GET /metrics ---"
METRICS=$(curl_safe 10 "${BASE_URL}/metrics") || { fail "5b – /metrics unreachable"; METRICS=""; }
if echo "$METRICS" | grep -q "admission_requests_total"; then
  ok "5b – /metrics contains admission_requests_total"
else
  fail "5b – /metrics missing admission_requests_total"
fi
echo "$METRICS" | head -30

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "=============================="
echo " PASSED: $PASS"
echo " FAILED: $FAIL"
echo "=============================="

if [[ $FAIL -gt 0 ]]; then
  exit 1
fi
exit 0
