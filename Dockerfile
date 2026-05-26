# =============================================================================
# Stage 1: builder — install Python deps from pyproject.toml
# =============================================================================
FROM python:3.12-slim AS builder

# gcc needed for any C-extension wheels (httptools, uvloop); slim has none by default
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy only the packaging manifest first — layer-cache friendly
COPY pyproject.toml ./

# Create a minimal stub so `pip install .` can resolve the package without
# needing the full source tree at this point
RUN mkdir -p app && touch app/__init__.py

# Install into --user prefix so Stage 2 can copy a single directory
RUN pip install --no-cache-dir --user .

# =============================================================================
# Stage 2: runtime — lean final image
# =============================================================================
FROM python:3.12-slim AS runtime

# ---- system deps for healthcheck ----
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# ---- environment ----
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    SCHEDULER_CONFIG=/app/config/default.yaml \
    PYTHONPATH=/app

# ---- non-root user (UID 10001 matches log-dir chown in runbook) ----
RUN groupadd --gid 10001 scheduler && \
    useradd --uid 10001 --gid scheduler --no-create-home --shell /bin/false scheduler

# ---- copy site-packages from builder ----
COPY --from=builder /root/.local /home/scheduler/.local
ENV PATH=/home/scheduler/.local/bin:$PATH

# ---- copy application source ----
WORKDIR /app
COPY app/ ./app/
COPY config/ ./config/

# ---- log dir (owned by scheduler; may be shadowed by a host volume) ----
RUN mkdir -p /var/log/scheduler && chown scheduler:scheduler /var/log/scheduler

USER scheduler

EXPOSE 4001

# ---- healthcheck ----
# interval/timeout/retries chosen for a ~20 s warm-up budget.
# FastAPI /health returns 200 {"status": "ok"} once Redis is reachable.
HEALTHCHECK --interval=30s --timeout=5s --retries=3 --start-period=20s \
    CMD curl -fsS http://127.0.0.1:4001/health || exit 1

# ---- CMD ----
# SINGLE WORKER IS INTENTIONAL.
# Dispatcher coroutines coordinate exclusively through Redis (slot counters,
# ZSETs, Lua scripts). Running N workers would spawn N dispatchers per model,
# leading to N× read-then-write races on the queue ZSETs that the Lua EVAL
# path is NOT designed to de-duplicate across OS processes.  One worker;
# asyncio handles concurrency within that process.
CMD ["uvicorn", "app.main:app", \
     "--host", "0.0.0.0", \
     "--port", "4001", \
     "--workers", "1", \
     "--loop", "uvloop", \
     "--http", "httptools", \
     "--no-access-log"]
