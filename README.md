# LLM Observability

> A lightweight, event-driven inference logging and ingestion system for LLM applications — built entirely in **Python**.

Multi-provider chatbot · Streaming responses · PII redaction · Live metrics dashboard · Local model support via Ollama

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Architecture Overview](#architecture-overview)
3. [Data Flow Cheat Sheet](#data-flow-cheat-sheet)
4. [Services](#services)
5. [Setup Instructions](#setup-instructions)
  - [Docker Compose (recommended)](#option-a-docker-compose-recommended)
  - [Local Development](#option-b-local-development)
6. [Configuration](#configuration)
7. [SDK Usage](#sdk-usage)
8. [Schema Design Decisions](#schema-design-decisions)
9. [Tradeoffs Made](#tradeoffs-made)
10. [What I'd Improve with More Time](#what-id-improve-with-more-time)
11. [Bonus Features](#bonus-features-implemented)

---

## Quick Start

```bash
git clone <repo-url>
cd llm-observability

cp .env.example .env
# Edit .env — add at least one of:
#   OPENAI_API_KEY, ANTHROPIC_API_KEY, GOOGLE_API_KEY
# OR set OLLAMA_BASE_URL for free local models (no API key needed)

docker compose up --build
```


| URL                                                                | What                   |
| ------------------------------------------------------------------ | ---------------------- |
| [http://localhost:3000/chat](http://localhost:3000/chat)           | Chat interface         |
| [http://localhost:3000/dashboard](http://localhost:3000/dashboard) | Metrics dashboard      |
| [http://localhost:3000/logs](http://localhost:3000/logs)           | Inference log explorer |
| [http://localhost:8080](http://localhost:8080)                     | Adminer (DB viewer)    |


Seed 200 synthetic logs for an instant dashboard demo:

```bash
docker compose exec ingestion python -m app.seed
```

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                        Browser                              │
└──────────────┬──────────────────────────────────────────────┘
               │ HTTP + SSE
               ▼
┌─────────────────────────────────────────────────────────────┐
│              web/  (FastAPI :3000)                          │
│                                                             │
│  /chat        → SSE streaming chat UI                       │
│  /dashboard   → Live metrics + charts                       │
│  /logs        → Inference log explorer                      │
│                                                             │
│  Calls LLM via llm_obs SDK (stream_chat + auto_instrument)  │
└──────────────┬──────────────────────────────────────────────┘
               │ POST /v1/ingest/batch (async, fire-and-forget)
               ▼
┌─────────────────────────────────────────────────────────────┐
│           ingestion/  (FastAPI :4000)                       │
│                                                             │
│  1. Validate payload (Pydantic)                             │
│  2. Write to inference_events (audit row, Postgres)         │
│  3. Enqueue Celery job → Redis DB 1 (broker)                │
└──────────────┬──────────────────────────────────────────────┘
               │ Celery worker consumes from Redis DB 1
               ▼
┌─────────────────────────────────────────────────────────────┐
│           Celery Worker                                     │
│                                                             │
│  1. Derive previews + input_hash from payload               │
│  2. UPSERT inference_logs (idempotent by span id)           │
│  3. UPDATE inference_events → PROCESSED                     │
│  4. PUBLISH to Redis DB 0 channel "metrics.events"          │
└──────────────┬──────────────────────────────────────────────┘
               │ Redis Pub/Sub
               ▼
┌─────────────────────────────────────────────────────────────┐
│  GET /v1/stream (SSE)  →  Dashboard live feed               │
└─────────────────────────────────────────────────────────────┘

LLM Providers:  OpenAI · Anthropic · Google Gemini · AWS Bedrock · Ollama · any OpenAI-compatible URL
Storage:        PostgreSQL 16 (conversations, messages, inference_logs, inference_events)
Queue:          Redis DB 1 (Celery broker — temporary task messages)
Pub/Sub:        Redis DB 0 (live dashboard SSE feed)
PII + cost:     Computed in llm_obs SDK before HTTP ingest and before LLM calls
```

For detailed diagrams (sequence, ER, class, flow) see [ARCHITECTURE.md](./ARCHITECTURE.md).

---

## Data Flow Cheat Sheet

One chat message, end to end:

```text
Browser                web/ (:3000)              SDK (llm_obs)           ingestion/ (:4000)       worker              dashboard
   │                        │                        │                        │                    │                    │
   │ POST /api/chat (SSE)   │                        │                        │                    │                    │
   ├───────────────────────►│                        │                        │                    │                    │
   │                        │ INSERT messages        │                        │                    │                    │
   │                        │ (raw content, Postgres)│                        │                    │                    │
   │                        │                        │                        │                    │                    │
   │                        │ set_obs_context(conv)  │                        │                    │                    │
   │                        │ stream_chat(...)       │                        │                    │                    │
   │                        ├───────────────────────►│ sanitize_messages      │                    │                    │
   │                        │                        │ (PII before LLM)       │                    │                    │
   │                        │                        │                        │                    │                    │
   │                        │                        │ auto_instrument span   │                    │                    │
   │                        │                        │ → LLM provider         │                    │                    │
   │◄── SSE tokens ─────────┤◄── stream chunks ──────┤                        │                    │                    │
   │                        │                        │ span: TTFT, usage, out  │                    │                    │
   │                        │                        │                        │                    │                    │
   │                        │ INSERT assistant msg   │ span.end()             │                    │                    │
   │                        │                        │ → compute_cost()       │                    │                    │
   │                        │                        │ → client.log()         │                    │                    │
   │                        │                        │   (PII before HTTP)    │                    │                    │
   │                        │                        │ → BatchTransport queue │                    │                    │
   │                        │                        │                        │                    │                    │
   │                        │                        │ POST /v1/ingest/batch  │                    │                    │
   │                        │                        ├───────────────────────►│ INSERT inference_events (Postgres)
   │                        │                        │                        │ Celery.delay() → Redis DB 1
   │                        │                        │◄── 202 Accepted ───────┤                    │
   │                        │                        │                        │                    │                    │
   │                        │                        │                        │ consume job        │                    │
   │                        │                        │                        ├───────────────────►│ UPSERT inference_logs
   │                        │                        │                        │                    │ UPDATE events→PROCESSED
   │                        │                        │                        │                    │ PUBLISH metrics.events
   │                        │                        │                        │                    │ (Redis DB 0)
   │                        │                        │                        │                    │                    │
   │                        │ GET /api/dashboard/stream (SSE proxy)            │                    │                    │
   │◄── live log event ─────┤◄─────────────────────────────────────────────────┤ SUBSCRIBE ─────────┤                    │
   │                        │                        │                        │                    │                    │
   │ GET /logs              │ GET /api/dashboard/logs (proxy)                  │                    │                    │
   ├───────────────────────►├─────────────────────────────────────────────────►│ SELECT inference_logs
```

### At each step — what gets stored where

| Step | Component | Writes to | Notes |
| ---- | --------- | --------- | ----- |
| 1 | `web/routers/chat.py` | `messages`, `conversations` | Raw chat text for multi-turn context |
| 2 | `guard.sanitize_messages_for_llm` | — (in memory) | PII scrubbed before LLM sees messages |
| 3 | `InferenceSpan` (auto_instrument) | — (in memory) | Records latency, TTFT, tokens, output |
| 4 | `span.end()` → `client.log()` | SDK in-memory queue | Adds `cost_usd`, redacts request/response |
| 5 | `BatchTransport` | — → HTTP | Flushes every 2s or 20 events |
| 6 | `ingestion/routers/ingest.py` | `inference_events` | Durable copy; status `RECEIVED` |
| 6 | `process_inference_log.delay()` | Redis DB 1 | Temporary Celery job until consumed |
| 7 | Celery worker | `inference_logs` | Queryable observability rows |
| 7 | Celery worker | Redis DB 0 pub/sub | Live dashboard feed |
| 8 | Dashboard UI | — (reads only) | `/logs` → `inference_logs`; stream → Redis |

### Key IDs

| ID | Generated by | Used for |
| -- | ------------ | -------- |
| `conversation_id` | Web (UUID) | Links chat thread ↔ logs via `set_obs_context()` |
| `InferenceSpan.id` | SDK (`new_id()`) | Primary key in `inference_logs` |
| `inference_events.id` | Ingestion (UUID) | Internal queue/audit row — not the log id |

### Timing

- **Chat tokens** — real-time (SSE, synchronous with LLM)
- **Ingest HTTP** — async, batched (≤2s delay typical)
- **Worker → DB** — async (Celery, usually sub-second after ingest)
- **Dashboard live feed** — near real-time via Redis pub/sub after worker runs

### If something breaks

| Symptom | Likely cause | Check |
| ------- | ------------ | ----- |
| Chat works, no logs in dashboard | Worker not running | `docker compose ps worker` |
| Logs in `inference_events` stuck at `RECEIVED` | Celery/Redis issue | Redis DB 1, worker logs |
| Logs missing entirely | Ingest down or SDK transport failed | `INGEST_URL`, ingestion :4000 |
| Logs exist but no `conversation_id` | `set_obs_context()` not called | `web/app/routers/chat.py` |

---

## Services


| Service     | Port | Tech             | Role                                        |
| ----------- | ---- | ---------------- | ------------------------------------------- |
| `web`       | 3000 | FastAPI + Jinja2 | Chat UI, dashboard, conversation management |
| `ingestion` | 4000 | FastAPI          | Receives SDK logs, enqueues Celery jobs     |
| `worker`    | —    | Celery           | Consumes Redis queue; writes `inference_logs`; publishes live metrics |
| `postgres`  | 5432 | PostgreSQL 16    | All persistent storage                      |
| `redis`     | 6379 | Redis 7          | DB 0: pub/sub · DB 1: Celery broker · DB 2: result backend |
| `adminer`   | 8080 | Adminer          | DB admin UI                                 |


---

## Setup Instructions

### Option A: Docker Compose (recommended)

**Prerequisites:** Docker Desktop

```bash
# 1. Clone and configure
cp .env.example .env

# 2. Set at least one LLM provider in .env:
#
#   Cloud providers (paid):
#     OPENAI_API_KEY=sk-...
#     ANTHROPIC_API_KEY=sk-ant-...
#     GOOGLE_API_KEY=AIza...
#
#   Local models — FREE, no API key:
#     Install Ollama: https://ollama.com
#     Pull a model:   ollama pull gemma3:4b
#     Then set:       OLLAMA_BASE_URL=http://host.docker.internal:11434

# 3. Start everything
docker compose up --build

# 4. (Optional) Seed demo data for the dashboard
docker compose exec ingestion python -m app.seed
```

**Stopping:**

```bash
docker compose down          # stop containers
docker compose down -v       # stop + wipe database volumes
```

---

### Option B: Local Development

**Prerequisites:** Python 3.12+, PostgreSQL 16, Redis 7

```bash
# 1. Start infrastructure only
docker compose -f docker-compose.dev.yml up -d
# This starts postgres :5432, redis :6379, adminer :8080

# 2. Install SDK
cd sdk && pip install -e . && cd ..

# 3. Install service dependencies
cd ingestion && pip install -r requirements.txt && cd ..
cd web && pip install -r requirements.txt && cd ..

# 4. Copy and configure env
cp .env.example .env
# Set DATABASE_URL, REDIS_URL, and at least one provider key
# For local dev, OLLAMA_BASE_URL=http://localhost:11434

# 5. Run database migrations
cd ingestion && alembic upgrade head && cd ..

# 6. Start services (3 separate terminals)

# Terminal 1 — Ingestion API
cd ingestion && uvicorn app.main:app --port 4000 --reload

# Terminal 2 — Celery worker
cd ingestion && celery -A app.worker worker -l info

# Terminal 3 — Web
cd web && uvicorn app.main:app --port 3000 --reload
```

---

## Configuration

All configuration is via environment variables in `.env`:

```bash
# ── Database ──────────────────────────────────────────────────
DATABASE_URL=postgresql+asyncpg://obs:obs@localhost:5432/obs

# ── Redis ─────────────────────────────────────────────────────
REDIS_URL=redis://localhost:6379/0
CELERY_BROKER_URL=redis://localhost:6379/1
CELERY_RESULT_BACKEND=redis://localhost:6379/2

# ── Ingestion ─────────────────────────────────────────────────
INGEST_URL=http://localhost:4000       # web → ingestion URL
INGEST_API_KEY=dev-key                 # simple auth header

# ── LLM Endpoints (recommended) ──────────────────────────────
# Comma-separated URLs — SDK probes each for provider + models
#   http://localhost:11434
#   ollama://http://host.docker.internal:11434
#   http://10.0.1.5:8080|my-api-key
LLM_ENDPOINTS=

# ── LLM Providers (legacy — used when LLM_ENDPOINTS is empty) ─
OPENAI_API_KEY=
ANTHROPIC_API_KEY=
GOOGLE_API_KEY=

# ── Ollama (legacy) ──────────────────────────────────────────
# Local dev:       http://localhost:11434
# Docker Compose:  http://host.docker.internal:11434
OLLAMA_BASE_URL=

# ── Probe fallback ───────────────────────────────────────────
LLM_DEFAULT_MODEL=          # used when URL probe finds no models
LLM_PROBE_TIMEOUT=5

# ── App ───────────────────────────────────────────────────────
ENVIRONMENT=dev
```

**Ollama quick start:**

```bash
# Install Ollama from https://ollama.com, then:
ollama pull gemma3:4b       # ~3GB, good balance of speed/quality
ollama pull llama3.2:1b     # ~1GB, very fast
ollama pull mistral         # ~4GB, strong reasoning
```

---

## SDK Usage

The `llm_obs` SDK auto-captures inference metadata. The web app uses two SDK entry points:

1. **`ObservabilityClient.auto_instrument()`** — patches provider SDKs; creates an `InferenceSpan` per call
2. **`stream_chat()`** — unified streaming chat; probes URLs from env; redacts PII before the LLM call

### Auto-instrumentation (startup)

```python
from llm_obs import ObservabilityClient

obs = ObservabilityClient(
    endpoint="http://localhost:4000",
    api_key="dev-key",
    environment="dev",
    redact_pii=True,
)
obs.auto_instrument()   # patches openai / anthropic / gemini / bedrock
```

### Streaming chat (web handler)

```python
from llm_obs import stream_chat, set_obs_context

set_obs_context(conversation_id=str(conv.id))   # links logs to conversation

async for chunk in stream_chat(
    provider="ollama",
    model="gemma3:4b",
    messages=[{"role": "user", "content": "Hello"}],
):
    print(chunk, end="")
```

### URL and model discovery

```python
from llm_obs import available_providers

# Reads LLM_ENDPOINTS / OLLAMA_BASE_URL / API keys from env
# Ollama:     GET /api/tags  → model names
# vLLM etc.:  GET /v1/models → model ids
providers = available_providers()
# → {"ollama": ["gemma3:4b", "llama3.2"]}
```

### What happens on each LLM call

```
patched provider call
  → InferenceSpan.start (captures request, conversation_id from contextvars)
  → stream tokens → span.set_ttft(), span.append_output(), span.set_usage()
  → span.end() → builds payload, compute_cost(), ObservabilityClient.log()
  → PII redact in SDK → BatchTransport → POST /v1/ingest/batch
```

### Manual span (custom instrumentation)

```python
span = obs.start_span(
    provider="openai",
    model="gpt-4o-mini",
    request={"messages": [{"role": "user", "content": "Hello"}]},
    conversation_id="conv-123",
)
span.set_ttft(ms=210)
span.set_usage(prompt_tokens=42, completion_tokens=11)
span.end(status="success", streamed=True)
```

---

## Schema Design Decisions

### `conversations`

Tracks chat sessions. Soft-delete via `status=ARCHIVED` keeps history intact without permanent deletion. `model` and `provider` columns store the last-used settings so resumed conversations default to the same configuration.

### `messages`

Stores chat history for multi-turn context. Single `content` column (raw text). PII is **not** duplicated here — privacy for observability is handled in `inference_logs` (SDK redacts before ingest) and before LLM calls (`sanitize_messages_for_llm` in `stream_chat`).

### `inference_logs`

The core table — one row per LLM call. Key design choices:

- **SDK-generated id as primary key** — allows the worker's `ON CONFLICT DO UPDATE` upsert to be idempotent, making Celery retries safe
- **`request_payload` + `response_payload` as JSONB** — PII-redacted in the SDK before HTTP ingest
- **`input_preview` / `output_preview`** (256 chars) — derived by worker for fast dashboard display
- **`ttft_ms`** (time-to-first-token) — measured by `InferenceSpan` on first streaming chunk
- **`cost_usd`** — computed in the SDK (`span.end()` → `compute_cost()`), stored by worker as-is
- **`pii_detections`** — attached by SDK during `ObservabilityClient.log()`

### `inference_events`

Durable audit/replay buffer in **Postgres**. Every payload is written here *before* the Celery job runs. The Celery job itself lives temporarily in **Redis DB 1** until a worker consumes it.

```sql
UPDATE inference_events SET status = 'RECEIVED', error = NULL
WHERE status = 'FAILED';
-- Then re-enqueue jobs manually
```

Status lifecycle: `RECEIVED → PROCESSED` (success) or `RECEIVED → FAILED` (all retries exhausted).

### `metric_rollups`

Pre-aggregated 1-minute buckets keyed by `(bucket, provider, model)`. Dashboard time-series queries for 7d+ ranges hit this table instead of scanning all of `inference_logs`. Falls back to live aggregation for recent minutes not yet rolled up. The PK design means upserts are safe and idempotent.

---

## Tradeoffs Made


| Decision               | Chosen                                     | Alternative                        | Reason                                                                                |
| ---------------------- | ------------------------------------------ | ---------------------------------- | ------------------------------------------------------------------------------------- |
| **Queue**              | Celery + Redis                             | Kafka, RabbitMQ                    | Celery is operationally trivial; Redis already present; sufficient for POC throughput |
| **Analytics DB**       | PostgreSQL only                            | ClickHouse, TimescaleDB            | JSONB + percentile functions handle POC volume; avoids another infra dependency       |
| **Streaming protocol** | SSE (Server-Sent Events)                   | WebSockets                         | SSE is unidirectional, works behind every HTTP proxy, simpler to implement            |
| **PII detection**      | Custom regex + Luhn in SDK                 | Microsoft Presidio, AWS Comprehend | Redact before LLM call and before HTTP ingest; deterministic, no external service   |
| **UI framework**       | Jinja2 + Tailwind CDN                      | React/Next.js                      | Python-only project; no Node.js or build pipeline needed                              |
| **Idempotency**        | SDK id + `ON CONFLICT DO UPDATE`           | Deduplication table                | Unique per call; safe for unlimited Celery retries                                    |
| **Multi-turn context** | Last 20 messages from DB                   | Full history / vector search       | Keeps prompt size predictable; avoids context overflow for POC                        |
| **Cost computation**   | SDK `compute_cost()` in `span.end()`       | External pricing API               | Cost ships with payload; worker is a simple writer                                    |
| **Local LLMs**         | Ollama (OpenAI-compatible `/v1`)           | llama.cpp directly                 | Reuses existing OpenAI client code; Ollama manages model downloads and GPU            |


---

## What I'd Improve with More Time

**Infrastructure & Reliability**

1. **Committed Alembic migrations** — expand versioned migration history as schema evolves
2. **Celery Beat rollup job** — periodic task every 60s to materialize `metric_rollups` instead of live aggregation, making 7d+ queries instant
3. **Kafka instead of Redis queue** — persistent message log enables true replay without the `inference_events` workaround

**Features**
4. **Authentication** — multi-tenant API key auth per user; conversation isolation
5. **Full-text log search** — `pg_trgm` GIN index on `input_preview`/`output_preview` for fast keyword search in the log explorer
6. **Named entity PII** — integrate `spaCy` NER for person names and organisation detection (currently disabled due to false-positive rate)
7. **Model-generated conversation titles** — after first exchange, ask the LLM to generate a short title instead of truncating the first message
8. **Token streaming accuracy** — OpenAI `stream_options: {include_usage: true}` is used but Anthropic token counts come at stream end; unify the timing

**Operations**
9. **Kubernetes manifests** — `Deployment`, `Service`, `HorizontalPodAutoscaler` YAML for the 3 stateless app containers
10. **Cost table in DB** — make the price table editable at runtime via a simple admin endpoint, not hardcoded
11. **Playwright e2e test** — automated happy-path: send message → verify it appears in dashboard live feed within 5 seconds

---

## Bonus Features Implemented


| Feature                                   | How                                                                      |
| ----------------------------------------- | ------------------------------------------------------------------------ |
| ✅ Multi-provider support                  | OpenAI, Anthropic, Google Gemini, Ollama — switchable per chat message   |
| ✅ Streaming responses                     | SSE end-to-end: LLM → FastAPI → Browser, with TTFT measurement           |
| ✅ Live dashboard                          | Redis Pub/Sub → SSE → Chart.js charts auto-update as logs arrive         |
| ✅ Latency / Throughput / Error dashboards | p50/p95/p99 latency, requests over time, error breakdown by type         |
| ✅ Event-based architecture                | Celery + Redis fully decouples ingestion from log processing             |
| ✅ PII redaction                           | SDK: before LLM (`guard.py`) + before ingest (`client.log()`); `pii_detections` on each log |
| ✅ URL-based provider discovery            | `discovery.py` probes Ollama `/api/tags`, OpenAI-compat `/v1/models`, Bedrock API |
| ✅ Docker Compose one-command              | `docker compose up --build` starts all 6 services                        |
| ✅ Cancel a conversation                   | `asyncio.Event` registry; cancel button stops LLM stream mid-response    |
| ✅ List conversations                      | Sidebar with all active conversations, sorted by last activity           |
| ✅ Resume a conversation                   | Full message history loaded; multi-turn context preserved                |
| ✅ Local model support                     | Ollama integration; auto-discovers pulled models via `/api/tags`         |
| ✅ Markdown rendering                      | `marked.js` renders LLM responses with proper formatting                 |


