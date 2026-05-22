# LLM Observability

> A lightweight, event-driven inference logging and ingestion system for LLM applications — built entirely in **Python**.

Multi-provider chatbot · Streaming responses · PII redaction · Live metrics dashboard · Local model support via Ollama

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Architecture Overview](#architecture-overview)
3. [Services](#services)
4. [Setup Instructions](#setup-instructions)
  - [Docker Compose (recommended)](#option-a-docker-compose-recommended)
  - [Local Development](#option-b-local-development)
5. [Configuration](#configuration)
6. [SDK Usage](#sdk-usage)
7. [Schema Design Decisions](#schema-design-decisions)
8. [Tradeoffs Made](#tradeoffs-made)
9. [What I'd Improve with More Time](#what-id-improve-with-more-time)
10. [Bonus Features](#bonus-features-implemented)

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
│  Calls LLM providers via llm_obs SDK wrappers              │
└──────────────┬──────────────────────────────────────────────┘
               │ POST /v1/ingest/batch (async, fire-and-forget)
               ▼
┌─────────────────────────────────────────────────────────────┐
│           ingestion/  (FastAPI :4000)                       │
│                                                             │
│  1. Validate payload (Pydantic)                             │
│  2. Write to inference_events (audit row)                   │
│  3. Enqueue Celery job → Redis                              │
└──────────────┬──────────────────────────────────────────────┘
               │ Celery job via Redis broker
               ▼
┌─────────────────────────────────────────────────────────────┐
│           Celery Worker                                     │
│                                                             │
│  1. PII redact (regex + Luhn)                               │
│  2. Compute cost_usd from price table                       │
│  3. UPSERT inference_logs (idempotent by ULID)              │
│  4. PUBLISH to Redis channel "metrics.events"               │
└──────────────┬──────────────────────────────────────────────┘
               │ Redis Pub/Sub
               ▼
┌─────────────────────────────────────────────────────────────┐
│  GET /v1/stream (SSE)  →  Dashboard live feed               │
└─────────────────────────────────────────────────────────────┘

LLM Providers:  OpenAI · Anthropic · Google Gemini · Ollama (local)
Storage:        PostgreSQL 16 (all persistent data)
Queue/PubSub:   Redis 7
```

For detailed diagrams (sequence, ER, class, flow) see `[ARCHITECTURE.md](./ARCHITECTURE.md)`.

---

## Services


| Service     | Port | Tech             | Role                                        |
| ----------- | ---- | ---------------- | ------------------------------------------- |
| `web`       | 3000 | FastAPI + Jinja2 | Chat UI, dashboard, conversation management |
| `ingestion` | 4000 | FastAPI          | Receives SDK logs, enqueues Celery jobs     |
| `worker`    | —    | Celery           | Processes logs: PII redact, cost, DB write  |
| `postgres`  | 5432 | PostgreSQL 16    | All persistent storage                      |
| `redis`     | 6379 | Redis 7          | Celery broker + Pub/Sub                     |
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

# ── LLM Providers (set at least one) ─────────────────────────
OPENAI_API_KEY=
ANTHROPIC_API_KEY=
GOOGLE_API_KEY=

# ── Ollama (local, free, no key needed) ──────────────────────
# Local dev:       http://localhost:11434
# Docker Compose:  http://host.docker.internal:11434
OLLAMA_BASE_URL=

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

The `llm_obs` SDK wraps your LLM client to auto-capture inference metadata. Zero code changes to your LLM call logic.

### Auto-instrumentation

```python
from llm_obs import ObservabilityClient, wrap_openai
from openai import OpenAI

obs = ObservabilityClient(
    endpoint="http://localhost:4000",
    api_key="dev-key",
    environment="prod",
)

# Wrap once — every subsequent call is auto-logged
client = wrap_openai(OpenAI(), obs)

response = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[{"role": "user", "content": "Explain LLMs in one sentence."}],
)
```

The SDK captures: model, provider, latency, TTFT, token usage, cost, timestamps, status, errors, request/response previews, conversation ID.

### Streaming

```python
stream = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[{"role": "user", "content": "Tell me a story."}],
    stream=True,
)
# Streaming is auto-instrumented — TTFT is measured on first chunk
for chunk in stream:
    print(chunk.choices[0].delta.content or "", end="")
```

### Anthropic & Gemini

```python
from llm_obs import wrap_anthropic, wrap_gemini
from anthropic import Anthropic
import google.generativeai as genai

# Anthropic
anthropic_client = wrap_anthropic(Anthropic(), obs)

# Gemini
genai.configure(api_key="...")
gemini_model = wrap_gemini(genai.GenerativeModel("gemini-1.5-flash"), obs)
```

### Manual span (custom instrumentation)

```python
span = obs.start_span(
    provider="openai",
    model="gpt-4o-mini",
    request={"messages": [{"role": "user", "content": "Hello"}]},
    conversation_id="conv-123",
)

# ... do your LLM call manually ...

span.set_ttft(ms=210)
span.set_usage(prompt_tokens=42, completion_tokens=11)
span.end(status="success", finish_reason="stop", streamed=True)
```

---

## Schema Design Decisions

### `conversations`

Tracks chat sessions. Soft-delete via `status=ARCHIVED` keeps history intact without permanent deletion. `model` and `provider` columns store the last-used settings so resumed conversations default to the same configuration.

### `messages`

Two content columns: `content` (raw, used for LLM context window) and `content_redacted` (PII-scrubbed, used in dashboards). This allows accurate multi-turn conversations while keeping the observability layer privacy-safe. Setting `STORE_RAW_MESSAGES=false` can disable raw storage for stricter environments.

### `inference_logs`

The core table — one row per LLM call. Key design choices:

- **SDK-generated ULID as primary key** — allows the worker's `ON CONFLICT DO UPDATE` upsert to be idempotent, making Celery retries completely safe
- `**request_payload` + `response_payload` as JSONB** — schema-free, handles evolving LLM API response shapes without migrations
- `**input_preview` / `output_preview`** (256 chars, PII-redacted) — fast dashboard display without loading full payloads
- `**input_hash**` (sha256 of request) — enables duplicate detection and caching analysis
- `**ttft_ms**` (time-to-first-token) — critical for streaming UX quality measurement, stored separately from total `latency_ms`
- `**cost_usd**` computed in the worker — keeps the hot path lean; price table is easily updated

### `inference_events`

Audit/replay buffer. Every raw SDK payload is written here *before* the Celery job runs. If the worker has a bug and corrupts data, you can:

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
| **PII detection**      | Custom regex + Luhn                        | Microsoft Presidio, AWS Comprehend | Zero external services; deterministic; no data leaves the system; fast                |
| **UI framework**       | Jinja2 + Tailwind CDN                      | React/Next.js                      | Python-only project; no Node.js or build pipeline needed                              |
| **Idempotency**        | ULID primary key + `ON CONFLICT DO UPDATE` | Deduplication table                | Simpler; ULID is already unique per call; safe for unlimited retries                  |
| **Multi-turn context** | Last 20 messages from DB                   | Full history / vector search       | Keeps prompt size predictable; avoids context overflow for POC                        |
| **Cost computation**   | Hardcoded price table in worker            | External pricing API               | Eliminates a network dependency; easily updated; transparent                          |
| **Local LLMs**         | Ollama (OpenAI-compatible `/v1`)           | llama.cpp directly                 | Reuses existing OpenAI client code; Ollama manages model downloads and GPU            |


---

## What I'd Improve with More Time

**Infrastructure & Reliability**

1. **Committed Alembic migrations** — auto-generate the initial migration file so schema is versioned and reproducible across environments
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
| ✅ PII redaction                           | Regex + Luhn validation; runs in worker; `pii_detections` stored per log |
| ✅ Docker Compose one-command              | `docker compose up --build` starts all 6 services                        |
| ✅ Cancel a conversation                   | `asyncio.Event` registry; cancel button stops LLM stream mid-response    |
| ✅ List conversations                      | Sidebar with all active conversations, sorted by last activity           |
| ✅ Resume a conversation                   | Full message history loaded; multi-turn context preserved                |
| ✅ Local model support                     | Ollama integration; auto-discovers pulled models via `/api/tags`         |
| ✅ Markdown rendering                      | `marked.js` renders LLM responses with proper formatting                 |


