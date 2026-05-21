# LLM Observability — Inference Logging System

A lightweight, event-driven inference logging and ingestion system for LLM applications. Built in Python.

---

## Architecture Overview

```
Browser
  │
  ▼
web/ (FastAPI :3000)
  ├── /chat          ← SSE streaming chat (OpenAI / Anthropic / Gemini)
  ├── /dashboard     ← Metrics dashboard (live + historical)
  └── /logs          ← Log explorer
  │
  └── llm_obs SDK ──► POST /v1/ingest ──► ingestion/ (FastAPI :4000)
                                                │
                                          InferenceEvent (Postgres)
                                          + Celery job → Redis queue
                                                │
                                          worker/ (Celery)
                                          ├── PII redaction
                                          ├── Cost computation
                                          ├── Upsert inference_logs
                                          └── Redis Pub/Sub ──► live dashboard SSE
```

## Services

| Service | Port | Description |
|---|---|---|
| `web` | 3000 | Chat UI + dashboard frontend |
| `ingestion` | 4000 | Receives logs, queues Celery jobs |
| `worker` | — | Processes logs (Celery) |
| `postgres` | 5432 | All persistent storage |
| `redis` | 6379 | Celery broker + pub/sub |
| `adminer` | 8080 | DB admin UI |

---

## One-Command Setup

```bash
cp .env.example .env
# Add at least one provider key in .env (OPENAI_API_KEY, ANTHROPIC_API_KEY, or GOOGLE_API_KEY)

docker compose up --build
```

Then open:
- **Chat:** http://localhost:3000/chat
- **Dashboard:** http://localhost:3000/dashboard
- **Logs:** http://localhost:3000/logs
- **Adminer:** http://localhost:8080 (server: postgres, user: obs, pass: obs, db: obs)

To seed demo data (200 synthetic logs for instant dashboard):
```bash
docker compose exec ingestion python -m app.seed
```

---

## Local Development

```bash
# Start only infrastructure
make dev

# Install dependencies
cd sdk       && pip install -e .
cd ingestion && pip install -r requirements.txt
cd web       && pip install -r requirements.txt

# Run services (3 terminals)
cd ingestion && uvicorn app.main:app --port 4000 --reload
cd ingestion && celery -A app.worker worker -l info
cd web       && uvicorn app.main:app --port 3000 --reload
```

---

## SDK Usage

```python
from llm_obs import ObservabilityClient, wrap_openai
from openai import OpenAI

obs = ObservabilityClient(endpoint="http://localhost:4000", api_key="dev-key")
client = wrap_openai(OpenAI(), obs)

# Every call is now auto-logged
response = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[{"role": "user", "content": "Hello!"}],
)
```

### Manual span

```python
span = obs.start_span(provider="openai", model="gpt-4o-mini", request={...})
span.set_ttft()
span.set_usage(prompt_tokens=42, completion_tokens=11)
span.end(status="success")
```

---

## Schema Design Decisions

### `conversations`
Holds chat sessions. `status` enum (ACTIVE / ARCHIVED) lets the UI filter without deleting.

### `messages`
Both `content` (raw) and `content_redacted` (PII-scrubbed) are stored. Raw is used for LLM context; dashboards read redacted.

### `inference_logs`
One row per LLM call. The `id` is SDK-generated (ULID-formatted) so the worker upsert is idempotent. Full `request_payload` and `response_payload` are stored as JSONB (post-redaction). Contains derived fields: `total_tokens`, `cost_usd`, `input_preview`, `output_preview`.

### `inference_events`
Audit/replay table. Every raw payload is stored here before the Celery job runs. If the worker crashes, events can be replayed. Status transitions: RECEIVED → PROCESSED or FAILED.

### `metric_rollups`
Pre-aggregated 1-minute buckets per (provider, model). Dashboard time-series queries use this for ranges > 1h; falls back to live aggregation from `inference_logs` for recent data.

---

## Tradeoffs Made

| Decision | Why |
|---|---|
| **Celery over Kafka** | Much simpler to run, same decoupling semantics. Kafka would be overkill for a POC. |
| **PostgreSQL only (no ClickHouse)** | JSONB + partial indexes are sufficient for POC volume. Noted as a scale limitation. |
| **SSE over WebSockets** | Unidirectional, simpler protocol, works behind proxies. |
| **Regex PII over Presidio** | Zero external services; deterministic; easy to extend. |
| **Idempotent upsert** | SDK-generated IDs allow safe Celery retries. |
| **Jinja2 templates over React** | Zero JS build step; Python-only project. Tailwind via CDN. |
| **Single Postgres for everything** | Conversations + logs in same DB simplifies joins and ops. |

---

## What I'd Improve with More Time

1. **Alembic migrations** — auto-generate and commit the first migration so schema changes are tracked.
2. **Auth** — multi-tenant API key auth; user sessions.
3. **Streaming Anthropic cancel** — the current cancel checks an asyncio Event but Anthropic's sync SDK needs proper thread interruption.
4. **Metric rollup worker** — a periodic Celery beat task to materialize `metric_rollups` instead of live aggregation.
5. **Log search** — full-text search on `input_preview`/`output_preview` with pg_trgm.
6. **k8s manifests** — Deployment + Service YAML for the 3 app containers.
7. **Playwright e2e test** — happy path: send message → see it in dashboard live feed.
8. **Cost table** — make the price table configurable via DB, not code.

---

## Bonus Features Implemented

- **Multi-provider** — OpenAI, Anthropic, Google Gemini; switchable per-chat
- **Streaming responses** — SSE end-to-end (browser → web → LLM → browser)
- **Live dashboard** — Redis Pub/Sub → SSE live feed of processed events
- **Event-based architecture** — Celery + Redis decouples ingestion from processing
- **PII redaction** — regex + Luhn check; runs in worker before persisting
- **Cancel conversation** — asyncio Event registry; cancel button in chat UI
- **List / resume conversations** — sidebar with conversation history
- **Docker Compose** — `docker compose up --build` brings everything up

---

## Submission Notes

Send to: work@ollive.ai
- GitHub repo link
- Architecture notes (see `ARCHITECTURE.md`)
- Demo: http://localhost:3000 after `docker compose up --build`
