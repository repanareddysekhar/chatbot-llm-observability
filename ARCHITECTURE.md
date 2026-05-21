# LLM Observability & Inference Logging — Architecture

> **POC blueprint** for a lightweight, multi-provider LLM observability platform with a chatbot UI, a logging SDK, an ingestion pipeline, event-driven processing, PII redaction, and live dashboards.
>
> **Audience:** the implementation engineer building this end-to-end.
> **Goal:** pragmatic, impressive, completable by one developer.
> **Language:** TypeScript everywhere (Node 20+, React 19).

---

## 1. Executive Summary

We build a **TypeScript monorepo** containing:

1. A **Next.js 15** chatbot + dashboard web app.
2. A **Fastify** ingestion API.
3. A **BullMQ** background worker (event-driven post-processing).
4. A shared **`@llm-obs/sdk`** package that wraps OpenAI, Anthropic, and Google Generative AI clients and forwards inference logs to the ingestion API.
5. A shared **`@llm-obs/pii`** package for regex + heuristic PII redaction.
6. **PostgreSQL** for storage, **Redis** for queues + pub/sub.
7. **Docker Compose** for one-command setup.

The flow is:

```
  Browser ──► /api/chat (Next.js, SSE)
                │
                ├─ uses @llm-obs/sdk → calls OpenAI / Anthropic / Gemini (streaming)
                │       │
                │       └─► POST /v1/ingest  (Fastify)
                │                 │
                │                 ├─► persist raw event (postgres) + enqueue BullMQ job
                │                 │
                │                 └─► Redis Stream / BullMQ queue
                │                           │
                │                           ▼
                │                       Worker
                │                       ├─ PII redact full payload
                │                       ├─ extract derived metadata (cost, ttft, etc.)
                │                       ├─ upsert inference_logs row
                │                       └─ publish to Redis Pub/Sub channel "metrics"
                │
                ▼
  Browser ──► /api/dashboard/stream (SSE) ◄── subscribes to Redis Pub/Sub
```

---

## 2. Tech Stack Decisions

| Concern | Choice | Why |
|---|---|---|
| Language | **TypeScript 5.5+** (Node 20 LTS) | Type safety across SDK ↔ API ↔ UI. |
| Monorepo | **pnpm workspaces + Turborepo** | Fast, simple, great caching. |
| Web framework (UI) | **Next.js 15 (App Router)** + **React 19** | One app for chat + dashboard, SSE-friendly, RSC for tables. |
| Styling | **Tailwind CSS 4** + **shadcn/ui** + **lucide-react** | Modern, polished UI fast. |
| Charts | **Recharts** (+ small **visx** sparkline) | Declarative, lightweight, React-native. |
| Backend API | **Fastify 5** + **@fastify/sse-v2** + **@fastify/cors** | Fast, schema-first, native zod integration. |
| Validation | **Zod 3** (+ `zod-to-json-schema` for Fastify) | Single source of truth for shapes. |
| DB | **PostgreSQL 16** | JSONB + indexes give us "logs DB" feel without ELK. |
| ORM | **Prisma 5** | Best TS DX, easy migrations, generated client. |
| Queue | **BullMQ** (on Redis 7) | Robust, observable, retries, much lighter than Kafka. |
| Pub/Sub (live dashboard) | **Redis Pub/Sub** | Trivial fan-out to dashboard SSE consumers. |
| Streaming wire format | **Server-Sent Events (SSE)** | Simpler than WebSockets, fits unidirectional streams. |
| LLM SDKs | `openai@5`, `@anthropic-ai/sdk@0.30+`, `@google/generative-ai@0.21+` | Official SDKs, all stream natively. |
| PII | Regex + heuristics + Luhn (custom), optional `compromise` NER | Zero external services; predictable; fine for POC. |
| Auth | **None for POC** (single-user, dev). API key header (`x-obs-api-key`) on ingestion for hygiene. | Keep scope tight. |
| Lint/format | **Biome** (single tool) | Faster than ESLint+Prettier, one config. |
| Testing | **Vitest** + **Playwright** (one happy-path e2e) | Fast unit + a single visual e2e. |
| Container | **Docker Compose** | One-command stand up. |
| Process mgmt (dev) | `pnpm run dev` via Turborepo `--parallel` | Single dev command. |
| Telemetry of itself | **pino** structured logs | JSON logs in every service. |

---

## 3. Project Structure

```
llm-observability/
├── ARCHITECTURE.md                  ← this file
├── README.md
├── docker-compose.yml
├── docker-compose.dev.yml           ← optional: only infra (pg, redis)
├── .env.example
├── package.json
├── pnpm-workspace.yaml
├── turbo.json
├── biome.json
├── tsconfig.base.json
│
├── apps/
│   ├── web/                         ← Next.js 15 (chatbot + dashboard)
│   │   ├── package.json
│   │   ├── next.config.ts
│   │   ├── tailwind.config.ts
│   │   ├── postcss.config.mjs
│   │   ├── app/
│   │   │   ├── layout.tsx
│   │   │   ├── page.tsx                       ← redirects to /chat
│   │   │   ├── (chat)/
│   │   │   │   ├── layout.tsx                 ← sidebar w/ conv list
│   │   │   │   ├── chat/page.tsx              ← new chat
│   │   │   │   └── chat/[conversationId]/page.tsx
│   │   │   ├── dashboard/
│   │   │   │   ├── layout.tsx
│   │   │   │   ├── page.tsx                   ← overview
│   │   │   │   ├── logs/page.tsx              ← log explorer
│   │   │   │   └── logs/[id]/page.tsx         ← log detail
│   │   │   └── api/
│   │   │       ├── chat/route.ts              ← POST, SSE streaming
│   │   │       ├── conversations/route.ts     ← GET, POST
│   │   │       ├── conversations/[id]/route.ts
│   │   │       ├── conversations/[id]/cancel/route.ts
│   │   │       ├── conversations/[id]/messages/route.ts
│   │   │       └── dashboard/
│   │   │           ├── summary/route.ts
│   │   │           ├── timeseries/route.ts
│   │   │           ├── logs/route.ts
│   │   │           └── stream/route.ts        ← SSE live feed
│   │   ├── components/
│   │   │   ├── chat/
│   │   │   │   ├── ChatPage.tsx
│   │   │   │   ├── ConversationSidebar.tsx
│   │   │   │   ├── MessageList.tsx
│   │   │   │   ├── Message.tsx
│   │   │   │   ├── ChatInput.tsx
│   │   │   │   ├── ModelPicker.tsx
│   │   │   │   └── CancelButton.tsx
│   │   │   ├── dashboard/
│   │   │   │   ├── MetricCard.tsx
│   │   │   │   ├── LatencyChart.tsx
│   │   │   │   ├── ThroughputChart.tsx
│   │   │   │   ├── ErrorBreakdown.tsx
│   │   │   │   ├── TokenUsageChart.tsx
│   │   │   │   ├── TopModelsTable.tsx
│   │   │   │   ├── LiveEventFeed.tsx
│   │   │   │   └── LogsTable.tsx
│   │   │   └── ui/                            ← shadcn-generated
│   │   ├── hooks/
│   │   │   ├── useChat.ts
│   │   │   ├── useConversations.ts
│   │   │   ├── useLiveMetrics.ts
│   │   │   └── useEventStream.ts
│   │   ├── lib/
│   │   │   ├── obs.ts                         ← initializes @llm-obs/sdk
│   │   │   ├── llm/
│   │   │   │   ├── index.ts                   ← provider factory
│   │   │   │   ├── openai.ts
│   │   │   │   ├── anthropic.ts
│   │   │   │   └── gemini.ts
│   │   │   ├── db.ts                          ← prisma client
│   │   │   └── cancel-registry.ts             ← in-memory abort handles
│   │   └── public/
│   │
│   ├── ingestion/                   ← Fastify API
│   │   ├── package.json
│   │   ├── tsconfig.json
│   │   ├── src/
│   │   │   ├── server.ts
│   │   │   ├── env.ts
│   │   │   ├── plugins/
│   │   │   │   ├── prisma.ts
│   │   │   │   ├── redis.ts
│   │   │   │   ├── queue.ts
│   │   │   │   ├── sse.ts
│   │   │   │   └── auth.ts                    ← api-key check
│   │   │   ├── routes/
│   │   │   │   ├── health.ts
│   │   │   │   ├── ingest.ts                  ← POST /v1/ingest, /batch
│   │   │   │   ├── logs.ts                    ← GET /v1/logs(/:id)
│   │   │   │   ├── metrics.ts                 ← summary, timeseries, errors
│   │   │   │   └── stream.ts                  ← SSE
│   │   │   └── schemas/
│   │   │       └── ingest.ts                  ← zod
│   │   └── Dockerfile
│   │
│   └── worker/                      ← BullMQ worker
│       ├── package.json
│       ├── src/
│       │   ├── index.ts
│       │   ├── env.ts
│       │   ├── processors/
│       │   │   ├── ingest.processor.ts        ← main inference-log processor
│       │   │   └── aggregate.processor.ts     ← optional periodic rollups
│       │   └── lib/
│       │       ├── cost.ts                    ← model price table
│       │       └── publish.ts                 ← redis pub/sub helper
│       └── Dockerfile
│
└── packages/
    ├── sdk/                         ← @llm-obs/sdk
    │   ├── package.json
    │   ├── src/
    │   │   ├── index.ts
    │   │   ├── client.ts                       ← ObservabilityClient
    │   │   ├── transport.ts                    ← HTTP w/ batching+retry
    │   │   ├── types.ts                        ← InferenceLog, etc.
    │   │   ├── providers/
    │   │   │   ├── openai.ts                   ← wrapOpenAI
    │   │   │   ├── anthropic.ts                ← wrapAnthropic
    │   │   │   └── gemini.ts                   ← wrapGemini
    │   │   ├── stream.ts                       ← stream tap utilities
    │   │   └── id.ts                           ← ulid generation
    │   └── tests/
    │
    ├── pii/                         ← @llm-obs/pii
    │   ├── package.json
    │   ├── src/
    │   │   ├── index.ts
    │   │   ├── patterns.ts                     ← regex catalog
    │   │   ├── luhn.ts                         ← credit card validation
    │   │   ├── redact.ts
    │   │   └── ner.ts                          ← optional names/orgs
    │   └── tests/
    │
    ├── db/                          ← shared prisma client
    │   ├── package.json
    │   ├── prisma/
    │   │   ├── schema.prisma
    │   │   └── seed.ts
    │   ├── src/
    │   │   └── index.ts                        ← exports PrismaClient singleton
    │   └── migrations/
    │
    ├── types/                       ← shared zod + TS types
    │   ├── package.json
    │   └── src/
    │       ├── index.ts
    │       ├── inference.ts                    ← InferenceLogPayload schema
    │       ├── metrics.ts
    │       └── events.ts
    │
    └── config/                      ← tsconfig + biome presets
        ├── tsconfig/
        └── biome/
```

---

## 4. Service Architecture & Communication

### 4.1 Services

| Service | Port | Talks To |
|---|---|---|
| `web` (Next.js) | 3000 | LLM providers (HTTP), `@llm-obs/sdk` → `ingestion`, `postgres` (read for conv/messages), `ingestion` SSE for live dashboard |
| `ingestion` (Fastify) | 4000 | `postgres` (write events, read logs), `redis` (BullMQ + pub/sub) |
| `worker` (BullMQ) | n/a | `redis` (consume), `postgres` (write logs), `redis` (publish metrics) |
| `postgres` | 5432 | — |
| `redis` | 6379 | — |
| `adminer` (optional) | 8080 | `postgres` |

### 4.2 Communication patterns

- **Browser ↔ web**: HTTP + **SSE** (chat streaming, dashboard live feed).
- **web (SDK) → ingestion**: HTTP POST (fire-and-forget with retries, batched).
- **ingestion → worker**: **BullMQ job** on Redis queue `inference_logs`.
- **worker → dashboards**: **Redis Pub/Sub** channel `metrics.events` + DB writes.
- **dashboard `/api/dashboard/stream` → browser**: SSE, fanned out from the Pub/Sub subscription.

### 4.3 Why this split

- The chatbot stays **snappy** because it never blocks on persistence.
- We genuinely demonstrate **event-driven** processing (decoupled writes, post-processing, replayability).
- The worker is the right place for **PII redaction**, **cost computation**, and **derived metrics** — keeps the hot path lean.

---

## 5. Database Schema (Prisma)

> File: `packages/db/prisma/schema.prisma`. Postgres 16.

### 5.1 Enums

```prisma
enum ConversationStatus { ACTIVE  CANCELLED  COMPLETED  ARCHIVED }
enum MessageRole        { USER  ASSISTANT  SYSTEM  TOOL }
enum InferenceStatus    { SUCCESS  ERROR  CANCELLED  TIMEOUT }
enum Provider           { OPENAI  ANTHROPIC  GOOGLE  OTHER }
enum EventStatus        { RECEIVED  PROCESSED  FAILED }
```

### 5.2 `conversations`

| Column | Type | Notes |
|---|---|---|
| `id` | `uuid` PK | `gen_random_uuid()` |
| `title` | `text` | autogen from first user msg |
| `user_id` | `text?` | nullable for POC |
| `status` | `ConversationStatus` | default `ACTIVE` |
| `model` | `text?` | current/last model used |
| `provider` | `Provider?` | last provider |
| `created_at` | `timestamptz` | default `now()` |
| `updated_at` | `timestamptz` | `@updatedAt` |
| `metadata` | `jsonb` | free-form |

Indexes: `(updated_at desc)`, `(status, updated_at desc)`.

### 5.3 `messages`

| Column | Type | Notes |
|---|---|---|
| `id` | `uuid` PK | |
| `conversation_id` | `uuid` FK → `conversations.id` ON DELETE CASCADE | |
| `role` | `MessageRole` | |
| `content` | `text` | original; **server-side only** |
| `content_redacted` | `text` | what dashboards read |
| `tokens` | `int?` | per-message tokens if known |
| `inference_log_id` | `uuid?` FK → `inference_logs.id` | null for user/system |
| `created_at` | `timestamptz` | |
| `metadata` | `jsonb` | tool calls, attachments |

Indexes: `(conversation_id, created_at)`.

### 5.4 `inference_logs`

> The star table. One row per LLM call.

| Column | Type | Notes |
|---|---|---|
| `id` | `uuid` PK | from SDK (ULID-as-uuid) for idempotency |
| `conversation_id` | `uuid?` FK | |
| `session_id` | `text?` | external/SDK-supplied |
| `provider` | `Provider` | |
| `model` | `text` | |
| `request_id` | `text?` | provider-side id when available |
| `status` | `InferenceStatus` | |
| `error_type` | `text?` | e.g. `rate_limit`, `context_length` |
| `error_message` | `text?` | redacted |
| `prompt_tokens` | `int?` | |
| `completion_tokens` | `int?` | |
| `total_tokens` | `int?` | generated col `prompt+completion` |
| `cost_usd` | `numeric(12,6)?` | computed in worker |
| `latency_ms` | `int` | total |
| `ttft_ms` | `int?` | time to first token (streaming) |
| `streamed` | `boolean` | default false |
| `temperature` | `numeric(4,3)?` | |
| `max_tokens` | `int?` | |
| `stop_reason` | `text?` | finish_reason |
| `input_preview` | `text` | first 256 chars, redacted |
| `output_preview` | `text` | first 256 chars, redacted |
| `input_hash` | `text` | sha256 of full request |
| `request_payload` | `jsonb` | redacted full request (messages, params) |
| `response_payload` | `jsonb` | redacted full response |
| `pii_detections` | `jsonb` | `[{type, count}]` |
| `sdk_version` | `text?` | |
| `environment` | `text` | `dev`/`staging`/`prod` |
| `started_at` | `timestamptz` | |
| `ended_at` | `timestamptz` | |
| `created_at` | `timestamptz` | server side default `now()` |
| `metadata` | `jsonb` | |

Indexes:
- `(created_at desc)`
- `(conversation_id, created_at desc)`
- `(provider, model, created_at desc)`
- `(status, created_at desc)`
- BRIN on `created_at` (POC-overkill but cheap).

### 5.5 `inference_events` (audit / replay buffer)

| Column | Type | Notes |
|---|---|---|
| `id` | `uuid` PK | |
| `payload` | `jsonb` | raw body received |
| `status` | `EventStatus` | |
| `received_at` | `timestamptz` | |
| `processed_at` | `timestamptz?` | |
| `error` | `text?` | last error if FAILED |
| `attempts` | `int` | default 0 |

Indexes: `(status, received_at)`.

### 5.6 `metric_rollups` (optional, populated by worker every minute)

| Column | Type | Notes |
|---|---|---|
| `bucket` | `timestamptz` (1-min bucket) PK part | |
| `provider` | `Provider` PK part | |
| `model` | `text` PK part | |
| `count_total` | `int` | |
| `count_error` | `int` | |
| `sum_latency_ms` | `bigint` | |
| `sum_ttft_ms` | `bigint` | |
| `sum_prompt_tokens` | `bigint` | |
| `sum_completion_tokens` | `bigint` | |
| `sum_cost_usd` | `numeric(14,6)` | |

PK: `(bucket, provider, model)`. Used for fast dashboard time-series; falls back to live aggregation if missing.

---

## 6. API Contracts

### 6.1 Web app (Next.js route handlers) — talks to the browser

All under `apps/web/app/api`. JSON unless noted.

#### `POST /api/chat` — send message, stream response (SSE)

Request:

```json
{
  "conversationId": "uuid | null",
  "message": "string",
  "provider": "openai" | "anthropic" | "google",
  "model": "string",
  "stream": true
}
```

Response (SSE event stream, `text/event-stream`):

```
event: meta
data: {"conversationId":"...","messageId":"...","inferenceLogId":"..."}

event: token
data: {"delta":"Hello"}

event: token
data: {"delta":" there"}

event: usage
data: {"promptTokens":42,"completionTokens":11}

event: done
data: {"finishReason":"stop","latencyMs":1234,"ttftMs":210}

event: error
data: {"type":"rate_limit","message":"..."}
```

Server registers an `AbortController` keyed by `inferenceLogId` in `lib/cancel-registry.ts` so `/cancel` can interrupt.

#### `GET /api/conversations`

Query: `?limit=50&cursor=<id>&status=ACTIVE`

Response:

```json
{
  "items": [
    {
      "id": "...",
      "title": "...",
      "status": "ACTIVE",
      "model": "gpt-4o-mini",
      "provider": "openai",
      "updatedAt": "ISO",
      "lastMessagePreview": "..."
    }
  ],
  "nextCursor": "..."
}
```

#### `POST /api/conversations`

Request: `{ "title"?: "string", "model"?: "string", "provider"?: "..." }`
Response: full conversation object.

#### `GET /api/conversations/:id`

Response: `{ conversation, messages: Message[] }` (messages ordered by `created_at`).

#### `DELETE /api/conversations/:id` — soft delete (`status=ARCHIVED`).

#### `POST /api/conversations/:id/cancel`

Request body optional: `{ "inferenceLogId"?: "..." }`. If supplied, cancel just that inference; otherwise cancel **any active inference** for the conversation.
Response: `{ "cancelled": true, "inferenceLogId": "..." }`.

#### Dashboard proxy endpoints (thin wrappers around ingestion API)

- `GET /api/dashboard/summary?range=24h`
- `GET /api/dashboard/timeseries?metric=latency_p95&range=24h&groupBy=model`
- `GET /api/dashboard/logs?provider=&model=&status=&q=&limit=&cursor=`
- `GET /api/dashboard/stream` — SSE, forwards from ingestion `/v1/stream`.

### 6.2 Ingestion API (Fastify)

All routes under `/v1`. Require header `x-obs-api-key: <env.INGEST_API_KEY>`. JSON unless SSE.

#### `POST /v1/ingest` — single inference log

Request body (validated by zod; see §6.3 for full schema):

```json
{
  "id": "01HXYZ...",                       // SDK-generated ULID, idempotency key
  "conversationId": "uuid|null",
  "sessionId": "string|null",
  "provider": "openai",
  "model": "gpt-4o-mini",
  "status": "success",
  "startedAt": "ISO",
  "endedAt": "ISO",
  "latencyMs": 1234,
  "ttftMs": 210,
  "streamed": true,
  "request": {
    "messages": [{ "role":"user","content":"..." }],
    "temperature": 0.7,
    "maxTokens": 512
  },
  "response": {
    "content": "...",
    "finishReason": "stop"
  },
  "usage": { "promptTokens": 42, "completionTokens": 11 },
  "error": null,
  "sdkVersion": "0.1.0",
  "environment": "dev",
  "metadata": { "feature": "support-bot" }
}
```

Response: `202 Accepted`

```json
{ "id": "01HXYZ...", "queued": true }
```

#### `POST /v1/ingest/batch`

Body: `{ "events": InferenceLog[] }` (≤ 100). Returns `{ "accepted": n, "rejected": [...] }`.

#### `GET /v1/health` → `{ "ok": true, "redis": "up", "db": "up" }`

#### `GET /v1/logs`

Query: `provider, model, status, conversationId, q, from, to, limit (≤200), cursor`.
Response:

```json
{
  "items": [InferenceLogRow],
  "nextCursor": "..."
}
```

#### `GET /v1/logs/:id` → `InferenceLogRow` (full payload included).

#### `GET /v1/metrics/summary?range=24h`

```json
{
  "range":"24h",
  "totalRequests": 12345,
  "errorRate": 0.013,
  "p50LatencyMs": 420,
  "p95LatencyMs": 1800,
  "p99LatencyMs": 3200,
  "avgTtftMs": 180,
  "totalPromptTokens": 1234567,
  "totalCompletionTokens": 765432,
  "totalCostUsd": 12.34,
  "byProvider": [{ "provider":"openai", "count": 9000, "errorRate": 0.01 }],
  "byModel": [{ "model":"gpt-4o-mini","count":7000 }]
}
```

#### `GET /v1/metrics/timeseries`

Query: `metric=(requests|latency_p50|latency_p95|errors|cost|tokens), range=24h, bucket=1m|5m|1h, groupBy=(provider|model|none)`.

Response:

```json
{
  "bucket":"5m",
  "series":[
    { "key":"openai/gpt-4o-mini", "points":[{"t":"ISO","v":42}, ...] }
  ]
}
```

#### `GET /v1/metrics/errors?range=24h` → `[{ errorType, count, lastSeen }]`.

#### `GET /v1/stream` — SSE feed of newly processed events

Events: `event: log` with body `InferenceLogRow` (lean version, no full payload).

### 6.3 Shared schemas (`packages/types/src/inference.ts`)

```ts
import { z } from "zod";

export const ProviderEnum = z.enum(["openai", "anthropic", "google", "other"]);
export const StatusEnum   = z.enum(["success", "error", "cancelled", "timeout"]);

export const UsageSchema = z.object({
  promptTokens: z.number().int().nonnegative().optional(),
  completionTokens: z.number().int().nonnegative().optional(),
  totalTokens: z.number().int().nonnegative().optional(),
});

export const MessageSchema = z.object({
  role: z.enum(["system","user","assistant","tool"]),
  content: z.string(),
  name: z.string().optional(),
});

export const InferenceLogPayloadSchema = z.object({
  id: z.string().min(10),
  conversationId: z.string().uuid().nullable().optional(),
  sessionId: z.string().nullable().optional(),
  provider: ProviderEnum,
  model: z.string(),
  status: StatusEnum,
  startedAt: z.string().datetime(),
  endedAt: z.string().datetime(),
  latencyMs: z.number().int().nonnegative(),
  ttftMs: z.number().int().nonnegative().optional(),
  streamed: z.boolean().default(false),
  request: z.object({
    messages: z.array(MessageSchema).optional(),
    prompt: z.string().optional(),
    temperature: z.number().optional(),
    maxTokens: z.number().int().optional(),
    tools: z.array(z.any()).optional(),
    extra: z.record(z.any()).optional(),
  }),
  response: z.object({
    content: z.string().optional(),
    finishReason: z.string().optional(),
    toolCalls: z.array(z.any()).optional(),
  }).optional(),
  usage: UsageSchema.optional(),
  error: z.object({
    type: z.string(),
    message: z.string(),
    code: z.string().optional(),
  }).nullable().optional(),
  sdkVersion: z.string().optional(),
  environment: z.string().default("dev"),
  metadata: z.record(z.any()).optional(),
});

export type InferenceLogPayload = z.infer<typeof InferenceLogPayloadSchema>;
```

---

## 7. SDK Design (`packages/sdk`)

### 7.1 Public surface (`src/index.ts`)

```ts
export { ObservabilityClient } from "./client";
export { wrapOpenAI }    from "./providers/openai";
export { wrapAnthropic } from "./providers/anthropic";
export { wrapGemini }    from "./providers/gemini";
export type { InferenceLogPayload, ObservabilityClientOptions } from "./types";
```

### 7.2 `ObservabilityClient`

```ts
export interface ObservabilityClientOptions {
  endpoint: string;                       // e.g. http://localhost:4000
  apiKey?: string;
  environment?: "dev"|"staging"|"prod";
  sdkVersion?: string;
  batchSize?: number;                     // default 20
  flushIntervalMs?: number;               // default 1500
  maxRetries?: number;                    // default 3
  onError?: (err: unknown) => void;
  defaultMetadata?: Record<string, unknown>;
  redactPIIClientSide?: boolean;          // default false; worker still redacts
}

export class ObservabilityClient {
  constructor(opts: ObservabilityClientOptions);

  /** Manually record a fully-formed log. */
  log(event: InferenceLogPayload): void;

  /** Open a span for custom instrumentation. */
  startSpan(input: {
    conversationId?: string;
    sessionId?: string;
    provider: Provider;
    model: string;
    request: InferenceLogPayload["request"];
    metadata?: Record<string, unknown>;
  }): InferenceSpan;

  flush(): Promise<void>;
  shutdown(): Promise<void>;     // flushes + clears interval
}

export interface InferenceSpan {
  readonly id: string;
  setTtft(ms?: number): void;
  appendOutput(chunk: string): void;     // for streaming previews
  setUsage(u: { promptTokens?: number; completionTokens?: number }): void;
  setError(err: { type: string; message: string; code?: string }): void;
  setMetadata(m: Record<string, unknown>): void;
  end(result?: { status?: InferenceStatus; finishReason?: string }): void;
}
```

### 7.3 Provider wrappers (auto-instrumented)

```ts
import OpenAI from "openai";
const obs    = new ObservabilityClient({ endpoint: "http://localhost:4000" });
const openai = wrapOpenAI(new OpenAI(), obs, {
  conversationId: () => currentConversationId,   // resolved per call
});
```

Each wrapper:

1. Proxies `chat.completions.create` (OpenAI), `messages.create` (Anthropic), `generateContentStream` / `generateContent` (Gemini).
2. On call: starts a span, records `startedAt`, `provider`, `model`, `request`.
3. For streaming: returns an async iterator that **tees** chunks — yields to caller, also feeds the span (TTFT on first token, output append).
4. On finish: collects `usage`, sets `endedAt`, calls `span.end()`.
5. On error: catches, sets error, ends span with `status=error`, **re-throws** the original error.

Internally each wrapper uses a small `tapStream(iter, onChunk, onEnd, onError)` helper (`src/stream.ts`).

### 7.4 Transport (`src/transport.ts`)

- Buffers events in memory; flushes when `batchSize` reached or `flushIntervalMs` ticks.
- Uses `fetch` with `keepalive: true` and `AbortSignal.timeout(5000)`.
- Exponential backoff retry (250ms → 4s) up to `maxRetries`.
- On `shutdown()` posts everything remaining (called by Next.js on process exit hook).

### 7.5 Cost model (`apps/worker/src/lib/cost.ts`)

POC keeps a small table; worker computes cost on processing:

```ts
export const PRICE_TABLE: Record<string, { input: number; output: number }> = {
  "openai:gpt-4o-mini":     { input: 0.15 / 1e6,  output: 0.60 / 1e6 },
  "openai:gpt-4.1-mini":    { input: 0.40 / 1e6,  output: 1.60 / 1e6 },
  "anthropic:claude-3-5-haiku-latest":  { input: 0.80 / 1e6, output: 4.00 / 1e6 },
  "anthropic:claude-3-5-sonnet-latest": { input: 3.00 / 1e6, output: 15.00 / 1e6 },
  "google:gemini-1.5-flash":             { input: 0.075 / 1e6, output: 0.30 / 1e6 },
  "google:gemini-1.5-pro":               { input: 1.25 / 1e6,  output: 5.00 / 1e6 },
};
```

If unknown → `cost_usd = null`. Document this in README.

---

## 8. Event Architecture

### 8.1 Queues

| Queue | Producer | Consumer | Job data |
|---|---|---|---|
| `inference_logs` | ingestion API | worker | `{ eventId, payload }` |
| `aggregate_rollups` (repeatable) | worker boot | worker | every 60s, no payload |

### 8.2 Pub/Sub channels

| Channel | Publisher | Subscriber | Payload |
|---|---|---|---|
| `metrics.events` | worker (after upsert) | ingestion `/v1/stream` SSE | lean `InferenceLogRow` |
| `metrics.summary` | worker aggregator | dashboards (future) | rollup deltas |

### 8.3 Worker job flow (`apps/worker/src/processors/ingest.processor.ts`)

1. Read `payload` from job.
2. Mark `inference_events.status='RECEIVED'` (already done in ingestion route on enqueue).
3. **Redact** `request.messages[*].content` and `response.content` via `@llm-obs/pii`. Track detections.
4. Compute:
   - `total_tokens = promptTokens + completionTokens` (if missing).
   - `cost_usd` from `PRICE_TABLE`.
   - `input_preview` / `output_preview` (256 chars, redacted).
   - `input_hash = sha256(JSON.stringify(request.messages || request.prompt))`.
5. **Upsert** `inference_logs` (`id` is PK; idempotent).
6. If `conversationId` set, **upsert assistant `messages` row** with `inference_log_id` and redacted content.
7. Update `inference_events.status='PROCESSED', processed_at=now()`.
8. `redis.publish('metrics.events', JSON.stringify(leanRow))`.
9. Increment in-process counters that feed `metric_rollups` (flushed every 60s).

Retries: BullMQ default `attempts: 5, backoff: { type: "exponential", delay: 500 }`. After all attempts, set `inference_events.status='FAILED'`.

### 8.4 Why not just write synchronously?

We deliberately split the write so we can:
- demonstrate **decoupling** (the API stays fast even under bursts),
- run heavier PII/NER work off the hot path,
- replay events from `inference_events` (audit table) if the worker had a bug.

---

## 9. Frontend Component Tree

### 9.1 Chat experience

```
app/(chat)/layout.tsx
└─ <ChatLayout>
   ├─ <ConversationSidebar>          # list, create, archive, search
   │   ├─ <ConversationListItem>     # active state, delete menu
   │   └─ <NewChatButton>
   └─ <ChatPage>                     # chat/[conversationId]/page.tsx
       ├─ <ModelPicker>              # provider + model dropdowns
       ├─ <MessageList>
       │   └─ <Message>              # markdown render, role badge, copy
       ├─ <StreamingIndicator>       # shows TTFT + live tokens/sec
       ├─ <CancelButton>             # visible only while streaming
       └─ <ChatInput>                # textarea, Enter to send, Shift+Enter newline
```

**Cancel flow:**
- `useChat` keeps an `AbortController` and the current `inferenceLogId`.
- Cancel button calls `controller.abort()` (closes the SSE on client), then `POST /api/conversations/:id/cancel` so the server aborts the upstream provider stream and writes the log as `status=cancelled`.

**Resume a conversation:**
- Clicking a sidebar item routes to `/chat/:conversationId`; the page server-component fetches messages from Postgres; `useChat` rehydrates from those messages and stays ready for new turns.

### 9.2 Dashboard

```
app/dashboard/layout.tsx
└─ <DashboardLayout>
   ├─ <Sidebar>                      # Overview, Logs
   └─ <Outlet>
       ├─ /dashboard
       │   ├─ <RangePicker>          # 1h / 24h / 7d
       │   ├─ <MetricCard x6>        # requests, error rate, p50/p95/p99, total cost, total tokens
       │   ├─ <ThroughputChart>      # area, group by provider
       │   ├─ <LatencyChart>         # p50/p95/p99 lines + TTFT overlay
       │   ├─ <ErrorBreakdown>       # bar by error_type
       │   ├─ <TokenUsageChart>      # stacked by model
       │   ├─ <TopModelsTable>
       │   └─ <LiveEventFeed>        # rolling 50 events via SSE
       └─ /dashboard/logs
           ├─ <LogsFilters>          # provider, model, status, q, daterange
           ├─ <LogsTable>            # virtualized
           └─ /dashboard/logs/[id]   # full log detail: request/response panels, timeline
```

### 9.3 Key hooks

- `useChat(conversationId)` — manages SSE, abort, message buffer.
- `useConversations()` — SWR/`useQuery` over `/api/conversations`.
- `useLiveMetrics()` — opens SSE to `/api/dashboard/stream`, maintains rolling buffers per series.
- `useMetricsSummary(range)` — polls `/api/dashboard/summary` every 30s plus revalidates on tab focus.

> Use **`@tanstack/react-query`** for non-SSE data fetching (caching, retries, devtools). It plays well with RSC for prefetch.

---

## 10. Docker Compose Setup

`docker-compose.yml` (root):

```yaml
services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: obs
      POSTGRES_PASSWORD: obs
      POSTGRES_DB: obs
    ports: ["5432:5432"]
    volumes: ["pgdata:/var/lib/postgresql/data"]
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U obs"]
      interval: 5s
      retries: 10

  redis:
    image: redis:7-alpine
    ports: ["6379:6379"]
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      retries: 10

  ingestion:
    build: { context: ., dockerfile: apps/ingestion/Dockerfile }
    environment:
      DATABASE_URL: postgres://obs:obs@postgres:5432/obs
      REDIS_URL: redis://redis:6379
      INGEST_API_KEY: dev-key
      PORT: 4000
    depends_on:
      postgres: { condition: service_healthy }
      redis:    { condition: service_healthy }
    ports: ["4000:4000"]

  worker:
    build: { context: ., dockerfile: apps/worker/Dockerfile }
    environment:
      DATABASE_URL: postgres://obs:obs@postgres:5432/obs
      REDIS_URL: redis://redis:6379
    depends_on:
      postgres: { condition: service_healthy }
      redis:    { condition: service_healthy }

  web:
    build: { context: ., dockerfile: apps/web/Dockerfile }
    environment:
      DATABASE_URL: postgres://obs:obs@postgres:5432/obs
      INGEST_URL: http://ingestion:4000
      INGEST_API_KEY: dev-key
      OPENAI_API_KEY: ${OPENAI_API_KEY}
      ANTHROPIC_API_KEY: ${ANTHROPIC_API_KEY}
      GOOGLE_API_KEY: ${GOOGLE_API_KEY}
    depends_on:
      ingestion: { condition: service_started }
    ports: ["3000:3000"]

  adminer:
    image: adminer:4
    ports: ["8080:8080"]
    depends_on: [postgres]

volumes:
  pgdata:
```

Add `docker-compose.dev.yml` that only starts `postgres`, `redis`, `adminer` for `pnpm dev`.

One-command start:

```bash
cp .env.example .env   # fill in provider keys
docker compose up --build
```

Migrations: ingestion container runs `prisma migrate deploy` on startup (entrypoint shell script).

---

## 11. PII Redaction Strategy (`packages/pii`)

### 11.1 Detectors (regex + heuristics)

| Type | Strategy |
|---|---|
| `email` | `/\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/gi` |
| `phone` | E.164 + common US/EU formats, validated by digit count 7–15 |
| `ssn` | `/\b\d{3}-\d{2}-\d{4}\b/g` (US) |
| `credit_card` | 13–19 digit run (with optional spaces/dashes) **+ Luhn check** to avoid FPs |
| `ipv4` | standard regex with octet ≤255 check |
| `ipv6` | conservative pattern |
| `iban` | `/\b[A-Z]{2}\d{2}[A-Z0-9]{11,30}\b/` + length-by-country table |
| `api_key` | OpenAI `sk-…`, Anthropic `sk-ant-…`, AWS `AKIA…`, generic high-entropy tokens (`>= 32 chars`, base64ish, entropy threshold) |
| `url_with_secret` | URLs containing `token=`, `apikey=`, `password=` query params (mask values) |
| `person_name` (optional) | `compromise` NER, `doc.people()`; disabled by default (FP-prone) |

### 11.2 API

```ts
export type PIIType =
  | "email"|"phone"|"ssn"|"credit_card"|"ipv4"|"ipv6"|"iban"|"api_key"|"url_secret"|"person_name";

export interface PIIDetection { type: PIIType; count: number; }

export interface RedactionOptions {
  enable?: Partial<Record<PIIType, boolean>>;
  placeholder?: (t: PIIType) => string;          // default `[REDACTED_${t.toUpperCase()}]`
}

export function redact(text: string, opts?: RedactionOptions): {
  redacted: string;
  detections: PIIDetection[];
};

export function redactDeep<T>(value: T, opts?: RedactionOptions): { value: T; detections: PIIDetection[] };
```

`redactDeep` walks JSON; redacts only string leaves.

### 11.3 Where redaction happens

1. **Worker (canonical):** before persisting `request_payload`, `response_payload`, `input_preview`, `output_preview`, `error_message`. All dashboard reads go through this.
2. **SDK (optional, opt-in):** if `redactPIIClientSide: true`, also redact payload before sending. Useful when ingestion is across a network boundary.
3. **`messages.content`:** stored as the original text (we still need full context for the model in future turns); `messages.content_redacted` is what dashboards read. If POC needs strict privacy, set `STORE_RAW_MESSAGES=false` and only persist `content_redacted`.

### 11.4 Visibility

`pii_detections` jsonb per log + a small dashboard card "PII redacted this hour: 12 emails, 3 phones".

---

## 12. Dashboard Design

### 12.1 Overview page (`/dashboard`)

**Top metric cards (range = selectable 1h / 24h / 7d):**

1. Total requests
2. Error rate (and trend vs previous period)
3. p50 / p95 / p99 latency (single card with three figures)
4. Avg TTFT (streaming only)
5. Total tokens (prompt + completion)
6. Estimated cost USD

**Charts:**

| Chart | Type | X | Y | Group by |
|---|---|---|---|---|
| Throughput | stacked area | time bucket | requests | provider |
| Latency | multi-line | time | p50 / p95 / p99 | overall |
| TTFT | line | time | avg TTFT | model |
| Errors | bar (horizontal) | error_type | count | — |
| Token usage | stacked bar | time | tokens | model (prompt vs completion variants) |
| Cost | area | time | USD | provider |

**Tables:**

- **Top 5 models** by request count (with avg latency, error rate, cost).
- **Slowest 10 requests** (last 24h) — link to log detail.

**Live feed:** scrollable list, newest 50, fed by SSE. Each row: ts, provider/model, latency, status badge, conversation link.

### 12.2 Logs explorer (`/dashboard/logs`)

- Filters: provider, model, status, free-text search (matches `input_preview`/`output_preview` redacted text), date range.
- Virtualized table: `ts | provider/model | status | latency | ttft | prompt→completion tokens | cost | conversation`.
- Row click → `/dashboard/logs/:id` with:
  - Header summary (status, model, latency, cost, env).
  - Request panel (JSON, redacted) — collapsible.
  - Response panel (text + JSON, redacted).
  - PII detections summary.
  - Timeline (started → TTFT → ended).
  - Linked conversation card (if any).

### 12.3 Data sources

- Overview cards: `GET /v1/metrics/summary` (cached 10s server-side).
- Time series: `GET /v1/metrics/timeseries` — backend prefers `metric_rollups` for ranges > 1h, falls back to live aggregation from `inference_logs` for the latest minute.
- Live feed: SSE `/v1/stream` (forwarded by web).
- Logs table: `GET /v1/logs` with cursor pagination on `(created_at, id)`.

---

## 13. Conversation Lifecycle Details

### 13.1 Create

- New chat page calls `POST /api/conversations` lazily on first message send.
- Title autogenerated: take first user message, truncate to 60 chars (worker can later replace with model-generated title — optional stretch goal).

### 13.2 Resume

- Clicking sidebar → route to `/chat/:id`.
- Server component fetches `conversations` + `messages` ordered by `created_at`.
- `useChat` rehydrates and is ready for new turns. Provider/model default to `conversations.model` if present.

### 13.3 Cancel

Two layers:

1. **Cancel current inference**: button on the message that's streaming. Aborts SSE on client (`AbortController.abort()`) and posts `/cancel`. Server uses `lib/cancel-registry.ts`:

   ```ts
   const registry = new Map<string, AbortController>();  // key = inferenceLogId
   ```

   On `/api/chat`, before invoking the provider stream, we `registry.set(id, controller)` and pass `controller.signal`. On `/cancel`, we `registry.get(id)?.abort()`. The provider SDK respects the signal and stops. The wrapper writes the log with `status=cancelled` and partial usage if available.

2. **Cancel/archive conversation**: `DELETE /api/conversations/:id` sets `status=ARCHIVED`. Hidden from sidebar by default; "Show archived" toggle reveals.

### 13.4 List

- Sidebar uses `useConversations()` with React Query; revalidates on focus and after each message send.
- Search: client-side filter on title + last preview.

---

## 14. Streaming End-to-End

```
ChatInput.submit()
  → fetch("/api/chat", { body, signal })
       → web/app/api/chat/route.ts:
            - creates/loads conversation
            - persists user `messages` row
            - obs.startSpan({...request})  // ULID = inferenceLogId
            - registers AbortController in cancel-registry
            - calls wrappedOpenAI.chat.completions.create({ stream: true, signal })
            - returns SSE Response, piping provider chunks → "token" events
              and tee'ing into span.appendOutput()
            - on finish: span.end(), persist assistant `messages`, write to ingestion (via SDK)
            - on abort: span.end({ status: "cancelled" })
  → browser receives `meta` → `token`* → `usage` → `done` (or `error`)
```

Streaming for Anthropic and Gemini uses their respective SDKs; the wrapper unifies them into the same "delta string" event for the UI.

---

## 15. Environment Variables (`.env.example`)

```
# Postgres
DATABASE_URL=postgres://obs:obs@localhost:5432/obs

# Redis
REDIS_URL=redis://localhost:6379

# Ingestion
INGEST_URL=http://localhost:4000
INGEST_API_KEY=dev-key

# Web
NEXT_PUBLIC_APP_NAME=LLM Obs POC

# LLM provider keys (at least one required)
OPENAI_API_KEY=
ANTHROPIC_API_KEY=
GOOGLE_API_KEY=

# Behavior
ENVIRONMENT=dev
STORE_RAW_MESSAGES=true
ENABLE_NER=false
```

---

## 16. Implementation Plan (suggested order)

> Designed so the engineer can demo *something* after every step.

1. **Repo scaffolding**: pnpm + turbo + tsconfig + biome.
2. **Infra up**: `docker-compose.dev.yml` (pg+redis+adminer) + `packages/db` with Prisma schema and initial migration.
3. **Shared types** (`packages/types`) — zod schemas first.
4. **PII package** (`packages/pii`) with unit tests for each detector.
5. **Ingestion API** (`apps/ingestion`):
   - `/v1/health`, `/v1/ingest`, `/v1/ingest/batch` (enqueue + audit row).
   - `/v1/logs`, `/v1/logs/:id`, `/v1/metrics/summary`, `/v1/metrics/timeseries`, `/v1/stream`.
6. **Worker** (`apps/worker`): processor that redacts, computes cost, upserts logs, publishes Pub/Sub. Repeatable rollup job.
7. **SDK** (`packages/sdk`):
   - transport with batching/retry,
   - `ObservabilityClient`,
   - provider wrappers (OpenAI first, then Anthropic, then Gemini),
   - stream tap.
   - Unit tests using `nock` or mocked fetch + recorded SSE.
8. **Web app — chat**:
   - Prisma queries via `packages/db`.
   - `/api/conversations*` + `/api/chat` with SSE + cancel registry.
   - Sidebar + chat UI + model picker + cancel + resume.
9. **Web app — dashboard**:
   - Overview cards + charts (Recharts).
   - Logs explorer + log detail.
   - Live feed via SSE.
10. **Docker Compose for everything** + entrypoint scripts (prisma migrate, ts build).
11. **Polish**:
    - Seed script with 1k synthetic logs across providers for instant dashboard demo.
    - Playwright happy-path: send message → see it streaming → see it in dashboard live feed.
    - README with screenshots + GIF.

---

## 17. Practical Tradeoffs (called out for the reviewer)

- **BullMQ over Kafka:** sufficient for POC throughput, much easier to run; we still get "events" semantics.
- **Prisma over Drizzle:** faster to ship with great DX; migrations are first-class.
- **Postgres-only (no ClickHouse/Loki):** with the right indexes + JSONB this handles POC volume; honest about scale limits in README.
- **SSE over WebSockets:** unidirectional, behind plain HTTP, no extra protocol surface.
- **Regex PII over Presidio:** zero infra cost, deterministic, easy to extend; we acknowledge NER's value and ship it as an opt-in.
- **Idempotency via SDK-supplied `id`:** ingestion is a pure write; worker upsert is safe to retry.
- **One Next.js app for both chat + dashboard:** fewer moving parts; clearly separated in routing.
- **Hard-coded price table:** trivially extendable; clearly marked "POC".

---

## 18. README Checklist (for the engineer to author last)

- One-command setup (`docker compose up --build`).
- Architecture diagram (the ASCII in §1 is fine; add a real SVG if time).
- Demo script:
  1. Start stack.
  2. Open `localhost:3000/chat`, send a message in OpenAI, then Anthropic, then Gemini.
  3. Open `localhost:3000/dashboard` — see metrics populate live.
  4. Hit Cancel mid-stream, see status `cancelled` in dashboard.
  5. Reload, click an archived conversation to resume.
  6. Show a redacted log detail page with a PII detection.
- Limitations section: auth, multi-tenant, scale, NER caveats.

---

**End of architecture document.** Every endpoint, schema field, package name, and folder above is intentional; deviations should be a conscious choice by the implementer, not accidental drift.
