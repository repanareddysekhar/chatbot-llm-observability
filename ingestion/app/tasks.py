from __future__ import annotations

import hashlib
import json
import logging
import sys
import uuid
from datetime import datetime, timezone
from typing import Any

from celery import Task
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from .config import settings
from .worker import celery_app

logger = logging.getLogger(__name__)

# Sync SQLAlchemy engine for Celery tasks (Celery is sync by default)
_sync_url = settings.database_url.replace("postgresql+asyncpg", "postgresql+psycopg2")
try:
    from sqlalchemy import create_engine as _create_engine
    sync_engine = _create_engine(_sync_url, pool_size=5, max_overflow=10)
    SyncSession = sessionmaker(sync_engine)
except Exception:
    sync_engine = None
    SyncSession = None


PRICE_TABLE: dict[str, dict[str, float]] = {
    # OpenAI
    "openai:gpt-4o-mini":               {"input": 0.15 / 1e6, "output": 0.60 / 1e6},
    "openai:gpt-4.1-mini":              {"input": 0.40 / 1e6, "output": 1.60 / 1e6},
    "openai:gpt-4o":                    {"input": 2.50 / 1e6, "output": 10.00 / 1e6},
    "openai:gpt-4.1":                   {"input": 2.00 / 1e6, "output": 8.00 / 1e6},
    # Anthropic
    "anthropic:claude-3-5-haiku-latest":  {"input": 0.80 / 1e6, "output": 4.00 / 1e6},
    "anthropic:claude-3-5-sonnet-latest": {"input": 3.00 / 1e6, "output": 15.00 / 1e6},
    "anthropic:claude-sonnet-4-5":        {"input": 3.00 / 1e6, "output": 15.00 / 1e6},
    # Google
    "google:gemini-1.5-flash":            {"input": 0.075 / 1e6, "output": 0.30 / 1e6},
    "google:gemini-1.5-pro":             {"input": 1.25 / 1e6, "output": 5.00 / 1e6},
    "google:gemini-2.0-flash":           {"input": 0.10 / 1e6, "output": 0.40 / 1e6},
}

# Estimated local compute cost for Ollama models.
# Based on ~$0.10-0.40/hr GPU amortised at typical token throughput per model tier.
# Used purely for observability comparisons — not real billing.
_OLLAMA_PRICE_TABLE: dict[str, dict[str, float]] = {
    # Sub-2B models — very fast on CPU, minimal power draw
    "gemma3:1b":       {"input": 0.02 / 1e6, "output": 0.02 / 1e6},
    "llama3.2:1b":     {"input": 0.02 / 1e6, "output": 0.02 / 1e6},
    "phi3:mini":       {"input": 0.02 / 1e6, "output": 0.02 / 1e6},
    "gemma2:2b":       {"input": 0.02 / 1e6, "output": 0.02 / 1e6},
    "deepseek-r1:1.5b":{"input": 0.02 / 1e6, "output": 0.02 / 1e6},
    # 4B–8B models — GPU mid-tier, comparable to gpt-4o-mini efficiency
    "gemma3:4b":       {"input": 0.08 / 1e6, "output": 0.08 / 1e6},
    "gemma2:9b":       {"input": 0.08 / 1e6, "output": 0.08 / 1e6},
    "llama3.2":        {"input": 0.08 / 1e6, "output": 0.08 / 1e6},
    "llama3.1:8b":     {"input": 0.08 / 1e6, "output": 0.08 / 1e6},
    "mistral":         {"input": 0.08 / 1e6, "output": 0.08 / 1e6},
    "mistral:7b":      {"input": 0.08 / 1e6, "output": 0.08 / 1e6},
    "phi3":            {"input": 0.08 / 1e6, "output": 0.08 / 1e6},
    "qwen2.5:7b":      {"input": 0.08 / 1e6, "output": 0.08 / 1e6},
    "deepseek-r1:7b":  {"input": 0.08 / 1e6, "output": 0.08 / 1e6},
    "codellama:7b":    {"input": 0.08 / 1e6, "output": 0.08 / 1e6},
    "codellama":       {"input": 0.08 / 1e6, "output": 0.08 / 1e6},
    # 12B+ models — higher GPU utilisation
    "gemma3:12b":      {"input": 0.20 / 1e6, "output": 0.20 / 1e6},
    "llama3.1":        {"input": 0.20 / 1e6, "output": 0.20 / 1e6},
    "mistral-nemo":    {"input": 0.20 / 1e6, "output": 0.20 / 1e6},
    "qwen2.5":         {"input": 0.20 / 1e6, "output": 0.20 / 1e6},
}

# Default estimate for unrecognised Ollama models (mid-tier assumption)
_OLLAMA_DEFAULT_PRICE = {"input": 0.08 / 1e6, "output": 0.08 / 1e6}


def _compute_cost(provider: str, model: str, prompt_tokens: int, completion_tokens: int) -> float | None:
    if provider.lower() == "ollama":
        # Normalise model name (strip tag variant for lookup, e.g. "llama3.2:latest" → "llama3.2")
        base_model = model.lower().split(":latest")[0]
        prices = _OLLAMA_PRICE_TABLE.get(base_model) or _OLLAMA_PRICE_TABLE.get(model.lower()) or _OLLAMA_DEFAULT_PRICE
        return prices["input"] * prompt_tokens + prices["output"] * completion_tokens

    key = f"{provider.lower()}:{model.lower()}"
    prices = PRICE_TABLE.get(key)
    if not prices:
        return None
    return prices["input"] * prompt_tokens + prices["output"] * completion_tokens


def _redact(text: str | None) -> tuple[str | None, list[dict]]:
    if not text:
        return text, []
    try:
        sys.path.insert(0, "/app/sdk")
        from llm_obs.pii import redact
        return redact(text)
    except ImportError:
        return text, []


def _publish_metric(payload: dict[str, Any]) -> None:
    try:
        import redis as redis_lib
        r = redis_lib.from_url(settings.redis_url)
        r.publish("metrics.events", json.dumps(payload, default=str))
    except Exception as exc:
        logger.warning("Failed to publish metric: %s", exc)


@celery_app.task(
    name="process_inference_log",
    bind=True,
    max_retries=5,
    default_retry_delay=2,
    autoretry_for=(Exception,),
    retry_backoff=True,
)
def process_inference_log(self: Task, event_id: str, payload: dict[str, Any]) -> None:
    if SyncSession is None:
        logger.error("Sync DB session unavailable — install psycopg2")
        return

    with SyncSession() as db:
        try:
            _process(db, event_id, payload)
            db.commit()
        except Exception as exc:
            db.rollback()
            logger.exception("Error processing event %s: %s", event_id, exc)
            # Mark event as failed
            db.execute(
                text("UPDATE inference_events SET status='FAILED', error=:err WHERE id=:id"),
                {"err": str(exc)[:500], "id": event_id},
            )
            db.commit()
            raise


def _process(db: Session, event_id: str, payload: dict[str, Any]) -> None:
    provider_raw = payload.get("provider", "other").lower()
    model = payload.get("model", "unknown")
    status_raw = payload.get("status", "success").upper()
    usage = payload.get("usage") or {}
    prompt_tokens = usage.get("prompt_tokens")
    completion_tokens = usage.get("completion_tokens")
    total_tokens = (
        (prompt_tokens or 0) + (completion_tokens or 0) if prompt_tokens or completion_tokens else None
    )

    # PII redaction
    request_payload = payload.get("request") or {}
    response_payload = payload.get("response") or {}
    all_detections: list[dict] = []

    try:
        from llm_obs.pii import redact_deep
        request_payload, req_det = redact_deep(request_payload)
        response_payload, resp_det = redact_deep(response_payload)
        all_detections = req_det + resp_det
    except ImportError:
        pass

    # Input/output previews
    messages = request_payload.get("messages") or []
    input_text = " ".join(m.get("content", "") for m in messages if isinstance(m, dict)) or request_payload.get("prompt", "")
    input_preview = input_text[:256] if input_text else None
    output_preview = (response_payload.get("content") or "")[:256] or None

    # Input hash
    input_hash = hashlib.sha256(json.dumps(request_payload, sort_keys=True).encode()).hexdigest()

    # Cost
    cost_usd = None
    if prompt_tokens is not None and completion_tokens is not None:
        cost_usd = _compute_cost(provider_raw, model, prompt_tokens, completion_tokens)

    # Provider enum mapping (ollama maps to OTHER since it's not a cloud provider)
    provider_map = {"openai": "OPENAI", "anthropic": "ANTHROPIC", "google": "GOOGLE", "ollama": "OTHER"}
    provider_enum = provider_map.get(provider_raw, "OTHER")

    # Error info
    error = payload.get("error") or {}
    error_type = error.get("type") if isinstance(error, dict) else None
    error_message = error.get("message") if isinstance(error, dict) else None

    # Upsert inference_log (idempotent)
    db.execute(
        text("""
            INSERT INTO inference_logs (
                id, conversation_id, session_id, provider, model, status,
                error_type, error_message, prompt_tokens, completion_tokens, total_tokens,
                cost_usd, latency_ms, ttft_ms, streamed, input_preview, output_preview,
                request_payload, response_payload, pii_detections,
                sdk_version, environment, started_at, ended_at, created_at
            ) VALUES (
                :id, :conv_id, :session_id, :provider, :model, :status,
                :error_type, :error_message, :prompt_tokens, :completion_tokens, :total_tokens,
                :cost_usd, :latency_ms, :ttft_ms, :streamed, :input_preview, :output_preview,
                :request_payload, :response_payload, :pii_detections,
                :sdk_version, :environment, :started_at, :ended_at, NOW()
            )
            ON CONFLICT (id) DO UPDATE SET
                status = EXCLUDED.status,
                error_type = EXCLUDED.error_type,
                error_message = EXCLUDED.error_message,
                prompt_tokens = EXCLUDED.prompt_tokens,
                completion_tokens = EXCLUDED.completion_tokens,
                total_tokens = EXCLUDED.total_tokens,
                cost_usd = EXCLUDED.cost_usd,
                pii_detections = EXCLUDED.pii_detections,
                input_preview = EXCLUDED.input_preview,
                output_preview = EXCLUDED.output_preview
        """),
        {
            "id": payload["id"],
            "conv_id": payload.get("conversation_id"),
            "session_id": payload.get("session_id"),
            "provider": provider_enum,
            "model": model,
            "status": status_raw,
            "error_type": error_type,
            "error_message": error_message,
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": total_tokens,
            "cost_usd": cost_usd,
            "latency_ms": payload.get("latency_ms", 0),
            "ttft_ms": payload.get("ttft_ms"),
            "streamed": payload.get("streamed", False),
            "input_preview": input_preview,
            "output_preview": output_preview,
            "request_payload": json.dumps(request_payload),
            "response_payload": json.dumps(response_payload),
            "pii_detections": json.dumps(all_detections),
            "sdk_version": payload.get("sdk_version"),
            "environment": payload.get("environment", "dev"),
            "started_at": payload.get("started_at"),
            "ended_at": payload.get("ended_at"),
        },
    )

    # Mark event as processed
    db.execute(
        text("UPDATE inference_events SET status='PROCESSED', processed_at=NOW() WHERE id=:id"),
        {"id": event_id},
    )

    # Publish lean metric to Redis pub/sub for live dashboard
    lean = {
        "id": payload["id"],
        "provider": provider_raw,
        "model": model,
        "status": status_raw.lower(),
        "latency_ms": payload.get("latency_ms", 0),
        "ttft_ms": payload.get("ttft_ms"),
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "cost_usd": cost_usd,
        "conversation_id": payload.get("conversation_id"),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    _publish_metric(lean)
