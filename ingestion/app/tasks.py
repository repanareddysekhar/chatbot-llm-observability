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
        (prompt_tokens or 0) + (completion_tokens or 0)
        if prompt_tokens or completion_tokens else None
    )

    # PII redaction — safety net (SDK already redacts before shipping)
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
    input_text = (
        " ".join(m.get("content", "") for m in messages if isinstance(m, dict))
        or request_payload.get("prompt", "")
    )
    input_preview = input_text[:256] if input_text else None
    output_preview = (response_payload.get("content") or "")[:256] or None

    # Input hash for deduplication / cache analysis
    input_hash = hashlib.sha256(
        json.dumps(request_payload, sort_keys=True).encode()
    ).hexdigest()

    # Cost — always comes pre-computed from the SDK (sdk/llm_obs/metrics/cost.py)
    cost_usd = payload.get("cost_usd")

    # Provider enum mapping
    provider_map = {
        "openai": "OPENAI", "anthropic": "ANTHROPIC",
        "google": "GOOGLE", "ollama": "OTHER",
        "bedrock": "OTHER", "openai_compatible": "OTHER",
    }
    provider_enum = provider_map.get(provider_raw, "OTHER")

    error = payload.get("error") or {}
    error_type = error.get("type") if isinstance(error, dict) else None
    error_message = error.get("message") if isinstance(error, dict) else None

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
                status            = EXCLUDED.status,
                error_type        = EXCLUDED.error_type,
                error_message     = EXCLUDED.error_message,
                prompt_tokens     = EXCLUDED.prompt_tokens,
                completion_tokens = EXCLUDED.completion_tokens,
                total_tokens      = EXCLUDED.total_tokens,
                cost_usd          = EXCLUDED.cost_usd,
                pii_detections    = EXCLUDED.pii_detections,
                input_preview     = EXCLUDED.input_preview,
                output_preview    = EXCLUDED.output_preview
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

    db.execute(
        text("UPDATE inference_events SET status='PROCESSED', processed_at=NOW() WHERE id=:id"),
        {"id": event_id},
    )

    _publish_metric({
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
    })
