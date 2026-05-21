from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_db
from ..models import InferenceLog

router = APIRouter()


def _log_to_dict(log: InferenceLog) -> dict:
    return {
        "id": log.id,
        "conversation_id": str(log.conversation_id) if log.conversation_id else None,
        "provider": log.provider.value,
        "model": log.model,
        "status": log.status.value,
        "latency_ms": log.latency_ms,
        "ttft_ms": log.ttft_ms,
        "prompt_tokens": log.prompt_tokens,
        "completion_tokens": log.completion_tokens,
        "total_tokens": log.total_tokens,
        "cost_usd": float(log.cost_usd) if log.cost_usd is not None else None,
        "streamed": log.streamed,
        "input_preview": log.input_preview,
        "output_preview": log.output_preview,
        "error_type": log.error_type,
        "environment": log.environment,
        "started_at": log.started_at.isoformat(),
        "ended_at": log.ended_at.isoformat(),
        "created_at": log.created_at.isoformat(),
    }


def _log_detail(log: InferenceLog) -> dict:
    d = _log_to_dict(log)
    d["request_payload"] = log.request_payload
    d["response_payload"] = log.response_payload
    d["pii_detections"] = log.pii_detections
    return d


@router.get("/logs")
async def list_logs(
    provider: str | None = None,
    model: str | None = None,
    status: str | None = None,
    conversation_id: str | None = None,
    limit: int = Query(50, le=200),
    cursor: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    q = select(InferenceLog).order_by(desc(InferenceLog.created_at))

    if provider:
        q = q.where(InferenceLog.provider == provider.upper())
    if model:
        q = q.where(InferenceLog.model == model)
    if status:
        q = q.where(InferenceLog.status == status.upper())
    if conversation_id:
        q = q.where(InferenceLog.conversation_id == conversation_id)
    if cursor:
        q = q.where(InferenceLog.created_at < cursor)

    q = q.limit(limit + 1)
    result = await db.execute(q)
    logs = result.scalars().all()

    next_cursor = None
    if len(logs) > limit:
        next_cursor = logs[limit - 1].created_at.isoformat()
        logs = logs[:limit]

    return {"items": [_log_to_dict(l) for l in logs], "next_cursor": next_cursor}


@router.get("/logs/{log_id}")
async def get_log(log_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(InferenceLog).where(InferenceLog.id == log_id))
    log = result.scalar_one_or_none()
    if not log:
        raise HTTPException(status_code=404, detail="Log not found")
    return _log_detail(log)
