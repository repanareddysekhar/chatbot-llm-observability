from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_db
from ..models import InferenceEvent, EventStatus
from ..schemas import BatchIngestPayload, BatchIngestResponse, IngestPayload, IngestResponse
from ..tasks import process_inference_log

router = APIRouter()


def _check_api_key(request: Request) -> None:
    from ..config import settings
    key = request.headers.get("x-obs-api-key")
    if key != settings.ingest_api_key:
        raise HTTPException(status_code=401, detail="Invalid API key")


async def _enqueue_one(payload: IngestPayload, db: AsyncSession) -> str:
    event = InferenceEvent(
        id=uuid.uuid4(),
        payload=payload.model_dump(mode="json"),
        status=EventStatus.RECEIVED,
    )
    db.add(event)
    await db.flush()
    # Fire Celery task
    process_inference_log.delay(str(event.id), payload.model_dump(mode="json"))
    return payload.id


@router.post("/ingest", response_model=IngestResponse, status_code=status.HTTP_202_ACCEPTED)
async def ingest_one(
    payload: IngestPayload,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    _check_api_key(request)
    await _enqueue_one(payload, db)
    await db.commit()
    return IngestResponse(id=payload.id)


@router.post("/ingest/batch", response_model=BatchIngestResponse, status_code=status.HTTP_202_ACCEPTED)
async def ingest_batch(
    body: BatchIngestPayload,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    _check_api_key(request)
    accepted = 0
    rejected: list[dict[str, Any]] = []

    for payload in body.events:
        try:
            await _enqueue_one(payload, db)
            accepted += 1
        except Exception as exc:
            rejected.append({"id": payload.id, "error": str(exc)})

    await db.commit()
    return BatchIngestResponse(accepted=accepted, rejected=rejected)
