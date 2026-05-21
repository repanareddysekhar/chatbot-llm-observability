from __future__ import annotations

import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel

from ..database import get_db
from ..models import Conversation, ConversationStatus, Message, MessageRole, Provider
from .. import cancel_registry

router = APIRouter(prefix="/api/conversations")


def _conv_to_dict(c: Conversation, last_message: str | None = None) -> dict:
    return {
        "id": str(c.id),
        "title": c.title,
        "status": c.status.value,
        "model": c.model,
        "provider": c.provider.value if c.provider else None,
        "created_at": c.created_at.isoformat(),
        "updated_at": c.updated_at.isoformat(),
        "last_message_preview": last_message,
    }


def _msg_to_dict(m: Message) -> dict:
    return {
        "id": str(m.id),
        "role": m.role.value,
        "content": m.content,
        "created_at": m.created_at.isoformat(),
        "inference_log_id": str(m.inference_log_id) if m.inference_log_id else None,
    }


@router.get("")
async def list_conversations(
    status: str | None = None,
    limit: int = 50,
    db: AsyncSession = Depends(get_db),
):
    q = select(Conversation).order_by(desc(Conversation.updated_at)).limit(limit)
    if status:
        q = q.where(Conversation.status == status.upper())
    else:
        q = q.where(Conversation.status != ConversationStatus.ARCHIVED)

    result = await db.execute(q)
    convs = result.scalars().all()
    return [_conv_to_dict(c) for c in convs]


class CreateConvRequest(BaseModel):
    title: str | None = None
    model: str | None = None
    provider: str | None = None


@router.post("", status_code=201)
async def create_conversation(body: CreateConvRequest, db: AsyncSession = Depends(get_db)):
    conv = Conversation(
        id=uuid.uuid4(),
        title=body.title or "New conversation",
        model=body.model,
        provider=Provider[body.provider.upper()] if body.provider else None,
        status=ConversationStatus.ACTIVE,
    )
    db.add(conv)
    await db.commit()
    await db.refresh(conv)
    return _conv_to_dict(conv)


@router.get("/{conv_id}")
async def get_conversation(conv_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Conversation).where(Conversation.id == uuid.UUID(conv_id))
    )
    conv = result.scalar_one_or_none()
    if not conv:
        raise HTTPException(status_code=404, detail="Conversation not found")

    msgs_result = await db.execute(
        select(Message)
        .where(Message.conversation_id == conv.id)
        .order_by(Message.created_at)
    )
    messages = msgs_result.scalars().all()
    d = _conv_to_dict(conv)
    d["messages"] = [_msg_to_dict(m) for m in messages]
    return d


@router.delete("/{conv_id}")
async def archive_conversation(conv_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Conversation).where(Conversation.id == uuid.UUID(conv_id)))
    conv = result.scalar_one_or_none()
    if not conv:
        raise HTTPException(status_code=404, detail="Not found")
    conv.status = ConversationStatus.ARCHIVED
    conv.updated_at = datetime.now(timezone.utc)
    await db.commit()
    return {"archived": True}


class CancelRequest(BaseModel):
    inference_log_id: str | None = None


@router.post("/{conv_id}/cancel")
async def cancel_conversation(conv_id: str, body: CancelRequest | None = None):
    log_id = body.inference_log_id if body else None
    if log_id:
        cancelled = cancel_registry.cancel(log_id)
        return {"cancelled": cancelled, "inference_log_id": log_id}
    return {"cancelled": False, "detail": "No inference_log_id provided"}
