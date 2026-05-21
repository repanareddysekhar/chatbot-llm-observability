from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import settings
from ..database import get_db
from ..llm.factory import stream_chat, _new_id
from ..models import Conversation, ConversationStatus, Message, MessageRole, Provider
from .. import cancel_registry

try:
    from llm_obs import ObservabilityClient
    _obs = ObservabilityClient(endpoint=settings.ingest_url, api_key=settings.ingest_api_key)
except ImportError:
    _obs = None

router = APIRouter(prefix="/api")


class ChatRequest(BaseModel):
    conversation_id: str | None = None
    message: str
    provider: str = "openai"
    model: str = "gpt-4o-mini"


@router.post("/chat")
async def chat(body: ChatRequest, db: AsyncSession = Depends(get_db)):
    """SSE streaming chat endpoint."""

    async def generate():
        # Load or create conversation
        conv_id = body.conversation_id
        if conv_id:
            result = await db.execute(
                select(Conversation).where(Conversation.id == uuid.UUID(conv_id))
            )
            conv = result.scalar_one_or_none()
        else:
            conv = None

        if not conv:
            conv = Conversation(
                id=uuid.uuid4(),
                title=body.message[:60],
                model=body.model,
                provider=Provider[body.provider.upper()] if body.provider in ("openai", "anthropic", "google") else None,
                status=ConversationStatus.ACTIVE,
            )
            db.add(conv)
            await db.flush()

        # Load history
        msgs_result = await db.execute(
            select(Message)
            .where(Message.conversation_id == conv.id)
            .order_by(Message.created_at)
        )
        history = msgs_result.scalars().all()

        # Build messages list
        llm_messages = [
            {"role": "system", "content": "You are a helpful assistant."}
        ]
        for m in history[-20:]:  # keep last 20 for context
            llm_messages.append({"role": m.role.value.lower(), "content": m.content})
        llm_messages.append({"role": "user", "content": body.message})

        # Persist user message
        user_msg = Message(
            id=uuid.uuid4(),
            conversation_id=conv.id,
            role=MessageRole.USER,
            content=body.message,
        )
        db.add(user_msg)
        await db.flush()

        inference_log_id = _new_id()
        cancel_event = cancel_registry.register(inference_log_id)

        # Send meta event
        yield {
            "event": "meta",
            "data": json.dumps({
                "conversation_id": str(conv.id),
                "inference_log_id": inference_log_id,
            }),
        }

        # Stream from LLM
        full_response = []
        try:
            async for chunk in stream_chat(
                provider=body.provider,
                model=body.model,
                messages=llm_messages,
                obs_client=_obs,
                conversation_id=str(conv.id),
                cancel_event=cancel_event,
            ):
                full_response.append(chunk)
                yield {"event": "token", "data": json.dumps({"delta": chunk})}

                if cancel_event.is_set():
                    yield {"event": "cancelled", "data": "{}"}
                    break

        except Exception as exc:
            yield {"event": "error", "data": json.dumps({"message": str(exc)[:300]})}
        finally:
            cancel_registry.unregister(inference_log_id)

        # Persist assistant message
        assistant_content = "".join(full_response)
        if assistant_content:
            asst_msg = Message(
                id=uuid.uuid4(),
                conversation_id=conv.id,
                role=MessageRole.ASSISTANT,
                content=assistant_content,
                inference_log_id=uuid.UUID(inference_log_id) if len(inference_log_id) == 36 else None,
            )
            db.add(asst_msg)

        conv.updated_at = datetime.now(timezone.utc)
        await db.commit()

        yield {"event": "done", "data": "{}"}

    return EventSourceResponse(generate())
