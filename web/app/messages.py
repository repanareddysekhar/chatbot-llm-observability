"""Helpers for building LLM context and persisting chat messages."""
from __future__ import annotations

import uuid

from .models import Message, MessageRole

SYSTEM_PROMPT = "You are a helpful assistant."
HISTORY_LIMIT = 20


def build_llm_messages(history: list[Message], user_message: str) -> list[dict]:
    """Build the message list sent to stream_chat (SDK redacts before the LLM call)."""
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    for msg in history[-HISTORY_LIMIT:]:
        messages.append({"role": msg.role.value.lower(), "content": msg.content})
    messages.append({"role": "user", "content": user_message})
    return messages


def new_user_message(conversation_id: uuid.UUID, content: str) -> Message:
    return Message(
        id=uuid.uuid4(),
        conversation_id=conversation_id,
        role=MessageRole.USER,
        content=content,
    )


def new_assistant_message(
    conversation_id: uuid.UUID,
    content: str,
    inference_log_id: str | None = None,
) -> Message:
    log_id = None
    if inference_log_id and len(inference_log_id) == 36:
        log_id = uuid.UUID(inference_log_id)
    return Message(
        id=uuid.uuid4(),
        conversation_id=conversation_id,
        role=MessageRole.ASSISTANT,
        content=content,
        inference_log_id=log_id,
    )
