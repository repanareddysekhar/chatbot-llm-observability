from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class MessagePayload(BaseModel):
    role: str
    content: str
    name: str | None = None


class RequestPayload(BaseModel):
    messages: list[MessagePayload] | None = None
    prompt: str | None = None
    temperature: float | None = None
    max_tokens: int | None = None
    extra: dict[str, Any] | None = None


class ResponsePayload(BaseModel):
    content: str | None = None
    finish_reason: str | None = None


class UsagePayload(BaseModel):
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    total_tokens: int | None = None


class ErrorPayload(BaseModel):
    type: str
    message: str
    code: str | None = None


class IngestPayload(BaseModel):
    id: str = Field(..., min_length=10)
    conversation_id: str | None = None
    session_id: str | None = None
    provider: str
    model: str
    status: str
    started_at: datetime
    ended_at: datetime
    latency_ms: int = Field(..., ge=0)
    ttft_ms: int | None = None
    streamed: bool = False
    request: RequestPayload
    response: ResponsePayload | None = None
    usage: UsagePayload | None = None
    # SDK computes cost_usd before shipping — worker uses this if present
    cost_usd: float | None = None
    # PII redacted in the SDK before ingest — worker stores as-is
    pii_detections: list[dict[str, Any]] | None = None
    error: ErrorPayload | None = None
    sdk_version: str | None = None
    environment: str = "dev"
    metadata: dict[str, Any] | None = None

    @field_validator("provider")
    @classmethod
    def validate_provider(cls, v: str) -> str:
        return v.lower()

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: str) -> str:
        return v.lower()


class BatchIngestPayload(BaseModel):
    events: list[IngestPayload] = Field(..., max_length=100)


class IngestResponse(BaseModel):
    id: str
    queued: bool = True


class BatchIngestResponse(BaseModel):
    accepted: int
    rejected: list[dict[str, Any]] = []
