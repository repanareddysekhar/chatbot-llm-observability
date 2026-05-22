import enum
import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger, Boolean, DateTime, Enum, ForeignKey,
    Integer, Numeric, String, Text, func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .database import Base


def _now() -> datetime:
    return datetime.now(timezone.utc)


class ConversationStatus(str, enum.Enum):
    ACTIVE = "ACTIVE"
    CANCELLED = "CANCELLED"
    COMPLETED = "COMPLETED"
    ARCHIVED = "ARCHIVED"


class MessageRole(str, enum.Enum):
    USER = "USER"
    ASSISTANT = "ASSISTANT"
    SYSTEM = "SYSTEM"


class InferenceStatus(str, enum.Enum):
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"
    CANCELLED = "CANCELLED"
    TIMEOUT = "TIMEOUT"


class Provider(str, enum.Enum):
    OPENAI = "OPENAI"
    ANTHROPIC = "ANTHROPIC"
    GOOGLE = "GOOGLE"
    OTHER = "OTHER"


class EventStatus(str, enum.Enum):
    RECEIVED = "RECEIVED"
    PROCESSED = "PROCESSED"
    FAILED = "FAILED"


PROVIDER_MAP = {
    "openai": Provider.OPENAI,
    "anthropic": Provider.ANTHROPIC,
    "google": Provider.GOOGLE,
}


class Conversation(Base):
    __tablename__ = "conversations"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    user_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    status: Mapped[ConversationStatus] = mapped_column(
        Enum(ConversationStatus, name="conversationstatus"), default=ConversationStatus.ACTIVE
    )
    model: Mapped[str | None] = mapped_column(String(100), nullable=True)
    provider: Mapped[Provider | None] = mapped_column(Enum(Provider, name="provider"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, onupdate=_now)
    metadata_: Mapped[dict | None] = mapped_column("metadata", JSONB, nullable=True)

    messages: Mapped[list["Message"]] = relationship("Message", back_populates="conversation", cascade="all, delete")
    inference_logs: Mapped[list["InferenceLog"]] = relationship("InferenceLog", back_populates="conversation")


class Message(Base):
    __tablename__ = "messages"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    conversation_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("conversations.id", ondelete="CASCADE"), nullable=False
    )
    role: Mapped[MessageRole] = mapped_column(Enum(MessageRole, name="messagerole"), nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    inference_log_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    metadata_: Mapped[dict | None] = mapped_column("metadata", JSONB, nullable=True)

    conversation: Mapped["Conversation"] = relationship("Conversation", back_populates="messages")


class InferenceLog(Base):
    __tablename__ = "inference_logs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    conversation_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("conversations.id"), nullable=True
    )
    session_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    provider: Mapped[Provider] = mapped_column(Enum(Provider, name="provider"), nullable=False)
    model: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[InferenceStatus] = mapped_column(Enum(InferenceStatus, name="inferencestatus"), nullable=False)
    error_type: Mapped[str | None] = mapped_column(String(100), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    prompt_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    completion_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    total_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    cost_usd: Mapped[float | None] = mapped_column(Numeric(12, 6), nullable=True)
    latency_ms: Mapped[int] = mapped_column(Integer, nullable=False)
    ttft_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    streamed: Mapped[bool] = mapped_column(Boolean, default=False)
    input_preview: Mapped[str | None] = mapped_column(Text, nullable=True)
    output_preview: Mapped[str | None] = mapped_column(Text, nullable=True)
    request_payload: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    response_payload: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    pii_detections: Mapped[list | None] = mapped_column(JSONB, nullable=True)
    sdk_version: Mapped[str | None] = mapped_column(String(20), nullable=True)
    environment: Mapped[str] = mapped_column(String(20), default="dev")
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ended_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    metadata_: Mapped[dict | None] = mapped_column("metadata", JSONB, nullable=True)

    conversation: Mapped["Conversation | None"] = relationship("Conversation", back_populates="inference_logs")


class InferenceEvent(Base):
    __tablename__ = "inference_events"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    status: Mapped[EventStatus] = mapped_column(
        Enum(EventStatus, name="eventstatus"), default=EventStatus.RECEIVED
    )
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)


class MetricRollup(Base):
    __tablename__ = "metric_rollups"

    bucket: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)
    provider: Mapped[str] = mapped_column(String(20), primary_key=True)
    model: Mapped[str] = mapped_column(String(100), primary_key=True)
    count_total: Mapped[int] = mapped_column(Integer, default=0)
    count_error: Mapped[int] = mapped_column(Integer, default=0)
    sum_latency_ms: Mapped[int] = mapped_column(BigInteger, default=0)
    sum_ttft_ms: Mapped[int] = mapped_column(BigInteger, default=0)
    sum_prompt_tokens: Mapped[int] = mapped_column(BigInteger, default=0)
    sum_completion_tokens: Mapped[int] = mapped_column(BigInteger, default=0)
    sum_cost_usd: Mapped[float] = mapped_column(Numeric(14, 6), default=0)
