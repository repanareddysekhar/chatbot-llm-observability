"""Mirror of ingestion models — web service only needs Conversation and Message."""
import enum
import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, Enum, ForeignKey, Integer, String, Text
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


class Provider(str, enum.Enum):
    OPENAI = "OPENAI"
    ANTHROPIC = "ANTHROPIC"
    GOOGLE = "GOOGLE"
    OTHER = "OTHER"


class Conversation(Base):
    __tablename__ = "conversations"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    user_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    status: Mapped[ConversationStatus] = mapped_column(
        Enum(ConversationStatus, name="conversationstatus", create_type=False),
        default=ConversationStatus.ACTIVE,
    )
    model: Mapped[str | None] = mapped_column(String(100), nullable=True)
    provider: Mapped[Provider | None] = mapped_column(
        Enum(Provider, name="provider", create_type=False), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, onupdate=_now)
    metadata_: Mapped[dict | None] = mapped_column("metadata", JSONB, nullable=True)

    messages: Mapped[list["Message"]] = relationship("Message", back_populates="conversation", cascade="all, delete")


class Message(Base):
    __tablename__ = "messages"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    conversation_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("conversations.id", ondelete="CASCADE"), nullable=False
    )
    role: Mapped[MessageRole] = mapped_column(
        Enum(MessageRole, name="messagerole", create_type=False), nullable=False
    )
    content: Mapped[str] = mapped_column(Text, nullable=False)
    content_redacted: Mapped[str | None] = mapped_column(Text, nullable=True)
    tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    inference_log_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)

    conversation: Mapped["Conversation"] = relationship("Conversation", back_populates="messages")
