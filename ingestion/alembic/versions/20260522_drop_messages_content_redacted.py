"""drop messages content_redacted

Revision ID: 20260522_drop_content_redacted
Revises:
Create Date: 2026-05-22
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "20260522_drop_content_redacted"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if not inspector.has_table("messages"):
        return
    if any(col["name"] == "content_redacted" for col in inspector.get_columns("messages")):
        op.drop_column("messages", "content_redacted")


def downgrade() -> None:
    op.add_column("messages", sa.Column("content_redacted", sa.Text(), nullable=True))
