"""Seed the database with synthetic inference logs for dashboard demo."""
from __future__ import annotations

import asyncio
import random
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from .database import AsyncSessionLocal
from .models import (
    Conversation, ConversationStatus, InferenceLog, InferenceStatus, Provider,
)

PROVIDERS = ["OPENAI", "ANTHROPIC", "GOOGLE"]
MODELS = {
    "OPENAI": ["gpt-4o-mini", "gpt-4o", "gpt-4.1-mini"],
    "ANTHROPIC": ["claude-3-5-haiku-latest", "claude-3-5-sonnet-latest"],
    "GOOGLE": ["gemini-1.5-flash", "gemini-1.5-pro"],
}
TOPICS = [
    "How do I implement a binary search tree?",
    "Explain quantum computing in simple terms",
    "Write a Python function to parse JSON",
    "What are the best practices for REST API design?",
    "Help me debug this SQL query",
    "Summarize the SOLID principles",
    "How does the attention mechanism in transformers work?",
    "Generate a weekly meal plan",
    "What are the pros and cons of microservices?",
    "Write unit tests for a FastAPI endpoint",
]


async def seed() -> None:
    async with AsyncSessionLocal() as db:
        # Create some conversations
        conversations = []
        for i in range(10):
            conv = Conversation(
                id=uuid.uuid4(),
                title=TOPICS[i],
                status=ConversationStatus.ACTIVE,
                model=random.choice(MODELS["OPENAI"]),
                provider=Provider.OPENAI,
            )
            db.add(conv)
            conversations.append(conv)
        await db.flush()

        # Create 200 synthetic inference logs spread over last 7 days
        now = datetime.now(timezone.utc)
        logs = []
        for i in range(200):
            provider = random.choice(PROVIDERS)
            model = random.choice(MODELS[provider])
            offset_hours = random.uniform(0, 168)  # 7 days
            started = now - timedelta(hours=offset_hours)
            latency = random.randint(200, 5000)
            ended = started + timedelta(milliseconds=latency)
            status = random.choices(
                ["SUCCESS", "ERROR", "CANCELLED"],
                weights=[88, 9, 3],
            )[0]
            prompt_tokens = random.randint(50, 2000)
            completion_tokens = random.randint(20, 800)

            error_types = [None, None, None, "rate_limit", "context_length", "timeout"]
            error_type = random.choice(error_types) if status == "ERROR" else None

            price_table = {
                "OPENAI:gpt-4o-mini": (0.15e-6, 0.60e-6),
                "OPENAI:gpt-4o": (2.50e-6, 10.00e-6),
                "OPENAI:gpt-4.1-mini": (0.40e-6, 1.60e-6),
                "ANTHROPIC:claude-3-5-haiku-latest": (0.80e-6, 4.00e-6),
                "ANTHROPIC:claude-3-5-sonnet-latest": (3.00e-6, 15.00e-6),
                "GOOGLE:gemini-1.5-flash": (0.075e-6, 0.30e-6),
                "GOOGLE:gemini-1.5-pro": (1.25e-6, 5.00e-6),
            }
            price_key = f"{provider}:{model}"
            prices = price_table.get(price_key, (1e-6, 2e-6))
            cost = prices[0] * prompt_tokens + prices[1] * completion_tokens

            log = InferenceLog(
                id=str(uuid.uuid4()),
                conversation_id=random.choice(conversations).id if random.random() > 0.3 else None,
                provider=Provider[provider],
                model=model,
                status=InferenceStatus[status],
                error_type=error_type,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                total_tokens=prompt_tokens + completion_tokens,
                cost_usd=cost,
                latency_ms=latency,
                ttft_ms=random.randint(80, 500) if random.random() > 0.3 else None,
                streamed=random.random() > 0.4,
                input_preview=random.choice(TOPICS)[:256],
                output_preview="Here is a detailed explanation..." + "x" * random.randint(0, 100),
                environment="dev",
                started_at=started,
                ended_at=ended,
            )
            logs.append(log)
            db.add(log)

        await db.commit()
        print(f"Seeded {len(conversations)} conversations and {len(logs)} inference logs.")


if __name__ == "__main__":
    asyncio.run(seed())
