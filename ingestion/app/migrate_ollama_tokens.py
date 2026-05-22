"""
Backfill migration for existing Ollama inference_logs rows that have
NULL prompt_tokens / completion_tokens / cost_usd.

Since the actual API token counts were not captured in the original code,
we estimate using the stored input_preview / output_preview text:
  - 1 token ≈ 4 characters (standard English approximation)
  - cost computed from the same model-tier price table used in tasks.py

Run with:
    docker compose exec ingestion python -m app.migrate_ollama_tokens
"""

from __future__ import annotations

import logging
from sqlalchemy import create_engine, text
from .config import settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger(__name__)

_OLLAMA_PRICE_TABLE: dict[str, dict[str, float]] = {
    # Sub-2B
    "gemma3:1b":        {"input": 0.02 / 1e6, "output": 0.02 / 1e6},
    "llama3.2:1b":      {"input": 0.02 / 1e6, "output": 0.02 / 1e6},
    "phi3:mini":        {"input": 0.02 / 1e6, "output": 0.02 / 1e6},
    "gemma2:2b":        {"input": 0.02 / 1e6, "output": 0.02 / 1e6},
    "deepseek-r1:1.5b": {"input": 0.02 / 1e6, "output": 0.02 / 1e6},
    # 4B–8B
    "gemma3:4b":        {"input": 0.08 / 1e6, "output": 0.08 / 1e6},
    "gemma2:9b":        {"input": 0.08 / 1e6, "output": 0.08 / 1e6},
    "llama3.2":         {"input": 0.08 / 1e6, "output": 0.08 / 1e6},
    "llama3.1:8b":      {"input": 0.08 / 1e6, "output": 0.08 / 1e6},
    "mistral":          {"input": 0.08 / 1e6, "output": 0.08 / 1e6},
    "mistral:7b":       {"input": 0.08 / 1e6, "output": 0.08 / 1e6},
    "phi3":             {"input": 0.08 / 1e6, "output": 0.08 / 1e6},
    "qwen2.5:7b":       {"input": 0.08 / 1e6, "output": 0.08 / 1e6},
    "deepseek-r1:7b":   {"input": 0.08 / 1e6, "output": 0.08 / 1e6},
    "codellama:7b":     {"input": 0.08 / 1e6, "output": 0.08 / 1e6},
    "codellama":        {"input": 0.08 / 1e6, "output": 0.08 / 1e6},
    # 12B+
    "gemma3:12b":       {"input": 0.20 / 1e6, "output": 0.20 / 1e6},
    "llama3.1":         {"input": 0.20 / 1e6, "output": 0.20 / 1e6},
    "mistral-nemo":     {"input": 0.20 / 1e6, "output": 0.20 / 1e6},
    "qwen2.5":          {"input": 0.20 / 1e6, "output": 0.20 / 1e6},
}
_DEFAULT_PRICE = {"input": 0.08 / 1e6, "output": 0.08 / 1e6}

CHARS_PER_TOKEN = 4  # standard English approximation


def _estimate_tokens(text: str | None) -> int:
    if not text:
        return 0
    return max(1, len(text) // CHARS_PER_TOKEN)


def _get_prices(model: str) -> dict:
    base = model.lower().split(":latest")[0]
    return _OLLAMA_PRICE_TABLE.get(base) or _OLLAMA_PRICE_TABLE.get(model.lower()) or _DEFAULT_PRICE


def run() -> None:
    sync_url = settings.database_url.replace("postgresql+asyncpg", "postgresql+psycopg2")
    engine = create_engine(sync_url)

    with engine.begin() as conn:
        # Fetch all Ollama (OTHER provider) rows missing token/cost data
        rows = conn.execute(text("""
            SELECT id, model, input_preview, output_preview
            FROM inference_logs
            WHERE provider = 'OTHER'
              AND (prompt_tokens IS NULL OR cost_usd IS NULL)
        """)).fetchall()

        if not rows:
            logger.info("No rows to migrate — all Ollama logs already have token/cost data.")
            return

        logger.info("Found %d Ollama rows to backfill...", len(rows))

        updated = 0
        for row in rows:
            log_id, model, input_preview, output_preview = row

            prompt_tokens  = _estimate_tokens(input_preview)
            completion_tokens = _estimate_tokens(output_preview)
            total_tokens   = prompt_tokens + completion_tokens

            prices = _get_prices(model or "")
            cost_usd = prices["input"] * prompt_tokens + prices["output"] * completion_tokens

            conn.execute(text("""
                UPDATE inference_logs
                SET
                    prompt_tokens     = :prompt_tokens,
                    completion_tokens = :completion_tokens,
                    total_tokens      = :total_tokens,
                    cost_usd          = :cost_usd
                WHERE id = :id
                  AND (prompt_tokens IS NULL OR cost_usd IS NULL)
            """), {
                "id": log_id,
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens": total_tokens,
                "cost_usd": round(cost_usd, 8),
            })
            updated += 1

        logger.info("Done. Backfilled %d rows.", updated)
        logger.info("Note: token counts are estimated (~%d chars/token). "
                    "Future Ollama calls will capture real counts from the API.", CHARS_PER_TOKEN)


if __name__ == "__main__":
    run()
