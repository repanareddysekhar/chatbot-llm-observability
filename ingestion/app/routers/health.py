from fastapi import APIRouter
from sqlalchemy import text

from ..database import AsyncSessionLocal
from ..config import settings

router = APIRouter()


@router.get("/health")
async def health():
    db_ok = False
    redis_ok = False

    try:
        async with AsyncSessionLocal() as db:
            await db.execute(text("SELECT 1"))
        db_ok = True
    except Exception:
        pass

    try:
        import redis.asyncio as aioredis
        r = aioredis.from_url(settings.redis_url)
        await r.ping()
        await r.aclose()
        redis_ok = True
    except Exception:
        pass

    return {"ok": db_ok and redis_ok, "db": "up" if db_ok else "down", "redis": "up" if redis_ok else "down"}
