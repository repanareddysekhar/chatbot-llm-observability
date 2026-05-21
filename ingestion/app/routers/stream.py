from __future__ import annotations

import asyncio
import json

from fastapi import APIRouter
from sse_starlette.sse import EventSourceResponse

from ..config import settings

router = APIRouter()


@router.get("/stream")
async def live_stream(request):
    """SSE endpoint that fans out Redis pub/sub messages to the browser."""

    async def event_generator():
        try:
            import redis.asyncio as aioredis
            r = aioredis.from_url(settings.redis_url)
            pubsub = r.pubsub()
            await pubsub.subscribe("metrics.events")

            async for message in pubsub.listen():
                if await request.is_disconnected():
                    break
                if message["type"] == "message":
                    yield {"event": "log", "data": message["data"].decode()}
        except asyncio.CancelledError:
            pass
        except Exception:
            yield {"event": "error", "data": "stream error"}

    return EventSourceResponse(event_generator())
