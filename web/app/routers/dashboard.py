"""Thin proxy to the ingestion API metrics endpoints + SSE forwarding."""
from __future__ import annotations

import asyncio

import httpx
from fastapi import APIRouter, Query, Request
from sse_starlette.sse import EventSourceResponse

from ..config import settings

router = APIRouter(prefix="/api/dashboard")

_HEADERS = {"x-obs-api-key": settings.ingest_api_key}


async def _proxy_get(path: str, params: dict | None = None) -> dict:
    async with httpx.AsyncClient(base_url=settings.ingest_url, timeout=10) as client:
        resp = await client.get(f"/v1{path}", params=params, headers=_HEADERS)
        resp.raise_for_status()
        return resp.json()


@router.get("/summary")
async def summary(range: str = Query("24h")):
    return await _proxy_get("/metrics/summary", {"range": range})


@router.get("/timeseries")
async def timeseries(
    metric: str = Query("requests"),
    range: str = Query("24h"),
    group_by: str = Query("none"),
):
    return await _proxy_get("/metrics/timeseries", {"metric": metric, "range": range, "group_by": group_by})


@router.get("/errors")
async def errors(range: str = Query("24h")):
    return await _proxy_get("/metrics/errors", {"range": range})


@router.get("/logs")
async def logs(
    provider: str | None = None,
    model: str | None = None,
    status: str | None = None,
    limit: int = 50,
    cursor: str | None = None,
):
    params = {"limit": limit}
    if provider:
        params["provider"] = provider
    if model:
        params["model"] = model
    if status:
        params["status"] = status
    if cursor:
        params["cursor"] = cursor
    return await _proxy_get("/logs", params)


@router.get("/logs/{log_id}")
async def log_detail(log_id: str):
    return await _proxy_get(f"/logs/{log_id}")


@router.get("/stream")
async def stream(request: Request):
    """Proxy the ingestion SSE stream to the browser."""

    async def generator():
        try:
            async with httpx.AsyncClient(base_url=settings.ingest_url, timeout=None) as client:
                async with client.stream("GET", "/v1/stream", headers=_HEADERS) as resp:
                    async for line in resp.aiter_lines():
                        if await request.is_disconnected():
                            break
                        if line.startswith("data:"):
                            yield {"event": "log", "data": line[5:].strip()}
        except asyncio.CancelledError:
            pass
        except Exception:
            yield {"event": "error", "data": "stream unavailable"}

    return EventSourceResponse(generator())
