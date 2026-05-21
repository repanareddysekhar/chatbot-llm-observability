from __future__ import annotations

from fastapi import APIRouter, Query
from sqlalchemy import func, text, select
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import Depends

from ..database import get_db
from ..models import InferenceLog, InferenceStatus

router = APIRouter()

RANGE_TO_INTERVAL = {
    "1h": "1 hour",
    "24h": "24 hours",
    "7d": "7 days",
    "30d": "30 days",
}

RANGE_TO_BUCKET = {
    "1h": "5 minutes",
    "24h": "1 hour",
    "7d": "6 hours",
    "30d": "1 day",
}


@router.get("/metrics/summary")
async def summary(
    range: str = Query("24h", pattern="^(1h|24h|7d|30d)$"),
    db: AsyncSession = Depends(get_db),
):
    interval = RANGE_TO_INTERVAL.get(range, "24 hours")

    result = await db.execute(text(f"""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END) AS errors,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY latency_ms) AS p50,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms) AS p95,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency_ms) AS p99,
            AVG(CASE WHEN ttft_ms IS NOT NULL THEN ttft_ms END) AS avg_ttft,
            SUM(prompt_tokens) AS total_prompt_tokens,
            SUM(completion_tokens) AS total_completion_tokens,
            SUM(cost_usd) AS total_cost
        FROM inference_logs
        WHERE created_at >= NOW() - INTERVAL '{interval}'
    """))
    row = result.mappings().one()

    by_provider = await db.execute(text(f"""
        SELECT
            provider,
            COUNT(*) AS count,
            SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END)::float / COUNT(*) AS error_rate
        FROM inference_logs
        WHERE created_at >= NOW() - INTERVAL '{interval}'
        GROUP BY provider
        ORDER BY count DESC
    """))

    by_model = await db.execute(text(f"""
        SELECT model, COUNT(*) AS count
        FROM inference_logs
        WHERE created_at >= NOW() - INTERVAL '{interval}'
        GROUP BY model
        ORDER BY count DESC
        LIMIT 10
    """))

    total = int(row["total"] or 0)
    errors = int(row["errors"] or 0)

    return {
        "range": range,
        "total_requests": total,
        "error_rate": round(errors / total, 4) if total else 0,
        "p50_latency_ms": int(row["p50"] or 0),
        "p95_latency_ms": int(row["p95"] or 0),
        "p99_latency_ms": int(row["p99"] or 0),
        "avg_ttft_ms": int(row["avg_ttft"] or 0),
        "total_prompt_tokens": int(row["total_prompt_tokens"] or 0),
        "total_completion_tokens": int(row["total_completion_tokens"] or 0),
        "total_cost_usd": float(row["total_cost"] or 0),
        "by_provider": [dict(r) for r in by_provider.mappings()],
        "by_model": [dict(r) for r in by_model.mappings()],
    }


@router.get("/metrics/timeseries")
async def timeseries(
    metric: str = Query("requests", pattern="^(requests|latency_p50|latency_p95|errors|cost|tokens)$"),
    range: str = Query("24h", pattern="^(1h|24h|7d|30d)$"),
    group_by: str = Query("none", pattern="^(provider|model|none)$"),
    db: AsyncSession = Depends(get_db),
):
    interval = RANGE_TO_INTERVAL.get(range, "24 hours")
    bucket = RANGE_TO_BUCKET.get(range, "1 hour")

    metric_expr = {
        "requests": "COUNT(*)",
        "latency_p50": "PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY latency_ms)",
        "latency_p95": "PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms)",
        "errors": "SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END)",
        "cost": "SUM(cost_usd)",
        "tokens": "SUM(total_tokens)",
    }.get(metric, "COUNT(*)")

    if group_by == "none":
        sql = f"""
            SELECT
                date_trunc('{bucket.split()[1]}', date_bin(INTERVAL '{bucket}', created_at, TIMESTAMPTZ '2000-01-01')) AS t,
                {metric_expr} AS v
            FROM inference_logs
            WHERE created_at >= NOW() - INTERVAL '{interval}'
            GROUP BY t ORDER BY t
        """
        result = await db.execute(text(sql))
        points = [{"t": r["t"].isoformat(), "v": float(r["v"] or 0)} for r in result.mappings()]
        return {"bucket": bucket, "series": [{"key": "all", "points": points}]}
    else:
        sql = f"""
            SELECT
                date_trunc('{bucket.split()[1]}', date_bin(INTERVAL '{bucket}', created_at, TIMESTAMPTZ '2000-01-01')) AS t,
                {group_by} AS grp,
                {metric_expr} AS v
            FROM inference_logs
            WHERE created_at >= NOW() - INTERVAL '{interval}'
            GROUP BY t, grp ORDER BY t
        """
        result = await db.execute(text(sql))
        series_map: dict[str, list] = {}
        for r in result.mappings():
            key = str(r["grp"])
            series_map.setdefault(key, []).append({"t": r["t"].isoformat(), "v": float(r["v"] or 0)})
        return {"bucket": bucket, "series": [{"key": k, "points": v} for k, v in series_map.items()]}


@router.get("/metrics/errors")
async def errors_breakdown(
    range: str = Query("24h", pattern="^(1h|24h|7d|30d)$"),
    db: AsyncSession = Depends(get_db),
):
    interval = RANGE_TO_INTERVAL.get(range, "24 hours")
    result = await db.execute(text(f"""
        SELECT error_type, COUNT(*) AS count, MAX(created_at) AS last_seen
        FROM inference_logs
        WHERE status = 'ERROR' AND created_at >= NOW() - INTERVAL '{interval}'
          AND error_type IS NOT NULL
        GROUP BY error_type
        ORDER BY count DESC
    """))
    return [{"error_type": r["error_type"], "count": int(r["count"]), "last_seen": r["last_seen"].isoformat()} for r in result.mappings()]
