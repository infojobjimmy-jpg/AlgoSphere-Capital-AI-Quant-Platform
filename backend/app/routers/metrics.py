import json

import redis.asyncio as redis
from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db.session import get_session
from app.memory.chroma_memory import query_similar, trend_from_performance

router = APIRouter()


class MemoryQuery(BaseModel):
    text: str = Field(..., min_length=2, max_length=8000)
    k: int = Field(8, ge=1, le=30)


@router.get("/timeline")
async def timeline(limit: int = 30) -> dict[str, list]:
    client = redis.from_url(settings.redis_url, decode_responses=True)
    try:
        items = await client.lrange(settings.redis_timeline_key(), 0, max(0, min(limit, 200) - 1))
        frames = []
        for raw in reversed(items):
            try:
                frames.append(json.loads(raw))
            except json.JSONDecodeError:
                continue
        return {"frames": frames}
    finally:
        await client.aclose()


@router.get("/fusion-events")
async def fusion_events(limit: int = 50, session: AsyncSession = Depends(get_session)):
    lim = max(1, min(limit, 200))
    res = await session.execute(
        text(
            """
            SELECT time, h3_cell, event_type, severity, summary, payload
            FROM fusion_events
            ORDER BY time DESC
            LIMIT :lim
            """
        ),
        {"lim": lim},
    )
    rows = res.mappings().all()
    return {
        "events": [
            {
                "time": r["time"].isoformat(),
                "h3_cell": r["h3_cell"],
                "event_type": r["event_type"],
                "severity": r["severity"],
                "summary": r["summary"],
                "payload": r["payload"],
            }
            for r in rows
        ]
    }


@router.post("/memory/similar")
async def memory_similar(body: MemoryQuery):
    hits = query_similar(body.text, k=body.k)
    return {"hits": hits}


@router.get("/history-compare")
async def history_compare(
    events_limit: int = 40,
    decisions_limit: int = 40,
    perf_limit: int = 60,
    session: AsyncSession = Depends(get_session),
):
    el = max(1, min(events_limit, 200))
    dl = max(1, min(decisions_limit, 200))
    res_e = await session.execute(
        text(
            """
            SELECT time, h3_cell, event_type, severity, summary
            FROM fusion_events
            ORDER BY time DESC
            LIMIT :lim
            """
        ),
        {"lim": el},
    )
    res_d = await session.execute(
        text(
            """
            SELECT time, event_id, severity, action, confidence
            FROM decisions
            ORDER BY time DESC
            LIMIT :lim
            """
        ),
        {"lim": dl},
    )
    res_p = await session.execute(
        text(
            """
            SELECT time, model_name, metric_name, metric_value
            FROM model_performance
            WHERE model_name = 'adaptive' AND metric_name = 'air_density_z'
            ORDER BY time DESC
            LIMIT :lim
            """
        ),
        {"lim": max(1, min(perf_limit, 500))},
    )
    perf_rows = [
        {
            "time": r["time"].isoformat(),
            "model_name": r["model_name"],
            "metric_name": r["metric_name"],
            "metric_value": float(r["metric_value"]),
        }
        for r in res_p.mappings().all()
    ]
    return {
        "fusion_events": [
            {
                "time": r["time"].isoformat(),
                "h3_cell": r["h3_cell"],
                "event_type": r["event_type"],
                "severity": r["severity"],
                "summary": r["summary"],
            }
            for r in res_e.mappings().all()
        ],
        "decisions": [
            {
                "time": r["time"].isoformat(),
                "event_id": r["event_id"],
                "severity": r["severity"],
                "action": r["action"],
                "confidence": float(r["confidence"]),
            }
            for r in res_d.mappings().all()
        ],
        "adaptive_threshold_trend": trend_from_performance(list(reversed(perf_rows))),
    }


@router.get("/counters")
async def counters() -> dict:
    client = redis.from_url(settings.redis_url, decode_responses=True)
    try:
        raw = await client.get(settings.redis_snapshot_key())
        if not raw:
            return {}
        snap = json.loads(raw)
        layers = snap.get("layers", {})
        return {
            "aircraft": len(layers.get("aircraft", [])),
            "satellites": len(layers.get("satellites", [])),
            "ships": len(layers.get("ships", [])),
            "cameras": len(layers.get("cameras", [])),
            "weather_cells": len(layers.get("weather", [])),
            "updated_at": snap.get("meta", {}).get("updated_at"),
        }
    finally:
        await client.aclose()
