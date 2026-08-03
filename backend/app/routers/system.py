from __future__ import annotations

import time
from typing import Any

import redis.asyncio as redis
from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db.session import get_session
from app.kafka_bus import probe_kafka_tcp

router = APIRouter()


@router.get("/health")
async def system_health(session: AsyncSession = Depends(get_session)) -> dict[str, Any]:
    """Aggregate connectivity: Kafka TCP, Redis PING, DB + latency."""
    t0 = time.perf_counter()
    kafka = "OK" if await probe_kafka_tcp(timeout=3.0) else "FAIL"

    redis_st = "FAIL"
    client = redis.from_url(settings.redis_url, decode_responses=True)
    try:
        if await client.ping():
            redis_st = "OK"
    except Exception:
        redis_st = "FAIL"
    finally:
        await client.aclose()

    db_st = "FAIL"
    try:
        await session.execute(text("SELECT 1"))
        db_st = "OK"
    except Exception:
        db_st = "FAIL"

    latency_ms = round((time.perf_counter() - t0) * 1000.0, 2)
    return {
        "kafka": kafka,
        "redis": redis_st,
        "db": db_st,
        "latency_ms": latency_ms,
    }
