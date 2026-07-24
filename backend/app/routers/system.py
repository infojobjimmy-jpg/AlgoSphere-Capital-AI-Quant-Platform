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
    """Aggregate connectivity: Kafka TCP, Redis PING, trading orders table + latency."""
    t0 = time.perf_counter()
    kafka = "OK" if await probe_kafka_tcp(timeout=3.0) else "FAIL"

    redis_st = "FAIL"
    mode = (settings.trading_mode or "paper").strip().lower()
    client = redis.from_url(settings.redis_url, decode_responses=True)
    try:
        if await client.ping():
            redis_st = "OK"
        mode_raw = await client.get(settings.redis_trading_mode_key())
        if mode_raw:
            mode = str(mode_raw).strip().lower()
    except Exception:
        redis_st = "FAIL"
    finally:
        await client.aclose()

    trading = "FAIL"
    last_trade_ts: str | None = None
    table = "live_orders" if mode == "live" else "paper_orders"
    try:
        res = await session.execute(text(f"SELECT max(time) AS mt FROM {table}"))
        row = res.mappings().first()
        if row and row.get("mt") is not None:
            mt = row["mt"]
            last_trade_ts = mt.isoformat() if hasattr(mt, "isoformat") else str(mt)
        trading = "OK"
    except Exception:
        trading = "FAIL"

    latency_ms = round((time.perf_counter() - t0) * 1000.0, 2)
    return {
        "kafka": kafka,
        "redis": redis_st,
        "trading": trading,
        "latency_ms": latency_ms,
        "last_trade_ts": last_trade_ts,
    }
