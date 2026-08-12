from __future__ import annotations

from typing import Any

import redis.asyncio as redis
from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

from app.config import settings
from app.services.news_guard import read_state, save_events

router = APIRouter()


class EconomicEvent(BaseModel):
    id: str | None = None
    title: str
    at: str
    risk: str = "MODERATE"
    currencies: list[str] = Field(default_factory=list)
    symbols: list[str] = Field(default_factory=list)
    off_before_min: int | None = None
    off_after_min: int | None = None
    source: str = "external"
    meta: dict[str, Any] = Field(default_factory=dict)


class EventBatch(BaseModel):
    events: list[EconomicEvent]


@router.get("/status")
async def status(symbol: str | None = Query(default=None)):
    client = redis.from_url(settings.redis_url, decode_responses=True)
    try:
        return await read_state(client, symbol=symbol)
    finally:
        await client.aclose()


@router.post("/events")
async def replace_events(body: EventBatch):
    client = redis.from_url(settings.redis_url, decode_responses=True)
    try:
        events = await save_events(client, [event.model_dump(exclude_none=True) for event in body.events])
        state = await read_state(client)
        return {"count": len(events), "state": state}
    finally:
        await client.aclose()


@router.get("/mt5")
async def mt5_guard(symbol: str | None = Query(default=None)):
    client = redis.from_url(settings.redis_url, decode_responses=True)
    try:
        state = await read_state(client, symbol=symbol)
    finally:
        await client.aclose()
    return {
        "allow_new_entries": state["status"] == "ON",
        "news_guard": state["status"],
        "risk": state["risk"],
        "symbol": state.get("symbol"),
        "next_event": state.get("next_event"),
        "checked_at": state["checked_at"],
    }
