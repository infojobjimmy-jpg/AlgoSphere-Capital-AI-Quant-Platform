from __future__ import annotations

import secrets
from typing import Any

import redis.asyncio as redis
from fastapi import APIRouter, Header, HTTPException, Query
from pydantic import BaseModel, Field

from app.config import settings
from app.services.economic_calendar import fetch_calendar
from app.services.news_guard import read_state, save_events
from app.services.news_guard_notify import dispatch, format_brief, format_tminus, minutes_until

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


@router.post("/refresh")
async def refresh_calendar():
    try:
        fetched = await fetch_calendar()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"economic_calendar_refresh_failed: {exc}") from exc

    client = redis.from_url(settings.redis_url, decode_responses=True)
    try:
        events = await save_events(client, fetched)
        state = await read_state(client)
        return {
            "provider": settings.news_guard_calendar_provider,
            "count": len(events),
            "state": state,
        }
    finally:
        await client.aclose()


@router.post("/notify")
async def notify(
    kind: str = Query(default="brief", pattern="^(brief|tminus)$"),
    sms: bool = Query(default=False),
    dry_run: bool = Query(default=False),
):
    client = redis.from_url(settings.redis_url, decode_responses=True)
    dedupe_key: str | None = None
    try:
        state = await read_state(client)
        event_id: str | None = None
        if kind == "brief":
            message = format_brief(state)
        else:
            event = state.get("next_event") or {}
            mins = minutes_until(event.get("at"))
            risk = str(event.get("risk") or "").upper()
            if mins is None or risk not in {"HIGH", "CRITICAL"} or not (8.5 <= mins <= 10.5):
                return {"sent": False, "reason": "no_tminus_event", "minutes": mins, "risk": risk}
            event_id = str(event.get("id") or event.get("at") or "event")
            message = format_tminus(state)

        if dry_run:
            return {"sent": False, "dry_run": True, "kind": kind, "message": message, "state": state}

        if kind == "tminus" and event_id:
            dedupe_key = f"{settings.app_slug}:news_guard:tminus_sent:{event_id}"
            claimed = await client.set(dedupe_key, "1", ex=7200, nx=True)
            if not claimed:
                return {"sent": False, "reason": "duplicate", "event_id": event_id}

        try:
            result = await dispatch(message, sms=sms)
        except Exception:
            if dedupe_key:
                await client.delete(dedupe_key)
            raise

        delivered = any(bool(channel.get("sent")) for channel in result.values() if isinstance(channel, dict))
        if not delivered and dedupe_key:
            await client.delete(dedupe_key)
        return {
            "sent": delivered,
            "kind": kind,
            "channels": result,
            "message": message,
            "reason": None if delivered else "no_notification_channel_delivered",
        }
    finally:
        await client.aclose()


@router.get("/mt5")
async def mt5_guard(
    symbol: str | None = Query(default=None),
    x_mt5_token: str = Header(default="", alias="x-mt5-token"),
):
    expected = settings.mt5_api_token or ""
    if not expected:
        raise HTTPException(status_code=503, detail="MT5_API_TOKEN is required")
    if not secrets.compare_digest(x_mt5_token, expected):
        raise HTTPException(status_code=403, detail="Forbidden")

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
