from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

from app.config import settings

RISK_ORDER = {"LOW": 0, "MODERATE": 1, "HIGH": 2, "CRITICAL": 3}
DEFAULT_WINDOWS = {
    "LOW": (0, 0),
    "MODERATE": (5, 15),
    "HIGH": (15, 45),
    "CRITICAL": (30, 75),
}


def _parse_dt(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _risk(value: str | None) -> str:
    raw = (value or "MODERATE").strip().upper()
    aliases = {
        "FAIBLE": "LOW",
        "LOW": "LOW",
        "MODERE": "MODERATE",
        "MODÉRÉ": "MODERATE",
        "MODERATE": "MODERATE",
        "ELEVE": "HIGH",
        "ÉLEVÉ": "HIGH",
        "HIGH": "HIGH",
        "CRITIQUE": "CRITICAL",
        "CRITICAL": "CRITICAL",
    }
    return aliases.get(raw, "MODERATE")


def _symbols(event: dict[str, Any]) -> list[str]:
    values = event.get("symbols") or event.get("instruments") or []
    if isinstance(values, str):
        values = [x.strip() for x in values.split(",")]
    return sorted({str(x).strip().upper() for x in values if str(x).strip()})


def normalize_event(event: dict[str, Any]) -> dict[str, Any]:
    at = _parse_dt(event["at"])
    risk = _risk(event.get("risk") or event.get("severity"))
    before, after = DEFAULT_WINDOWS[risk]
    before = int(event.get("off_before_min", before))
    after = int(event.get("off_after_min", after))
    return {
        "id": str(event.get("id") or f"{event.get('title', 'event')}:{at.isoformat()}"),
        "title": str(event.get("title") or "Economic event"),
        "at": at.isoformat(),
        "risk": risk,
        "currencies": sorted({str(x).strip().upper() for x in (event.get("currencies") or []) if str(x).strip()}),
        "symbols": _symbols(event),
        "off_before_min": max(0, before),
        "off_after_min": max(0, after),
        "source": str(event.get("source") or "external"),
        "meta": event.get("meta") or {},
    }


def event_window(event: dict[str, Any]) -> tuple[datetime, datetime]:
    at = _parse_dt(event["at"])
    return (
        at - timedelta(minutes=int(event.get("off_before_min", 0))),
        at + timedelta(minutes=int(event.get("off_after_min", 0))),
    )


def event_applies(event: dict[str, Any], symbol: str | None) -> bool:
    if not symbol:
        return True
    symbol_u = symbol.upper()
    symbols = event.get("symbols") or []
    if symbols:
        return symbol_u in symbols
    currencies = event.get("currencies") or []
    return any(cur in symbol_u for cur in currencies)


def evaluate(events: list[dict[str, Any]], *, symbol: str | None = None, now: datetime | None = None) -> dict[str, Any]:
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    normalized = [normalize_event(e) for e in events]
    applicable = [e for e in normalized if event_applies(e, symbol)]

    active: list[dict[str, Any]] = []
    upcoming: list[dict[str, Any]] = []
    for event in applicable:
        start, end = event_window(event)
        if start <= now <= end:
            active.append(event)
        elif now < start:
            upcoming.append(event)

    active.sort(key=lambda e: (-RISK_ORDER[e["risk"]], e["at"]))
    upcoming.sort(key=lambda e: e["at"])
    max_risk = active[0]["risk"] if active else "LOW"
    blocked = bool(active) and RISK_ORDER[max_risk] >= RISK_ORDER[_risk(settings.news_guard_block_at_risk)]

    next_event = upcoming[0] if upcoming else None
    return {
        "enabled": bool(settings.news_guard_enabled),
        "symbol": symbol.upper() if symbol else None,
        "status": "OFF" if settings.news_guard_enabled and blocked else "ON",
        "risk": max_risk,
        "active_events": active,
        "next_event": next_event,
        "checked_at": now.isoformat(),
        "fail_safe": bool(settings.news_guard_fail_safe),
    }


async def load_events(r: Any) -> list[dict[str, Any]]:
    raw = await r.get(settings.redis_news_guard_events_key())
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except Exception:
        return []
    return data if isinstance(data, list) else []


async def save_events(r: Any, events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = [normalize_event(e) for e in events]
    normalized.sort(key=lambda e: e["at"])
    await r.set(settings.redis_news_guard_events_key(), json.dumps(normalized, separators=(",", ":")))
    return normalized


async def read_state(r: Any, *, symbol: str | None = None) -> dict[str, Any]:
    if not settings.news_guard_enabled:
        return evaluate([], symbol=symbol)
    try:
        events = await load_events(r)
        state = evaluate(events, symbol=symbol)
        await r.set(settings.redis_news_guard_state_key(), json.dumps(state, separators=(",", ":")))
        return state
    except Exception:
        if settings.news_guard_fail_safe:
            return {
                "enabled": True,
                "symbol": symbol.upper() if symbol else None,
                "status": "OFF",
                "risk": "CRITICAL",
                "active_events": [],
                "next_event": None,
                "checked_at": datetime.now(timezone.utc).isoformat(),
                "fail_safe": True,
                "reason": "news_guard_unavailable",
            }
        raise
