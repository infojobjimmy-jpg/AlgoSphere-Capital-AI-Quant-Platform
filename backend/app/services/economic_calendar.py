from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any

import httpx

from app.config import settings

COUNTRY_CURRENCY = {
    "US": "USD",
    "CA": "CAD",
    "GB": "GBP",
    "EU": "EUR",
    "DE": "EUR",
    "FR": "EUR",
    "IT": "EUR",
    "ES": "EUR",
    "JP": "JPY",
    "AU": "AUD",
    "NZ": "NZD",
    "CH": "CHF",
}

IMPACT_TO_RISK = {
    "low": "LOW",
    "medium": "MODERATE",
    "moderate": "MODERATE",
    "high": "HIGH",
    "critical": "CRITICAL",
}

CRITICAL_KEYWORDS = (
    "cpi",
    "consumer price",
    "nonfarm",
    "non-farm",
    "nfp",
    "fomc",
    "interest rate",
    "fed rate",
    "federal funds",
    "core pce",
    "pce price",
    "gdp",
)


def _risk_for(event_name: str, impact: str | None) -> str:
    name = event_name.lower()
    if any(keyword in name for keyword in CRITICAL_KEYWORDS):
        return "CRITICAL"
    return IMPACT_TO_RISK.get((impact or "").strip().lower(), "MODERATE")


def _parse_finnhub_time(value: str) -> str:
    raw = value.strip()
    # Finnhub documents economic release timestamps as "YYYY-MM-DD HH:MM:SS".
    dt = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    return dt.isoformat()


def normalize_finnhub_event(row: dict[str, Any]) -> dict[str, Any]:
    country = str(row.get("country") or "").upper()
    currency = COUNTRY_CURRENCY.get(country)
    title = str(row.get("event") or "Economic event")
    risk = _risk_for(title, str(row.get("impact") or ""))
    currencies = [currency] if currency else []
    return {
        "id": f"finnhub:{country}:{row.get('time')}:{title}",
        "title": title,
        "at": _parse_finnhub_time(str(row["time"])),
        "risk": risk,
        "currencies": currencies,
        "symbols": [],
        "source": "finnhub:economic-calendar",
        "meta": {
            "country": country,
            "impact": row.get("impact"),
            "actual": row.get("actual"),
            "estimate": row.get("estimate"),
            "previous": row.get("prev"),
            "unit": row.get("unit"),
        },
    }


async def fetch_finnhub_calendar(
    *,
    from_date: date | None = None,
    to_date: date | None = None,
) -> list[dict[str, Any]]:
    if not settings.finnhub_api_key:
        raise RuntimeError("FINNHUB_API_KEY is not configured")

    start = from_date or datetime.now(timezone.utc).date()
    end = to_date or (start + timedelta(days=max(1, settings.news_guard_calendar_days)))
    params = {
        "from": start.isoformat(),
        "to": end.isoformat(),
        "token": settings.finnhub_api_key,
    }
    async with httpx.AsyncClient(timeout=settings.news_guard_http_timeout_sec) as client:
        response = await client.get("https://finnhub.io/api/v1/calendar/economic", params=params)
        response.raise_for_status()
        payload = response.json()

    rows = payload.get("economicCalendar", []) if isinstance(payload, dict) else []
    events: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict) or not row.get("time"):
            continue
        try:
            events.append(normalize_finnhub_event(row))
        except (ValueError, TypeError):
            continue
    events.sort(key=lambda event: event["at"])
    return events


async def fetch_calendar() -> list[dict[str, Any]]:
    provider = settings.news_guard_calendar_provider.strip().lower()
    if provider == "finnhub":
        return await fetch_finnhub_calendar()
    raise RuntimeError(f"Unsupported NEWS_GUARD_CALENDAR_PROVIDER={provider}")
