from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import httpx

from app.config import settings


def _targets(event: dict[str, Any] | None) -> str:
    if not event:
        return "global"
    symbols = event.get("symbols") or []
    currencies = event.get("currencies") or []
    values = symbols if symbols else currencies
    return ", ".join(str(x) for x in values) if values else "global"


def format_brief(state: dict[str, Any]) -> str:
    lines = [
        "### AlgoSphere News Guard",
        f"État: {state.get('status', '—')} | Risque: {state.get('risk', '—')}",
    ]
    event = state.get("next_event")
    if event:
        lines.extend(
            [
                f"Prochaine nouvelle: {event.get('title', 'Événement économique')}",
                f"Heure: {event.get('at', '—')}",
                f"Touchés: {_targets(event)}",
                f"Fenêtre OFF: -{event.get('off_before_min', 0)} / +{event.get('off_after_min', 0)} min",
            ]
        )
    else:
        lines.append("Aucune nouvelle prochaine enregistrée.")
    return "\n".join(lines)


def format_tminus(state: dict[str, Any]) -> str:
    event = state.get("next_event") or {}
    return "\n".join(
        [
            "⚠️ AlgoSphere News Guard — T-10 min",
            str(event.get("title") or "Événement économique"),
            f"Risque: {event.get('risk', state.get('risk', '—'))}",
            f"Touchés: {_targets(event)}",
            "EA: nouvelles entrées bloquées selon la fenêtre News Guard configurée.",
        ]
    )


async def send_discord(message: str) -> dict[str, Any]:
    url = (settings.news_guard_discord_webhook_url or "").strip()
    if not url:
        return {"configured": False, "sent": False}
    async with httpx.AsyncClient(timeout=settings.news_guard_http_timeout_sec) as client:
        response = await client.post(url, json={"content": message})
        response.raise_for_status()
    return {"configured": True, "sent": True}


async def send_sms(message: str) -> dict[str, Any]:
    sid = (settings.twilio_account_sid or "").strip()
    token = (settings.twilio_auth_token or "").strip()
    from_number = (settings.twilio_from_number or "").strip()
    to_number = (settings.news_guard_sms_to or "").strip()
    if not all((sid, token, from_number, to_number)):
        return {"configured": False, "sent": False}

    url = f"https://api.twilio.com/2010-04-01/Accounts/{sid}/Messages.json"
    data = {"From": from_number, "To": to_number, "Body": message[:1500]}
    async with httpx.AsyncClient(timeout=settings.news_guard_http_timeout_sec) as client:
        response = await client.post(url, data=data, auth=(sid, token))
        response.raise_for_status()
        payload = response.json()
    return {"configured": True, "sent": True, "sid": payload.get("sid")}


async def dispatch(message: str, *, sms: bool = False) -> dict[str, Any]:
    result = {"discord": await send_discord(message)}
    if sms:
        result["sms"] = await send_sms(message)
    return result


def minutes_until(value: str | None) -> float | None:
    if not value:
        return None
    try:
        event_time = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if event_time.tzinfo is None:
        event_time = event_time.replace(tzinfo=timezone.utc)
    return (event_time.astimezone(timezone.utc) - datetime.now(timezone.utc)).total_seconds() / 60.0
