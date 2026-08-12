from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from app.config import settings


def _targets(event: dict[str, Any] | None) -> str:
    if not event:
        return "global"
    symbols = event.get("symbols") or []
    currencies = event.get("currencies") or []
    values = symbols if symbols else currencies
    return ", ".join(str(x) for x in values) if values else "global"


def _local_time(value: str | None) -> str:
    if not value:
        return "—"
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        local = dt.astimezone(ZoneInfo(settings.news_guard_timezone))
        return local.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return value


def _risk_icon(risk: str | None) -> str:
    value = str(risk or "MODERATE").upper()
    return {
        "LOW": "🟢",
        "MODERATE": "🟡",
        "HIGH": "🟠",
        "CRITICAL": "🔴",
    }.get(value, "⚪")


def _event_line(event: dict[str, Any]) -> str:
    risk = str(event.get("risk") or "MODERATE").upper()
    title = str(event.get("title") or "Événement économique")
    meta = event.get("meta") or {}
    details = []
    if meta.get("estimate") is not None:
        details.append(f"est. {meta.get('estimate')}")
    if meta.get("previous") is not None:
        details.append(f"préc. {meta.get('previous')}")
    if meta.get("actual") is not None:
        details.append(f"réel {meta.get('actual')}")
    suffix = f" | {' | '.join(details)}" if details else ""
    return (
        f"{_risk_icon(risk)} {_local_time(event.get('at'))} Québec — {risk} — "
        f"{title} — {_targets(event)}{suffix}"
    )


def format_brief(state: dict[str, Any]) -> str:
    status = str(state.get("status") or "—")
    risk = str(state.get("risk") or "—")
    lines = [
        "### AlgoSphere News Guard",
        f"État: {status} | Risque actuel: {risk}",
    ]

    active = list(state.get("active_events") or [])
    if active:
        lines.append("**En cours / fenêtre OFF :**")
        for event in active[:4]:
            lines.append(_event_line(event))

    upcoming = list(state.get("upcoming_events") or [])
    if not upcoming and state.get("next_event"):
        upcoming = [state["next_event"]]

    if upcoming:
        lines.append("**À surveiller :**")
        for event in upcoming[:6]:
            lines.append(_event_line(event))
            if str(event.get("risk") or "").upper() in {"HIGH", "CRITICAL"}:
                lines.append(
                    f"↳ fenêtre OFF -{event.get('off_before_min', 0)} / +{event.get('off_after_min', 0)} min"
                )
    else:
        lines.append("Aucune nouvelle prochaine enregistrée.")

    if state.get("reason"):
        lines.append(f"⚠️ Fail-safe: {state.get('reason')}")

    return "\n".join(lines)[:1900]


def format_tminus(state: dict[str, Any]) -> str:
    event = state.get("next_event") or {}
    return "\n".join(
        [
            "⚠️ **AlgoSphere News Guard — T-10 min**",
            _event_line(event),
            f"Fenêtre OFF: -{event.get('off_before_min', 0)} / +{event.get('off_after_min', 0)} min",
            "EA: nouvelles entrées bloquées selon la fenêtre News Guard configurée.",
        ]
    )[:1900]


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


async def _safe_channel(name: str, sender, message: str) -> dict[str, Any]:
    try:
        return await sender(message)
    except Exception as exc:
        return {
            "configured": True,
            "sent": False,
            "error": type(exc).__name__,
            "channel": name,
        }


async def dispatch(message: str, *, sms: bool = False) -> dict[str, Any]:
    result = {"discord": await _safe_channel("discord", send_discord, message)}
    if sms:
        result["sms"] = await _safe_channel("sms", send_sms, message)
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
