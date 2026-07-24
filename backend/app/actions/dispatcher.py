from __future__ import annotations

import json
import logging
from typing import Any

import httpx

from app.config import settings

logger = logging.getLogger(__name__)


def _hooks() -> dict[str, list[dict[str, Any]]]:
    raw = (settings.action_hooks_json or "").strip()
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
        return {str(k).lower(): list(v) for k, v in obj.items() if isinstance(v, list)}
    except Exception:
        logger.exception("invalid ACTION_HOOKS_JSON")
        return {}


async def dispatch_for_decisions(snap: dict[str, Any]) -> None:
    hooks = _hooks()
    if not hooks:
        return
    decisions = snap.get("decisions") or []
    payload = {
        "ts": (snap.get("meta") or {}).get("updated_at"),
        "decisions": decisions[:80],
        "events": (snap.get("events") or [])[:40],
    }
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    async with httpx.AsyncClient(timeout=15.0) as client:
        for d in decisions:
            sev = str(d.get("severity", "low")).lower()
            for hook in hooks.get(sev, []) + hooks.get("all", []):
                htype = str(hook.get("type", "")).lower()
                url = str(hook.get("url", "")).strip()
                if htype == "webhook" and url.startswith("http"):
                    try:
                        r = await client.post(url, content=body, headers={"Content-Type": "application/json"})
                        r.raise_for_status()
                    except Exception:
                        logger.exception("webhook dispatch failed sev=%s url=%s", sev, url[:80])
                elif htype == "api" and url.startswith("http"):
                    try:
                        r = await client.post(url, json=payload)
                        r.raise_for_status()
                    except Exception:
                        logger.exception("api trigger failed sev=%s url=%s", sev, url[:80])
                elif htype == "email":
                    logger.warning("email hook configured but SMTP not wired; drop to logs only: %s", hook)


async def dispatch_for_snap(snap: dict[str, Any]) -> None:
    await dispatch_for_decisions(snap)
