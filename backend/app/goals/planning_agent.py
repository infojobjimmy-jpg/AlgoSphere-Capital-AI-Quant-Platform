from __future__ import annotations

import json
import logging
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.goals.store import insert_plan

logger = logging.getLogger(__name__)


def _decisions_plan_trigger(decisions: list[dict[str, Any]] | None) -> bool:
    for d in (decisions or [])[:14]:
        sev = str(d.get("severity", "")).lower()
        if sev in ("high", "critical"):
            return True
        act = str(d.get("action", "")).strip().upper()
        if act and act not in ("NO_OP", "NONE", "HOLD", "STANDBY", "NOOP"):
            return True
    return False


def _default_steps(_goal_desc: str, event_types: list[str]) -> list[dict[str, Any]]:
    _ = event_types
    return [
        {
            "id": "s1",
            "title": "Validate fused signals against goal context",
            "condition": "has_high_or_critical_event",
            "on_success": "advance",
            "fallback_action": "NOTIFY_ACTION_HOOKS",
        },
        {
            "id": "s2",
            "title": "Correlate wind–traffic if maritime or weather cues present",
            "condition": "has_correlation_or_wind_event",
            "on_success": "advance",
            "fallback_action": "REQUEST_REPLAY_WINDOW",
        },
        {
            "id": "s3",
            "title": "Stabilize: reduce operator load if noise sustained",
            "condition": "alerts_count_lte_4",
            "on_success": "complete",
            "fallback_action": "ADJUST_THRESHOLDS_SOFT",
        },
    ]


async def maybe_create_plan(
    session: AsyncSession,
    goals: list[dict[str, Any]],
    events: list[dict[str, Any]],
    decisions: list[dict[str, Any]] | None = None,
    planner_profile: str | None = None,
) -> int | None:
    if not goals:
        return None
    if len(events) < 2 and not _decisions_plan_trigger(decisions):
        return None
    top = max(goals, key=lambda g: float(g.get("priority", 0.0)))
    gid = int(top["id"])
    title = f"Auto-plan for goal {gid}: {str(top.get('description', ''))[:80]}"
    etypes = [str(e.get("event_type", "")) for e in events[:20]]
    steps = _default_steps(str(top.get("description", "")), etypes)
    d_sample = [
        f"{d.get('severity')}:{d.get('action')}"
        for d in (decisions or [])[:8]
    ]

    use_openai = bool(settings.openai_api_key) and planner_profile != "template"
    if planner_profile == "openai" and not settings.openai_api_key:
        use_openai = False
    if use_openai:
        try:
            from openai import AsyncOpenAI

            client = AsyncOpenAI(api_key=settings.openai_api_key)
            prompt = (
                "Return JSON only: {\"steps\":[{\"id\":\"s1\",\"title\":\"...\","
                "\"condition\":\"has_high_or_critical_event|has_critical_alert|always_true|alerts_count_lte_4|has_correlation_or_wind_event\","
                "\"on_success\":\"advance|complete\",\"fallback_action\":\"NOTIFY_ACTION_HOOKS|REQUEST_REPLAY_WINDOW|LOG\"}, ...]} "
                "3 to 5 steps. Conditions must be from the allowed set.\n"
                f"GOAL: {top.get('description')}\nEVENT_TYPES: {etypes}\n"
                f"STRATEGIST_DECISIONS: {d_sample}\n"
            )
            resp = await client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
            )
            txt = (resp.choices[0].message.content or "{}").strip()
            data = json.loads(txt)
            if isinstance(data.get("steps"), list) and data["steps"]:
                steps = data["steps"][:6]
        except Exception:
            logger.exception("LLM planning failed; using template plan")

    ctx = {"events_sample": etypes[:12], "goal_id": gid, "decisions_sample": d_sample}
    pid = await insert_plan(session, goal_id=gid, title=title, steps=steps, context=ctx)
    return pid
