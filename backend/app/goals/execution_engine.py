from __future__ import annotations

import logging
import re
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.goals.store import complete_plan, update_execution

logger = logging.getLogger(__name__)


def evaluate_condition(condition: str, snap: dict[str, Any]) -> bool:
    c = (condition or "always_true").strip()
    if c == "always_true":
        return True
    events = snap.get("events") or []
    alerts = snap.get("alerts") or []
    corr = snap.get("correlations") or []
    if c == "has_high_or_critical_event":
        return any(str(e.get("severity")).lower() in ("high", "critical") for e in events)
    if c == "has_critical_alert":
        return any(str(a.get("severity")).lower() == "critical" for a in alerts)
    if c == "has_correlation_or_wind_event":
        if corr:
            return True
        for e in events:
            pl = e.get("payload")
            if isinstance(pl, dict) and float(pl.get("wind_mps") or 0) >= 18:
                return True
        return False
    m = re.match(r"alerts_count_lte_(\d+)$", c)
    if m:
        n = int(m.group(1))
        return len(alerts) <= n
    m2 = re.match(r"events_count_gt:(\d+)$", c)
    if m2:
        return len(events) > int(m2.group(1))
    return False


async def tick_execution(
    session: AsyncSession,
    bundle: dict[str, Any],
    snap: dict[str, Any],
) -> dict[str, Any]:
    steps = bundle.get("steps") or []
    if not steps:
        return {"status": "idle", "detail": "no_steps"}

    idx = int(bundle.get("step_index", 0))
    state = dict(bundle.get("state") or {})
    completed = list(state.get("completed") or [])
    outcomes = list(state.get("outcomes") or [])

    if idx >= len(steps):
        await complete_plan(session, int(bundle["plan_id"]))
        return {"status": "completed", "plan_id": bundle["plan_id"], "step_index": idx}

    step = steps[idx]
    cond = str(step.get("condition", "always_true"))
    ok = evaluate_condition(cond, snap)
    step_id = str(step.get("id", str(idx)))

    if ok:
        completed.append(step_id)
        outcomes.append({"step": step_id, "result": "success", "condition": cond})
        on_success = str(step.get("on_success", "advance"))
        if on_success == "complete":
            idx = len(steps)
        else:
            idx += 1
        await update_execution(
            session, int(bundle["exec_id"]), step_index=idx, state={"completed": completed, "outcomes": outcomes}
        )
        if idx >= len(steps):
            await complete_plan(session, int(bundle["plan_id"]))
        return {
            "status": "completed" if idx >= len(steps) else "advanced",
            "plan_id": bundle["plan_id"],
            "step_index": idx,
            "last_step": step_id,
            "completed": completed,
        }

    fb = str(step.get("fallback_action", "LOG"))
    outcomes.append({"step": step_id, "result": "fallback", "action": fb})
    state = {"completed": completed, "outcomes": outcomes}
    await update_execution(session, int(bundle["exec_id"]), step_index=idx, state=state)
    return {
        "status": "fallback",
        "plan_id": bundle["plan_id"],
        "step_index": idx,
        "fallback": fb,
        "completed": completed,
    }
