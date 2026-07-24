"""Persist one strategy-memory row per tick (Cortex, same DB session semantics as other writers)."""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.learning.execution_scores import plan_execution_score
from app.meta_learning.fingerprint import strategy_fingerprint
from app.meta_learning.guidance import compact_guidance_payload
from app.meta_learning.store import insert_strategy_episode

logger = logging.getLogger(__name__)


async def record_strategy_memory_tick(
    session: AsyncSession,
    snap: dict[str, Any],
    goals: list[dict[str, Any]],
    evolution_slice: dict[str, Any],
) -> None:
    try:
        gids = tuple(sorted(int(g["id"]) for g in goals))
    except (KeyError, TypeError, ValueError):
        gids = ()
    slim = compact_guidance_payload(evolution_slice)
    fp = strategy_fingerprint(guidance=slim, goal_ids=gids)
    strat = (snap.get("meta") or {}).get("strategy") or {}
    ex = strat.get("execution") if isinstance(strat, dict) else {}
    ex = ex if isinstance(ex, dict) else {}
    st = str(ex.get("status", ""))
    if st in ("completed", "advanced", "fallback"):
        score = plan_execution_score(ex)
    else:
        score = None
    ap = strat.get("active_plan") if isinstance(strat, dict) else None
    pid = None
    if isinstance(ap, dict) and ap.get("plan_id") is not None:
        try:
            pid = int(ap["plan_id"])
        except (TypeError, ValueError):
            pid = None
    metrics = {
        "n_events": len(snap.get("events") or []),
        "n_decisions": len(snap.get("decisions") or []),
        "n_alerts": len(snap.get("alerts") or []),
        "exec_status": st,
    }
    await insert_strategy_episode(
        session,
        fingerprint=fp,
        deployment="live",
        metrics=metrics,
        outcome_score=score,
        plan_id=pid,
        guidance_applied=slim,
    )
    logger.debug("meta-learning: recorded strategy episode fp=%s score=%s", fp, score)
