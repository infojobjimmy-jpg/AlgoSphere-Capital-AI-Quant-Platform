from __future__ import annotations

import logging
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.cortex.critic import evaluate_plan_outcome
from app.goals.execution_engine import tick_execution
from app.goals.planning_agent import maybe_create_plan
from app.goals.store import get_active_plan_bundle
from app.meta_learning.guidance import planner_profile_choice

logger = logging.getLogger(__name__)


async def run_goals_post_cycle(
    session: AsyncSession,
    snap: dict[str, Any],
    goals: list[dict[str, Any]],
    *,
    evolution_guidance: dict[str, Any] | None = None,
) -> None:
    """
    Planning + execution + feedback tied to the latest fused snapshot.
    Mutates snap['meta']['strategy'] with live execution view.
    """
    meta = snap.setdefault("meta", {})
    strat = meta.setdefault("strategy", {})
    events = snap.get("events") or []

    bundle = await get_active_plan_bundle(session)
    prof = planner_profile_choice(evolution_guidance)
    if prof is None and settings.openai_api_key:
        prof = "openai"
    elif prof is None:
        prof = "template"
    strat["planner_profile"] = prof

    if bundle is None and goals:
        try:
            pid = await maybe_create_plan(
                session,
                goals,
                events,
                snap.get("decisions"),
                planner_profile=prof,
            )
            if pid:
                strat["plan_created"] = pid
                bundle = await get_active_plan_bundle(session)
        except Exception:
            logger.exception("plan creation failed")

    if not bundle:
        strat["execution"] = {"status": "idle", "detail": "no_active_plan"}
        return

    try:
        exec_view = await tick_execution(session, bundle, snap)
        strat["execution"] = exec_view
        strat["active_plan"] = {
            "plan_id": bundle["plan_id"],
            "goal_id": bundle["goal_id"],
            "title": bundle["title"],
            "steps": bundle.get("steps") or [],
            "step_index": exec_view.get("step_index", bundle.get("step_index", 0)),
        }
        if str(exec_view.get("status")) in ("completed", "fallback"):
            await evaluate_plan_outcome(
                session,
                goal_id=int(bundle["goal_id"]),
                plan_id=int(bundle["plan_id"]),
                exec_view=exec_view,
                snap=snap,
            )
    except Exception:
        logger.exception("goal execution/feedback failed")
        strat["execution"] = {"status": "error"}
