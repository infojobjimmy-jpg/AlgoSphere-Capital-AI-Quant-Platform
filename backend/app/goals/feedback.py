from __future__ import annotations

import logging
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.goals.store import adjust_goal_priority, insert_feedback
from app.learning.execution_scores import plan_execution_score

logger = logging.getLogger(__name__)


async def record_plan_feedback(
    session: AsyncSession,
    *,
    goal_id: int,
    plan_id: int,
    exec_view: dict[str, Any],
    snap: dict[str, Any],
) -> None:
    score = plan_execution_score(exec_view)

    await insert_feedback(
        session,
        subject_type="plan",
        subject_id=str(plan_id),
        score=score,
        notes={"execution": exec_view, "events": len(snap.get("events") or [])},
    )
    fac = 1.0 + 0.04 * (score - 0.5)
    await adjust_goal_priority(session, goal_id, fac)
