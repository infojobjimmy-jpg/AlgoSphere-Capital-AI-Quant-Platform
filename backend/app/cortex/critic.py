"""Critic: evaluates plan outcomes and writes feedback (single path into goals.feedback)."""

from __future__ import annotations

from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.goals.feedback import record_plan_feedback


async def evaluate_plan_outcome(
    session: AsyncSession,
    *,
    goal_id: int,
    plan_id: int,
    exec_view: dict[str, Any],
    snap: dict[str, Any],
) -> None:
    await record_plan_feedback(
        session,
        goal_id=goal_id,
        plan_id=plan_id,
        exec_view=exec_view,
        snap=snap,
    )
