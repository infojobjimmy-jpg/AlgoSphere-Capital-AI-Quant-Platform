from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


async def list_active_goals(session: AsyncSession) -> list[dict[str, Any]]:
    res = await session.execute(
        text(
            """
            SELECT id, description, priority, status, created_at, metadata
            FROM goals
            WHERE status = 'active'
            ORDER BY priority DESC, id ASC
            """
        )
    )
    rows = res.mappings().all()
    return [
        {
            "id": int(r["id"]),
            "description": r["description"],
            "priority": float(r["priority"]),
            "status": r["status"],
            "created_at": r["created_at"].isoformat(),
            "metadata": r["metadata"] or {},
        }
        for r in rows
    ]


async def insert_plan(
    session: AsyncSession,
    *,
    goal_id: int,
    title: str,
    steps: list[dict[str, Any]],
    context: dict[str, Any],
) -> int:
    res = await session.execute(
        text(
            """
            INSERT INTO plans (goal_id, title, status, steps, context, updated_at)
            VALUES (:gid, :title, 'active', CAST(:steps AS jsonb), CAST(:ctx AS jsonb), NOW())
            RETURNING id
            """
        ),
        {
            "gid": goal_id,
            "title": title[:500],
            "steps": json.dumps(steps),
            "ctx": json.dumps(context),
        },
    )
    pid = int(res.scalar_one())
    await session.execute(
        text("UPDATE plans SET status = 'superseded', updated_at = NOW() WHERE goal_id = :gid AND id <> :pid AND status = 'active'"),
        {"gid": goal_id, "pid": pid},
    )
    await session.execute(
        text(
            """
            INSERT INTO plan_executions (plan_id, step_index, state, last_tick_at)
            VALUES (:pid, 0, CAST(:st AS jsonb), NOW())
            """
        ),
        {"pid": pid, "st": json.dumps({"completed": [], "outcomes": []})},
    )
    await session.commit()
    return pid


async def get_active_plan_bundle(session: AsyncSession) -> dict[str, Any] | None:
    res = await session.execute(
        text(
            """
            SELECT p.id AS plan_id, p.goal_id, p.title, p.steps, p.status AS plan_status,
                   e.id AS exec_id, e.step_index, e.state
            FROM plans p
            JOIN plan_executions e ON e.plan_id = p.id
            WHERE p.status = 'active'
            ORDER BY p.id DESC, e.id DESC
            LIMIT 1
            """
        )
    )
    row = res.mappings().first()
    if not row:
        return None
    return {
        "plan_id": int(row["plan_id"]),
        "goal_id": int(row["goal_id"]),
        "title": row["title"],
        "steps": row["steps"] if isinstance(row["steps"], list) else [],
        "plan_status": row["plan_status"],
        "exec_id": int(row["exec_id"]),
        "step_index": int(row["step_index"]),
        "state": row["state"] if isinstance(row["state"], dict) else {},
    }


async def update_execution(
    session: AsyncSession,
    exec_id: int,
    *,
    step_index: int,
    state: dict[str, Any],
) -> None:
    await session.execute(
        text(
            """
            UPDATE plan_executions
            SET step_index = :si, state = CAST(:st AS jsonb), last_tick_at = NOW()
            WHERE id = :eid
            """
        ),
        {"si": step_index, "st": json.dumps(state), "eid": exec_id},
    )
    await session.commit()


async def complete_plan(session: AsyncSession, plan_id: int) -> None:
    await session.execute(
        text("UPDATE plans SET status = 'completed', updated_at = NOW() WHERE id = :pid"),
        {"pid": plan_id},
    )
    await session.commit()


async def adjust_goal_priority(session: AsyncSession, goal_id: int, factor: float) -> None:
    await session.execute(
        text(
            """
            UPDATE goals
            SET priority = GREATEST(0.5, LEAST(100.0, priority * :fac)), updated_at = NOW()
            WHERE id = :gid
            """
        ),
        {"fac": float(factor), "gid": goal_id},
    )
    await session.commit()


async def insert_feedback(
    session: AsyncSession,
    *,
    subject_type: str,
    subject_id: str,
    score: float,
    notes: dict[str, Any],
) -> None:
    await session.execute(
        text(
            """
            INSERT INTO feedback_records (subject_type, subject_id, score, notes)
            VALUES (:st, :sid, :sc, CAST(:n AS jsonb))
            """
        ),
        {
            "st": subject_type[:64],
            "sid": subject_id[:256],
            "sc": float(score),
            "n": json.dumps(notes),
        },
    )
    await session.commit()
