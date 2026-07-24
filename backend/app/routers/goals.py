from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_session
from app.goals.store import get_active_plan_bundle, list_active_goals

router = APIRouter()


class GoalCreate(BaseModel):
    description: str = Field(..., min_length=1, max_length=4000)
    priority: float = Field(5.0, ge=0.5, le=100.0)
    status: str = Field("active", pattern=r"^(active|paused|done)$")


class GoalPatch(BaseModel):
    description: str | None = Field(None, min_length=1, max_length=4000)
    priority: float | None = Field(None, ge=0.5, le=100.0)
    status: str | None = Field(None, pattern=r"^(active|paused|done|superseded)$")


@router.get("")
async def list_goals(status: str | None = None, session: AsyncSession = Depends(get_session)):
    if status in (None, "", "active"):
        goals = await list_active_goals(session)
        return {"goals": goals}
    if status == "all":
        res = await session.execute(
            text(
                """
                SELECT id, description, priority, status, created_at, metadata
                FROM goals
                ORDER BY priority DESC, id DESC
                LIMIT 500
                """
            )
        )
    else:
        st = status[:32]
        res = await session.execute(
            text(
                """
                SELECT id, description, priority, status, created_at, metadata
                FROM goals
                WHERE status = :st
                ORDER BY priority DESC, id ASC
                LIMIT 200
                """
            ),
            {"st": st},
        )
    rows = res.mappings().all()
    return {
        "goals": [
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
    }


@router.post("")
async def create_goal(body: GoalCreate, session: AsyncSession = Depends(get_session)):
    res = await session.execute(
        text(
            """
            INSERT INTO goals (description, priority, status, updated_at)
            VALUES (:d, :p, :st, NOW())
            RETURNING id, description, priority, status, created_at, metadata
            """
        ),
        {"d": body.description, "p": float(body.priority), "st": body.status},
    )
    row = res.mappings().one()
    await session.commit()
    return {
        "goal": {
            "id": int(row["id"]),
            "description": row["description"],
            "priority": float(row["priority"]),
            "status": row["status"],
            "created_at": row["created_at"].isoformat(),
            "metadata": row["metadata"] or {},
        }
    }


@router.patch("/{goal_id}")
async def patch_goal(goal_id: int, body: GoalPatch, session: AsyncSession = Depends(get_session)):
    fields: list[str] = []
    params: dict[str, Any] = {"gid": goal_id}
    if body.description is not None:
        fields.append("description = :desc")
        params["desc"] = body.description
    if body.priority is not None:
        fields.append("priority = :pri")
        params["pri"] = float(body.priority)
    if body.status is not None:
        fields.append("status = :st")
        params["st"] = body.status
    if not fields:
        raise HTTPException(status_code=400, detail="no fields to update")
    fields.append("updated_at = NOW()")
    q = f"UPDATE goals SET {', '.join(fields)} WHERE id = :gid RETURNING id, description, priority, status, created_at, metadata"
    res = await session.execute(text(q), params)
    row = res.mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="goal not found")
    await session.commit()
    return {
        "goal": {
            "id": int(row["id"]),
            "description": row["description"],
            "priority": float(row["priority"]),
            "status": row["status"],
            "created_at": row["created_at"].isoformat(),
            "metadata": row["metadata"] or {},
        }
    }


@router.get("/plans/summary")
async def plans_summary(session: AsyncSession = Depends(get_session)):
    bundle = await get_active_plan_bundle(session)
    res = await session.execute(
        text(
            """
            SELECT p.id, p.goal_id, p.title, p.status, p.steps, p.created_at, p.updated_at,
                   e.id AS exec_id, e.step_index, e.state, e.last_tick_at
            FROM plans p
            LEFT JOIN LATERAL (
                SELECT id, step_index, state, last_tick_at
                FROM plan_executions
                WHERE plan_id = p.id
                ORDER BY id DESC
                LIMIT 1
            ) e ON true
            ORDER BY p.id DESC
            LIMIT 24
            """
        )
    )
    recent: list[dict[str, Any]] = []
    for r in res.mappings().all():
        recent.append(
            {
                "id": int(r["id"]),
                "goal_id": int(r["goal_id"]) if r["goal_id"] is not None else None,
                "title": r["title"],
                "status": r["status"],
                "steps": r["steps"] if isinstance(r["steps"], list) else [],
                "created_at": r["created_at"].isoformat(),
                "updated_at": r["updated_at"].isoformat(),
                "execution": (
                    {
                        "id": int(r["exec_id"]),
                        "step_index": int(r["step_index"] or 0),
                        "state": r["state"] if isinstance(r["state"], dict) else {},
                        "last_tick_at": r["last_tick_at"].isoformat() if r["last_tick_at"] else None,
                    }
                    if r["exec_id"] is not None
                    else None
                ),
            }
        )
    return {"active_bundle": bundle, "recent_plans": recent}
