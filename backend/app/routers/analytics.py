from __future__ import annotations

import hashlib
import json

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import Depends

from app.db.session import get_session

router = APIRouter()
ALLOWED_EVENTS = {"page_view", "app_open", "checkout_click", "member_login", "gps_open", "community_open"}


class AnalyticsEvent(BaseModel):
    event: str = Field(min_length=3, max_length=40)
    path: str = Field(default="/", max_length=200)
    plan: str | None = Field(default=None, max_length=40)
    session_id: str = Field(min_length=8, max_length=80)
    properties: dict[str, str | int | float | bool | None] = Field(default_factory=dict)


@router.post("/event")
async def record_event(body: AnalyticsEvent, request: Request, session: AsyncSession = Depends(get_session)) -> dict:
    if body.event not in ALLOWED_EVENTS:
        return {"accepted": False}
    forwarded = request.headers.get("x-forwarded-for", "")
    host = forwarded.split(",", 1)[0].strip() or (request.client.host if request.client else "unknown")
    daily_visitor = hashlib.sha256(f"{host}:{request.headers.get('user-agent', '')}".encode()).hexdigest()[:24]
    safe_properties = dict(list(body.properties.items())[:12])
    await session.execute(
        text(
            """
            INSERT INTO analytics_events (event_name, path, plan, session_id, visitor_hash, properties)
            VALUES (:event, :path, :plan, :session_id, :visitor_hash, CAST(:properties AS JSONB))
            """
        ),
        {
            "event": body.event,
            "path": body.path,
            "plan": body.plan,
            "session_id": body.session_id,
            "visitor_hash": daily_visitor,
            "properties": json.dumps(safe_properties, separators=(",", ":")),
        },
    )
    await session.commit()
    return {"accepted": True}
