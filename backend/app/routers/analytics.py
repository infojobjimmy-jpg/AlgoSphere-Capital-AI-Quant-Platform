from __future__ import annotations

import hashlib
import json

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import Depends

from app.db.session import get_session
from app.services.whop_auth import current_member

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


@router.get("/summary")
async def analytics_summary(request: Request, session: AsyncSession = Depends(get_session)) -> dict:
    member = current_member(request)
    if not member or member.get("role") != "owner":
        from fastapi import HTTPException
        raise HTTPException(status_code=403, detail="Owner access required")
    totals = (
        await session.execute(
            text(
                """
                SELECT
                  COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '7 days') AS events_7d,
                  COUNT(DISTINCT session_id) FILTER (WHERE created_at >= NOW() - INTERVAL '7 days') AS visitors_7d,
                  COUNT(*) FILTER (WHERE event_name='checkout_click' AND created_at >= NOW() - INTERVAL '7 days') AS checkout_clicks_7d,
                  COUNT(*) FILTER (WHERE event_name='member_login' AND created_at >= NOW() - INTERVAL '7 days') AS logins_7d,
                  COUNT(DISTINCT session_id) FILTER (WHERE created_at >= NOW() - INTERVAL '30 days') AS visitors_30d
                FROM analytics_events
                """
            )
        )
    ).mappings().one()
    campaigns = (
        await session.execute(
            text(
                """
                SELECT
                  COALESCE(properties->>'utm_campaign', 'direct') AS campaign,
                  COALESCE(properties->>'utm_source', 'direct') AS source,
                  COUNT(DISTINCT session_id) AS visitors,
                  COUNT(*) FILTER (WHERE event_name='checkout_click') AS checkout_clicks
                FROM analytics_events
                WHERE created_at >= NOW() - INTERVAL '30 days'
                GROUP BY 1, 2
                ORDER BY checkout_clicks DESC, visitors DESC
                LIMIT 12
                """
            )
        )
    ).mappings().all()
    result = {key: int(value or 0) for key, value in totals.items()}
    visitors = result["visitors_7d"]
    result["checkout_rate_7d"] = round(result["checkout_clicks_7d"] * 100 / visitors, 1) if visitors else 0.0
    return {"totals": result, "campaigns": [dict(row) for row in campaigns]}
