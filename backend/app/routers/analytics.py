from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import time
from typing import Any

import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db.session import get_session
from app.services.whop_auth import current_member

router = APIRouter()
logger = logging.getLogger("acap.analytics")

ALLOWED_EVENTS = {"page_view", "app_open", "checkout_click", "member_login", "gps_open", "community_open"}
_PRESENCE_IDX = lambda: f"{settings.app_slug}:presence:index"
_PRESENCE_KEY = lambda vsid: f"{settings.app_slug}:presence:{vsid}"
_PRESENCE_TTL = 90


# ---------------------------------------------------------------------------
# Table bootstrap (called once at startup from main.py)
# IMPORTANT: this DDL must stay in sync with infra/sql/005_visitor_sessions.sql.
# When adding columns: update both files + add an ALTER TABLE guard below.
# ---------------------------------------------------------------------------

_VISITOR_SESSIONS_DDL = """
CREATE TABLE IF NOT EXISTS visitor_sessions (
    id                  BIGSERIAL PRIMARY KEY,
    visitor_session_id  TEXT        NOT NULL,
    visitor_id          TEXT,
    member_id           TEXT,
    username            TEXT,
    product_id          TEXT,
    plan                TEXT,
    authenticated       BOOLEAN     NOT NULL DEFAULT FALSE,
    first_seen          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    duration_seconds    INTEGER     NOT NULL DEFAULT 0,
    page_views          INTEGER     NOT NULL DEFAULT 1,
    heartbeat_count     INTEGER     NOT NULL DEFAULT 1,
    landing_page        TEXT,
    last_page           TEXT,
    referrer_domain     TEXT,
    utm_source          TEXT,
    utm_campaign        TEXT,
    device_type         TEXT,
    consent             TEXT        NOT NULL DEFAULT 'null',
    checkout_clicked    BOOLEAN     NOT NULL DEFAULT FALSE,
    login_completed     BOOLEAN     NOT NULL DEFAULT FALSE,
    converted_to_member BOOLEAN     NOT NULL DEFAULT FALSE
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_vs_vsid       ON visitor_sessions (visitor_session_id);
CREATE INDEX        IF NOT EXISTS idx_vs_first_seen ON visitor_sessions (first_seen DESC);
CREATE INDEX        IF NOT EXISTS idx_vs_last_seen  ON visitor_sessions (last_seen  DESC);
CREATE INDEX        IF NOT EXISTS idx_vs_member_id  ON visitor_sessions (member_id);
CREATE INDEX        IF NOT EXISTS idx_vs_utm_src    ON visitor_sessions (utm_source);
ALTER TABLE visitor_sessions ADD COLUMN IF NOT EXISTS heartbeat_count INTEGER NOT NULL DEFAULT 1;
ALTER TABLE visitor_sessions ADD COLUMN IF NOT EXISTS consent         TEXT    NOT NULL DEFAULT 'null';
"""


async def ensure_visitor_sessions_table() -> None:
    from app.db.session import SessionLocal, ensure_database
    await ensure_database()
    # asyncpg rejects multi-statement strings in a single execute() call.
    # Split on ";" and execute each non-empty statement individually.
    statements = [s.strip() for s in _VISITOR_SESSIONS_DDL.split(";") if s.strip()]
    async with SessionLocal() as session:
        for stmt in statements:
            await session.execute(text(stmt))
        await session.commit()


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class AnalyticsEvent(BaseModel):
    event: str = Field(min_length=3, max_length=40)
    path: str = Field(default="/", max_length=200)
    plan: str | None = Field(default=None, max_length=40)
    session_id: str = Field(min_length=8, max_length=80)
    properties: dict[str, str | int | float | bool | None] = Field(default_factory=dict)


class HeartbeatBody(BaseModel):
    visitor_session_id: str = Field(min_length=8, max_length=80)
    visitor_id: str = Field(default="", max_length=80)
    page: str = Field(default="/", max_length=200)
    device_type: str = Field(default="unknown", max_length=20)
    referrer_domain: str = Field(default="", max_length=120)
    utm_source: str = Field(default="", max_length=120)
    utm_campaign: str = Field(default="", max_length=120)
    # "accepted" | "declined" | "null" — frontend sends analyticsConsent() value
    consent: str = Field(default="null", max_length=10)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _require_owner(request: Request) -> dict[str, Any]:
    member = current_member(request)
    if not member or member.get("role") != "owner":
        raise HTTPException(status_code=403, detail="Owner access required")
    return member


def _duration_label(seconds: int) -> str:
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m"
    return f"{seconds // 3600}h {(seconds % 3600) // 60}m"


# ---------------------------------------------------------------------------
# Existing event endpoint
# ---------------------------------------------------------------------------

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
    if body.event == "checkout_click":
        await session.execute(
            text("""
                UPDATE visitor_sessions
                SET checkout_clicked = TRUE
                WHERE visitor_session_id = :sid
            """),
            {"sid": body.session_id},
        )
    await session.commit()
    return {"accepted": True}


# ---------------------------------------------------------------------------
# Heartbeat — presence tracking
#
# Consent rules:
#   declined + anonymous  → Redis minimal (no UTM/device/referrer), NO DB record.
#   declined + member     → Redis minimal, DB operational only (no UTM/device/referrer).
#   accepted              → Redis full, DB full.
#   null (unknown)        → treated as declined (conservative default).
# ---------------------------------------------------------------------------

@router.post("/heartbeat")
async def heartbeat(
    body: HeartbeatBody,
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> dict:
    member = current_member(request)
    is_auth = member is not None and member.get("role") not in {"owner"}
    is_owner = member is not None and member.get("role") == "owner"
    consent_accepted = body.consent == "accepted"

    membership_id = str(member.get("membership_id", "") if member else "")
    username = str(member.get("username", "") if member else "")
    product_id = str(member.get("product_id", "") if member else "")
    plan = str(member.get("plan", "") if member else "")

    now_ts = int(time.time())

    # ── Redis presence ───────────────────────────────────────────────────────
    # Always written (needed for live-visitors count).
    # Marketing fields stripped when consent not accepted.
    presence_data = json.dumps({
        "visitor_session_id": body.visitor_session_id,
        "type": "owner" if is_owner else ("member" if is_auth else "anonymous"),
        "member_id": membership_id if (is_auth or is_owner) else "",
        "username": username if (is_auth or is_owner) else "",
        "plan": plan if is_auth else "",
        "page": body.page,
        "last_seen": now_ts,
        "device_type": body.device_type if consent_accepted else "unknown",
        "utm_source": body.utm_source if consent_accepted else "",
        "utm_campaign": body.utm_campaign if consent_accepted else "",
    })

    redis_ok = False
    redis_client = aioredis.from_url(settings.redis_url, decode_responses=True)
    try:
        pipe = redis_client.pipeline()
        pipe.set(_PRESENCE_KEY(body.visitor_session_id), presence_data, ex=_PRESENCE_TTL)
        pipe.zadd(_PRESENCE_IDX(), {body.visitor_session_id: now_ts})
        pipe.zremrangebyscore(_PRESENCE_IDX(), 0, now_ts - _PRESENCE_TTL)
        await pipe.execute()
        redis_ok = True
    except Exception as exc:
        logger.error("heartbeat redis write failed: %s", type(exc).__name__)
    finally:
        await redis_client.aclose()

    # ── PostgreSQL persistence ───────────────────────────────────────────────
    # Anonymous visitors with no consent → skip entirely (no persistent record).
    if not consent_accepted and not is_auth and not is_owner:
        return {"ok": True, "redis": redis_ok, "db": None}

    # Marketing fields stored only when consent is accepted.
    store_marketing = consent_accepted

    db_ok = False
    try:
        await session.execute(
            text("""
                INSERT INTO visitor_sessions (
                    visitor_session_id, visitor_id, member_id, username,
                    product_id, plan, authenticated, landing_page, last_page,
                    referrer_domain, utm_source, utm_campaign, device_type,
                    login_completed, converted_to_member, consent
                ) VALUES (
                    :vsid, :vid, :member_id, :username,
                    :product_id, :plan, :auth, :page, :page,
                    :referrer, :utm_src, :utm_camp, :device,
                    :login, :converted, :consent
                )
                ON CONFLICT (visitor_session_id) DO UPDATE SET
                    last_seen        = NOW(),
                    duration_seconds = GREATEST(0,
                        EXTRACT(EPOCH FROM (NOW() - visitor_sessions.first_seen))::INTEGER
                    ),
                    heartbeat_count  = visitor_sessions.heartbeat_count + 1,
                    page_views       = CASE
                                         WHEN EXCLUDED.last_page IS DISTINCT FROM visitor_sessions.last_page
                                         THEN visitor_sessions.page_views + 1
                                         ELSE visitor_sessions.page_views
                                       END,
                    last_page        = EXCLUDED.last_page,
                    consent          = EXCLUDED.consent,
                    member_id        = COALESCE(EXCLUDED.member_id,        visitor_sessions.member_id),
                    username         = COALESCE(NULLIF(EXCLUDED.username, ''),   visitor_sessions.username),
                    product_id       = COALESCE(NULLIF(EXCLUDED.product_id, ''), visitor_sessions.product_id),
                    plan             = COALESCE(NULLIF(EXCLUDED.plan, ''),       visitor_sessions.plan),
                    authenticated    = visitor_sessions.authenticated OR EXCLUDED.authenticated,
                    login_completed  = visitor_sessions.login_completed  OR EXCLUDED.login_completed,
                    converted_to_member = visitor_sessions.converted_to_member OR EXCLUDED.converted_to_member
            """),
            {
                "vsid": body.visitor_session_id,
                "vid": (body.visitor_id or None) if consent_accepted else None,
                "member_id": membership_id or None,
                "username": username or None,
                "product_id": product_id or None,
                "plan": plan or None,
                "auth": is_auth,
                "page": body.page,
                "referrer": (body.referrer_domain or None) if store_marketing else None,
                "utm_src": (body.utm_source or None) if store_marketing else None,
                "utm_camp": (body.utm_campaign or None) if store_marketing else None,
                "device": (body.device_type or None) if store_marketing else None,
                "login": is_auth,
                "converted": is_auth and bool(product_id),
                "consent": body.consent,
            },
        )
        await session.commit()
        db_ok = True
    except Exception as exc:
        logger.error("visitor_sessions upsert failed: %s", type(exc).__name__)

    return {"ok": True, "redis": redis_ok, "db": db_ok}


# ---------------------------------------------------------------------------
# Live visitors — owner only
# ---------------------------------------------------------------------------

@router.get("/live-visitors")
async def live_visitors(request: Request) -> dict:
    _require_owner(request)
    now_ts = int(time.time())
    cutoff = now_ts - _PRESENCE_TTL

    redis_client = aioredis.from_url(settings.redis_url, decode_responses=True)
    try:
        await redis_client.zremrangebyscore(_PRESENCE_IDX(), 0, cutoff)
        vsids = await redis_client.zrangebyscore(_PRESENCE_IDX(), cutoff, "+inf")
        if not vsids:
            return {"total_online": 0, "members_online": 0, "anonymous_online": 0, "visitors": []}
        keys = [_PRESENCE_KEY(vsid) for vsid in vsids]
        raw_values = await redis_client.mget(*keys)
    finally:
        await redis_client.aclose()

    visitors = []
    members_online = 0
    for raw in raw_values:
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        visitor_type = data.get("type", "anonymous")
        is_member = visitor_type in {"member", "owner"}
        if is_member:
            members_online += 1
        last_seen_ts = int(data.get("last_seen", now_ts))
        visitors.append({
            "visitor_session_id": data.get("visitor_session_id", "")[:16] + "…",
            "type": "Membre identifié" if is_member else "Visiteur anonyme",
            "username": data.get("username") or None,
            "plan": data.get("plan") or None,
            "page": data.get("page", "/"),
            "last_seen_ago_s": now_ts - last_seen_ts,
            "device_type": data.get("device_type", "unknown"),
            "utm_source": data.get("utm_source") or None,
            "utm_campaign": data.get("utm_campaign") or None,
        })

    visitors.sort(key=lambda v: v["last_seen_ago_s"])
    return {
        "total_online": len(visitors),
        "members_online": members_online,
        "anonymous_online": len(visitors) - members_online,
        "visitors": visitors,
    }


# ---------------------------------------------------------------------------
# Visitor history — owner only
# ---------------------------------------------------------------------------

@router.get("/visitor-history")
async def visitor_history(
    request: Request,
    session: AsyncSession = Depends(get_session),
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=20, ge=1, le=100),
    search: str = Query(default="", max_length=120),
    date_from: str = Query(default="", max_length=20),
    date_to: str = Query(default="", max_length=20),
    authenticated: str = Query(default=""),
    plan: str = Query(default="", max_length=40),
    utm_source: str = Query(default="", max_length=120),
    converted: str = Query(default=""),
    sort: str = Query(default="last_seen", pattern="^(last_seen|duration_seconds|first_seen|page_views)$"),
    order: str = Query(default="desc", pattern="^(asc|desc)$"),
    format: str = Query(default="json", pattern="^(json|csv)$"),
) -> Any:
    _require_owner(request)

    conditions = ["1=1"]
    params: dict[str, Any] = {}

    if search:
        conditions.append("(username ILIKE :search OR visitor_session_id ILIKE :search)")
        params["search"] = f"%{search}%"
    if date_from:
        conditions.append("first_seen >= :date_from")
        params["date_from"] = date_from
    if date_to:
        conditions.append("first_seen <= :date_to")
        params["date_to"] = date_to
    if authenticated in {"true", "1"}:
        conditions.append("authenticated = TRUE")
    elif authenticated in {"false", "0"}:
        conditions.append("authenticated = FALSE")
    if plan:
        conditions.append("plan = :plan")
        params["plan"] = plan
    if utm_source:
        conditions.append("utm_source = :utm_source")
        params["utm_source"] = utm_source
    if converted in {"true", "1"}:
        conditions.append("converted_to_member = TRUE")
    elif converted in {"false", "0"}:
        conditions.append("converted_to_member = FALSE")

    where = " AND ".join(conditions)
    sort_col = sort if sort in {"last_seen", "duration_seconds", "first_seen", "page_views"} else "last_seen"
    sort_dir = "DESC" if order == "desc" else "ASC"

    count_row = (await session.execute(
        text(f"SELECT COUNT(*) FROM visitor_sessions WHERE {where}"), params
    )).scalar()
    total = int(count_row or 0)

    offset = (page - 1) * per_page
    rows = (await session.execute(
        text(f"""
            SELECT id, visitor_session_id, member_id, username, product_id, plan,
                   authenticated, first_seen, last_seen, duration_seconds, page_views,
                   heartbeat_count, landing_page, last_page, referrer_domain,
                   utm_source, utm_campaign, device_type, consent,
                   checkout_clicked, login_completed, converted_to_member
            FROM visitor_sessions
            WHERE {where}
            ORDER BY {sort_col} {sort_dir}
            LIMIT :limit OFFSET :offset
        """),
        {**params, "limit": per_page, "offset": offset},
    )).mappings().all()

    today_stats = (await session.execute(text("""
        SELECT
          COUNT(*) FILTER (WHERE first_seen >= NOW() - INTERVAL '24 hours') AS new_today,
          COUNT(*) FILTER (WHERE first_seen < NOW() - INTERVAL '24 hours'
                           AND last_seen >= NOW() - INTERVAL '24 hours') AS returning_today,
          COUNT(*) FILTER (WHERE authenticated = TRUE
                           AND last_seen >= NOW() - INTERVAL '24 hours') AS auth_today,
          COUNT(*) FILTER (WHERE checkout_clicked = TRUE
                           AND last_seen >= NOW() - INTERVAL '24 hours') AS checkout_today,
          COUNT(*) FILTER (WHERE converted_to_member = TRUE
                           AND last_seen >= NOW() - INTERVAL '24 hours') AS converted_today,
          COUNT(DISTINCT CASE WHEN last_seen >= NOW() - INTERVAL '7 days' THEN id END) AS total_7d,
          COUNT(DISTINCT CASE WHEN last_seen >= NOW() - INTERVAL '30 days' THEN id END) AS total_30d,
          ROUND(AVG(duration_seconds) FILTER (WHERE last_seen >= NOW() - INTERVAL '7 days'))::INTEGER
              AS avg_duration_7d
        FROM visitor_sessions
    """))).mappings().one()

    sessions = [
        {
            "id": row["id"],
            "visitor_session_id": str(row["visitor_session_id"])[:16] + "…",
            "type": "Membre identifié" if row["authenticated"] else "Visiteur anonyme",
            "username": row["username"],
            "plan": row["plan"],
            "first_seen": row["first_seen"].isoformat() if row["first_seen"] else None,
            "last_seen": row["last_seen"].isoformat() if row["last_seen"] else None,
            "duration_s": row["duration_seconds"],
            "duration_label": _duration_label(int(row["duration_seconds"] or 0)),
            "page_views": row["page_views"],
            "heartbeat_count": row["heartbeat_count"],
            "landing_page": row["landing_page"],
            "last_page": row["last_page"],
            "utm_source": row["utm_source"],
            "utm_campaign": row["utm_campaign"],
            "device_type": row["device_type"],
            "checkout_clicked": row["checkout_clicked"],
            "login_completed": row["login_completed"],
            "converted_to_member": row["converted_to_member"],
        }
        for row in rows
    ]

    if format == "csv":
        def _generate():
            buf = io.StringIO()
            writer = csv.DictWriter(buf, fieldnames=list(sessions[0].keys()) if sessions else [])
            writer.writeheader()
            for s in sessions:
                writer.writerow(s)
            yield buf.getvalue()
        return StreamingResponse(
            _generate(),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=visitor-history.csv"},
        )

    return {
        "total": total,
        "page": page,
        "per_page": per_page,
        "pages": max(1, (total + per_page - 1) // per_page),
        "today": {k: int(v or 0) for k, v in today_stats.items()},
        "sessions": sessions,
    }


# ---------------------------------------------------------------------------
# Existing summary endpoint
# ---------------------------------------------------------------------------

@router.get("/summary")
async def analytics_summary(request: Request, session: AsyncSession = Depends(get_session)) -> dict:
    member = current_member(request)
    if not member or member.get("role") != "owner":
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
