from __future__ import annotations

import hashlib
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_session
from app.services.whop_auth import current_member

router = APIRouter()
AVATARS = {"🧭", "🌎", "🚀", "🛰️", "✈️", "🚢", "🌊", "🦊", "🐺", "🦅", "🐋", "🤠"}
INTENTS = {"family", "friends", "community", "dating"}


def _identity(request: Request) -> str:
    member = current_member(request)
    if not member:
        raise HTTPException(status_code=401, detail="Active subscription required")
    identity = str(member.get("user_id") or member.get("membership_id") or "")
    if not identity:
        raise HTTPException(status_code=401, detail="Member identity unavailable")
    return identity


def _invite_code(identity: str) -> str:
    return hashlib.sha256(f"algosphere:{identity}".encode()).hexdigest()[:10].upper()


async def _ensure_profile(session: AsyncSession, identity: str, username: str = "") -> None:
    await session.execute(
        text(
            """
            INSERT INTO social_profiles (member_id, invite_code, display_name)
            VALUES (:member_id, :invite_code, :display_name)
            ON CONFLICT (member_id) DO NOTHING
            """
        ),
        {"member_id": identity, "invite_code": _invite_code(identity), "display_name": username or "Explorer"},
    )
    await session.commit()


class ProfileUpdate(BaseModel):
    display_name: str = Field(min_length=2, max_length=40)
    avatar: str = Field(default="🧭", max_length=8)
    bio: str = Field(default="", max_length=240)
    city: str = Field(default="", max_length=80)
    country: str = Field(default="", max_length=80)
    intent: str = Field(default="community", max_length=20)
    discoverable: bool = False


class PresenceUpdate(BaseModel):
    enabled: bool
    lat: float | None = Field(default=None, ge=-90, le=90)
    lon: float | None = Field(default=None, ge=-180, le=180)


class MessageCreate(BaseModel):
    room: str = Field(default="global", pattern=r"^[a-z0-9_-]{2,32}$")
    content: str = Field(min_length=1, max_length=500)


class ConnectRequest(BaseModel):
    invite_code: str = Field(min_length=6, max_length=20)
    label: str = Field(default="friends", max_length=20)


@router.get("/overview")
async def overview(request: Request, session: AsyncSession = Depends(get_session)) -> dict:
    identity = _identity(request)
    member = current_member(request) or {}
    await _ensure_profile(session, identity, str(member.get("username") or ""))
    profile = (
        await session.execute(text("SELECT * FROM social_profiles WHERE member_id=:id"), {"id": identity})
    ).mappings().one()
    connections = (
        await session.execute(
            text(
                """
                SELECT p.member_id, p.display_name, p.avatar, p.bio, p.city, p.country,
                       p.points, c.label, pr.lat, pr.lon, pr.updated_at AS presence_updated_at
                FROM social_connections c
                JOIN social_profiles p ON p.member_id=c.contact_id
                LEFT JOIN social_presence pr ON pr.member_id=p.member_id AND pr.enabled=TRUE
                WHERE c.owner_id=:id AND c.status='accepted'
                ORDER BY p.display_name
                """
            ),
            {"id": identity},
        )
    ).mappings().all()
    discovery = (
        await session.execute(
            text(
                """
                SELECT member_id, display_name, avatar, bio, city, country, intent, points
                FROM social_profiles
                WHERE discoverable=TRUE AND member_id<>:id
                ORDER BY updated_at DESC LIMIT 40
                """
            ),
            {"id": identity},
        )
    ).mappings().all()
    messages = (
        await session.execute(
            text(
                """
                SELECT m.id, m.room, m.content, m.created_at, p.display_name, p.avatar, p.points
                FROM social_messages m JOIN social_profiles p ON p.member_id=m.member_id
                WHERE m.room='global' ORDER BY m.id DESC LIMIT 60
                """
            )
        )
    ).mappings().all()
    return {
        "profile": dict(profile),
        "connections": [dict(row) for row in connections],
        "discovery": [dict(row) for row in discovery],
        "messages": [dict(row) for row in reversed(messages)],
        "avatars": sorted(AVATARS),
    }


@router.post("/profile")
async def update_profile(body: ProfileUpdate, request: Request, session: AsyncSession = Depends(get_session)) -> dict:
    identity = _identity(request)
    if body.avatar not in AVATARS or body.intent not in INTENTS:
        raise HTTPException(status_code=400, detail="Invalid avatar or social goal")
    await _ensure_profile(session, identity)
    await session.execute(
        text(
            """
            UPDATE social_profiles SET display_name=:display_name, avatar=:avatar, bio=:bio,
            city=:city, country=:country, intent=:intent, discoverable=:discoverable, updated_at=NOW()
            WHERE member_id=:member_id
            """
        ),
        {**body.model_dump(), "member_id": identity},
    )
    await session.commit()
    return {"ok": True}


@router.post("/presence")
async def update_presence(body: PresenceUpdate, request: Request, session: AsyncSession = Depends(get_session)) -> dict:
    identity = _identity(request)
    if body.enabled and (body.lat is None or body.lon is None):
        raise HTTPException(status_code=400, detail="Coordinates required")
    await _ensure_profile(session, identity)
    await session.execute(
        text(
            """
            INSERT INTO social_presence (member_id, lat, lon, enabled, updated_at)
            VALUES (:member_id, :lat, :lon, :enabled, NOW())
            ON CONFLICT (member_id) DO UPDATE SET lat=EXCLUDED.lat, lon=EXCLUDED.lon,
            enabled=EXCLUDED.enabled, updated_at=NOW()
            """
        ),
        {"member_id": identity, **body.model_dump()},
    )
    await session.commit()
    return {"ok": True, "enabled": body.enabled}


@router.post("/connect")
async def connect(body: ConnectRequest, request: Request, session: AsyncSession = Depends(get_session)) -> dict:
    identity = _identity(request)
    code = body.invite_code.strip().upper()
    target = (
        await session.execute(text("SELECT member_id FROM social_profiles WHERE invite_code=:code"), {"code": code})
    ).scalar_one_or_none()
    if not target or target == identity:
        raise HTTPException(status_code=404, detail="Invite code not found")
    label = body.label if body.label in {"family", "friends", "partner"} else "friends"
    for owner, contact in ((identity, target), (target, identity)):
        await session.execute(
            text(
                """
                INSERT INTO social_connections (owner_id, contact_id, label, status)
                VALUES (:owner, :contact, :label, 'accepted')
                ON CONFLICT (owner_id, contact_id) DO UPDATE SET label=EXCLUDED.label, status='accepted'
                """
            ),
            {"owner": owner, "contact": contact, "label": label},
        )
    await session.execute(text("UPDATE social_profiles SET points=points+10 WHERE member_id IN (:a,:b)"), {"a": identity, "b": target})
    await session.commit()
    return {"ok": True}


@router.post("/messages")
async def post_message(body: MessageCreate, request: Request, session: AsyncSession = Depends(get_session)) -> dict:
    identity = _identity(request)
    await _ensure_profile(session, identity)
    recent = (
        await session.execute(
            text("SELECT COUNT(*) FROM social_messages WHERE member_id=:id AND created_at>NOW()-INTERVAL '2 seconds'"),
            {"id": identity},
        )
    ).scalar_one()
    if recent:
        raise HTTPException(status_code=429, detail="Please wait before posting again")
    clean = " ".join(body.content.strip().split())
    await session.execute(
        text("INSERT INTO social_messages (member_id, room, content) VALUES (:id,:room,:content)"),
        {"id": identity, "room": body.room, "content": clean},
    )
    await session.execute(text("UPDATE social_profiles SET points=points+1, updated_at=NOW() WHERE member_id=:id"), {"id": identity})
    await session.commit()
    return {"ok": True}
