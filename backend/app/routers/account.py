from __future__ import annotations

import json

import redis.asyncio as redis
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from app.config import settings
from app.services.whop_auth import current_member

router = APIRouter()


class Preferences(BaseModel):
    favorites: list[dict] = Field(default_factory=list, max_length=100)
    alerts: list[dict] = Field(default_factory=list, max_length=50)


def _member(request: Request) -> dict:
    member = current_member(request)
    if not member:
        raise HTTPException(status_code=401, detail="Active subscription required")
    return member


def _key(member: dict) -> str:
    identity = member.get("user_id") or member.get("membership_id")
    return f"{settings.app_slug}:member:{identity}:preferences"


@router.get("/preferences")
async def get_preferences(request: Request) -> dict:
    member = _member(request)
    client = redis.from_url(settings.redis_url, decode_responses=True)
    try:
        raw = await client.get(_key(member))
        return json.loads(raw) if raw else {"favorites": [], "alerts": []}
    finally:
        await client.aclose()


@router.post("/preferences")
async def save_preferences(body: Preferences, request: Request) -> dict:
    member = _member(request)
    payload = body.model_dump()
    client = redis.from_url(settings.redis_url, decode_responses=True)
    try:
        await client.set(_key(member), json.dumps(payload, separators=(",", ":")))
        return payload
    finally:
        await client.aclose()
