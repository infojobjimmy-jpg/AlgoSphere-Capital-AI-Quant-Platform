from __future__ import annotations

import json
import logging

import httpx
import redis.asyncio as redis
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from app.config import settings
from app.services.license_email import send_license_email
from app.services.whop_auth import ACTIVE_STATUSES, current_member
from app.services.whop_fulfillment import is_fulfilled, record_fulfillment

logger = logging.getLogger("acap.account")

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


class ResendWelcomeBody(BaseModel):
    membership_id: str
    force: bool = False


@router.post("/resend-welcome")
async def resend_welcome(body: ResendWelcomeBody, request: Request) -> dict:
    member = current_member(request)
    if not member or member.get("role") != "owner":
        raise HTTPException(status_code=403, detail="Owner access required")

    membership_id = body.membership_id.strip()
    if not membership_id or len(membership_id) > 100:
        raise HTTPException(status_code=400, detail="Invalid membership_id")

    masked = membership_id[:8] + "***"

    if not settings.whop_api_key:
        raise HTTPException(status_code=503, detail="Whop integration not configured")

    # Fetch membership from Whop
    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.get(
            f"https://api.whop.com/api/v2/memberships/{membership_id}",
            headers={"Authorization": f"Bearer {settings.whop_api_key}"},
        )

    if resp.status_code == 404:
        raise HTTPException(status_code=404, detail="Membership not found")
    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail="Whop API unavailable")

    m = resp.json()
    product_id = str(m.get("product") or m.get("access_pass") or "")
    status = str(m.get("status") or "").lower()
    user_email = str(m.get("email") or "")
    license_key = str(m.get("license_key") or "")
    manage_url = str(m.get("manage_url") or "https://whop.com/@me/settings/orders/")

    allowed = {x.strip() for x in settings.whop_allowed_product_ids.split(",") if x.strip()}
    if allowed and product_id not in allowed:
        raise HTTPException(status_code=403, detail="Product not authorized")

    if status not in ACTIVE_STATUSES:
        raise HTTPException(status_code=403, detail=f"Membership status '{status}' is not active")

    if not user_email:
        raise HTTPException(status_code=422, detail="No email address on this membership")

    if not license_key:
        raise HTTPException(status_code=422, detail="No license key on this membership")

    # Idempotence guard
    already_sent = await is_fulfilled(membership_id)
    if already_sent and not body.force:
        return {"membership_id": masked, "action": "already_sent", "license_returned": False}

    # Fetch product title
    async with httpx.AsyncClient(timeout=10.0) as client:
        pr = await client.get(
            f"https://api.whop.com/api/v1/products/{product_id}",
            headers={"Authorization": f"Bearer {settings.whop_api_key}"},
        )
    product_title = pr.json().get("title") or settings.product_name if pr.status_code == 200 else settings.product_name

    source = "owner_resend_force" if (body.force and already_sent) else "owner_resend"
    if body.force and already_sent:
        logger.warning("account: owner forced resend for %s", masked)

    await send_license_email(user_email, product_title, license_key, manage_url)
    await record_fulfillment(membership_id, product_id, status, source, sent=True)
    logger.info("account: resend-welcome sent for %s by owner (force=%s)", masked, body.force)

    return {"membership_id": masked, "action": "sent", "license_returned": False}


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
