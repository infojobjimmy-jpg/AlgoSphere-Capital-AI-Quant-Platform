from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from typing import Any

import httpx
import redis.asyncio as aioredis
from fastapi import Depends, HTTPException, Request

from app.config import settings

ACTIVE_STATUSES = {"active", "trialing", "canceling"}
_REVOKED_SUFFIX = ":members:revoked"


def configured() -> bool:
    return bool(settings.whop_api_key and settings.auth_session_secret)


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode().rstrip("=")


def _unb64(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def create_session(payload: dict[str, Any], hours: int | None = None) -> str:
    if not settings.auth_session_secret:
        raise HTTPException(status_code=503, detail="Subscription validation is not configured")
    body = dict(payload)
    session_hours = hours if hours is not None else int(settings.auth_session_hours)
    body["exp"] = int(time.time()) + session_hours * 3600
    body.setdefault("validated_at", int(time.time()))
    encoded = _b64(json.dumps(body, separators=(",", ":"), sort_keys=True).encode())
    sig = _b64(hmac.new(settings.auth_session_secret.encode(), encoded.encode(), hashlib.sha256).digest())
    return f"{encoded}.{sig}"


def read_session(token: str | None) -> dict[str, Any] | None:
    if not token or not settings.auth_session_secret:
        return None
    try:
        encoded, sig = token.split(".", 1)
        expected = _b64(hmac.new(settings.auth_session_secret.encode(), encoded.encode(), hashlib.sha256).digest())
        if not hmac.compare_digest(sig, expected):
            return None
        payload = json.loads(_unb64(encoded))
        if int(payload.get("exp", 0)) <= int(time.time()):
            return None
        return payload
    except (ValueError, TypeError, json.JSONDecodeError):
        return None


def current_member(request: Request) -> dict[str, Any] | None:
    return read_session(request.cookies.get("algosphere_session"))


async def is_membership_revoked(client: aioredis.Redis, membership_id: str) -> bool:
    return bool(await client.sismember(f"{settings.app_slug}{_REVOKED_SUFFIX}", membership_id))


async def revoke_membership(client: aioredis.Redis, membership_id: str) -> None:
    await client.sadd(f"{settings.app_slug}{_REVOKED_SUFFIX}", membership_id)


async def restore_membership(client: aioredis.Redis, membership_id: str) -> None:
    await client.srem(f"{settings.app_slug}{_REVOKED_SUFFIX}", membership_id)


async def require_active_member(request: Request) -> dict[str, Any]:
    member = current_member(request)
    if not member:
        raise HTTPException(status_code=401, detail="Subscription required")
    if member.get("role") == "owner":
        return member
    membership_id = member.get("membership_id", "")
    client = aioredis.from_url(settings.redis_url, decode_responses=True)
    try:
        if membership_id and await is_membership_revoked(client, membership_id):
            raise HTTPException(status_code=403, detail="Your subscription has been canceled or revoked")
        validated_at = int(member.get("validated_at", 0))
        license_key = member.get("_lk", "")
        if license_key and configured() and (time.time() - validated_at > 6 * 3600):
            try:
                await validate_license(license_key)
            except HTTPException as exc:
                if exc.status_code in {401, 403}:
                    raise HTTPException(
                        status_code=403,
                        detail="Your subscription is no longer active. Please sign in again.",
                    ) from exc
    finally:
        await client.aclose()
    return member


async def validate_license(license_key: str) -> dict[str, Any]:
    if not configured():
        raise HTTPException(status_code=503, detail="Whop validation is not configured")
    key = license_key.strip()
    if not key or len(key) > 200:
        raise HTTPException(status_code=400, detail="Invalid license key")
    async with httpx.AsyncClient(timeout=15.0) as client:
        response = await client.get(
            f"https://api.whop.com/api/v1/memberships/{key}",
            headers={"Authorization": f"Bearer {settings.whop_api_key}"},
        )
    if response.status_code in {401, 403}:
        raise HTTPException(status_code=503, detail="Whop validation credentials were rejected")
    if response.status_code == 404:
        raise HTTPException(status_code=401, detail="Membership not found")
    if response.status_code >= 400:
        raise HTTPException(status_code=502, detail="Whop validation is temporarily unavailable")
    membership = response.json()
    status = str(membership.get("status") or "").lower()
    product = membership.get("product") or {}
    product_id = str(product.get("id") or membership.get("product_id") or "")
    allowed = {item.strip() for item in settings.whop_allowed_product_ids.split(",") if item.strip()}
    if status not in ACTIVE_STATUSES or (allowed and product_id not in allowed):
        raise HTTPException(status_code=403, detail="An active AlgoSphere subscription is required")
    user = membership.get("user") or {}
    plan = settings.plan_for_product_id(product_id)
    return {
        "membership_id": str(membership.get("id") or key),
        "product_id": product_id,
        "plan": plan,
        "status": status,
        "user_id": str(user.get("id") or ""),
        "username": str(user.get("username") or ""),
        "email": str(user.get("email") or ""),
        "expires_at": membership.get("expires_at") or membership.get("renewal_period_end"),
        "manage_url": membership.get("manage_url"),
        "product_name": str(product.get("title") or product.get("name") or product_id),
    }
