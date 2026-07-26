from __future__ import annotations

import hashlib
import hmac
import json
import logging
import time

import redis.asyncio as aioredis
from fastapi import APIRouter, HTTPException, Request

from app.config import settings
from app.services.license_email import send_license_email
from app.services.whop_auth import restore_membership, revoke_membership

logger = logging.getLogger("acap.webhooks")
router = APIRouter()

_INVALID_STATUSES = {"canceled", "cancelled", "expired", "unresolved", "drafted", "past_due"}
_ACTIVE_STATUSES = {"active", "trialing", "canceling"}
_HANDLED_EVENTS = {
    "membership.activated",
    "membership.went_valid",
    "membership.updated",
    "membership.deactivated",
    "membership.went_invalid",
    "membership.canceled",
    "membership.cancelled",
}


def _verify_signature(raw_body: bytes, header: str | None) -> bool:
    if not settings.whop_webhook_secret or not header:
        return False
    parts: dict[str, str] = {}
    for part in header.split(","):
        if "=" in part:
            k, v = part.split("=", 1)
            parts[k.strip()] = v.strip()
    timestamp = parts.get("t", "")
    v1_sig = parts.get("v1", "")
    if not timestamp or not v1_sig:
        return False
    try:
        if abs(time.time() - float(timestamp)) > 300:
            return False
    except ValueError:
        return False
    signed = f"{timestamp}.{raw_body.decode('utf-8', errors='replace')}".encode()
    expected = hmac.new(settings.whop_webhook_secret.encode(), signed, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, v1_sig)


@router.post("/whop")
async def whop_webhook(request: Request) -> dict:
    if not settings.whop_webhook_secret:
        raise HTTPException(status_code=503, detail="Webhooks are not configured")

    raw_body = await request.body()
    if not _verify_signature(raw_body, request.headers.get("Whop-Signature")):
        raise HTTPException(status_code=401, detail="Invalid webhook signature")

    try:
        event = json.loads(raw_body)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="Invalid JSON") from exc

    event_id = str(event.get("event_id") or event.get("id") or "")
    event_type = str(event.get("event") or event.get("event_type") or event.get("type") or "")
    data = event.get("data") or {}

    if event_type not in _HANDLED_EVENTS:
        return {"accepted": True, "action": "ignored"}

    client = aioredis.from_url(settings.redis_url, decode_responses=True)
    try:
        if event_id:
            idem_key = f"{settings.app_slug}:webhook:seen:{event_id}"
            if await client.exists(idem_key):
                return {"accepted": True, "action": "duplicate"}
            await client.set(idem_key, "1", ex=86400 * 7)

        membership_id = str(data.get("id") or "")
        status = str(data.get("status") or "").lower()
        product = data.get("product") or {}
        product_id = str(product.get("id") or data.get("product_id") or "")
        user = data.get("user") or {}
        user_email = str(user.get("email") or "")
        allowed = {x.strip() for x in settings.whop_allowed_product_ids.split(",") if x.strip()}
        action = "noop"

        if event_type in {"membership.activated", "membership.went_valid"}:
            if allowed and product_id and product_id not in allowed:
                logger.warning("webhook: unrecognized product_id received — ignoring activation")
                return {"accepted": True, "action": "ignored_product"}
            if membership_id:
                await restore_membership(client, membership_id)
            action = "activated"
            license_key = str(data.get("license_key") or "")
            if user_email and license_key:
                product_name = str(product.get("title") or product.get("name") or settings.product_name)
                manage_url = str(data.get("manage_url") or "https://whop.com/hub")
                try:
                    await send_license_email(user_email, product_name, license_key, manage_url)
                except Exception:
                    logger.exception("webhook: failed to send welcome email (details not logged)")
            elif not license_key:
                logger.error("webhook: membership activated but license_key absent — welcome email not sent")

        elif event_type in {"membership.deactivated", "membership.went_invalid",
                            "membership.canceled", "membership.cancelled"}:
            if membership_id:
                await revoke_membership(client, membership_id)
            action = "revoked"

        elif event_type == "membership.updated":
            if status in _INVALID_STATUSES:
                if membership_id:
                    await revoke_membership(client, membership_id)
                action = "revoked"
            elif status in _ACTIVE_STATUSES:
                if membership_id:
                    await restore_membership(client, membership_id)
                action = "restored"

    finally:
        await client.aclose()

    return {"accepted": True, "action": action}
