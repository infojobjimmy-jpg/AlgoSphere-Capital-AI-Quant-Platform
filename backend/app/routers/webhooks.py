from __future__ import annotations

import base64
import logging

import redis.asyncio as aioredis
from fastapi import APIRouter, HTTPException, Request

from app.config import settings
from app.services.whop_auth import restore_membership, revoke_membership
from app.services.whop_fulfillment import fulfill_membership

logger = logging.getLogger("acap.webhooks")
router = APIRouter()

_HANDLED_EVENTS = {
    "membership.activated",
    "membership.deactivated",
    "membership.cancel_at_period_end_changed",
    "refund.created",
    "payment.succeeded",
}


@router.post("/whop")
async def whop_webhook(request: Request) -> dict:
    if not settings.whop_webhook_secret:
        raise HTTPException(status_code=503, detail="Webhooks are not configured")

    raw_body = await request.body()
    payload_str = raw_body.decode("utf-8", errors="replace")

    from whop_sdk import AsyncWhop

    raw_webhook_secret = settings.whop_webhook_secret.strip()
    encoded_webhook_secret = base64.b64encode(
        raw_webhook_secret.encode("utf-8")
    ).decode("ascii")

    whop = AsyncWhop(
        api_key=settings.whop_api_key or "",
        webhook_key=encoded_webhook_secret,
    )
    try:
        event = whop.webhooks.unwrap(payload_str, headers=dict(request.headers))
    except Exception as exc:
        logger.warning("webhook sig verification failed: %s", type(exc).__name__)
        raise HTTPException(status_code=401, detail="Invalid webhook signature")
    finally:
        await whop.close()

    # Idempotence check applies to ALL events before type filtering so that
    # re-delivered messages always return "duplicate" regardless of event type.
    webhook_id = request.headers.get("webhook-id", "")
    client = aioredis.from_url(settings.redis_url, decode_responses=True)
    try:
        if webhook_id:
            idem_key = f"{settings.app_slug}:webhook:seen:{webhook_id}"
            if await client.exists(idem_key):
                return {"accepted": True, "action": "duplicate"}
            await client.set(idem_key, "1", ex=86400 * 7)

        event_type = str(getattr(event, "type", "") or "")
        if event_type not in _HANDLED_EVENTS:
            return {"accepted": True, "action": "ignored"}

        action = "noop"

        if event_type in {
            "membership.activated",
            "membership.deactivated",
            "membership.cancel_at_period_end_changed",
        }:
            data = getattr(event, "data", None)
            membership_id = str(getattr(data, "id", "") or "")
            product = getattr(data, "product", None)
            product_id = str(getattr(product, "id", "") or "")
            product_title = str(getattr(product, "title", "") or settings.product_name)
            user = getattr(data, "user", None)
            user_email = str(getattr(user, "email", "") or "")
            license_key = str(getattr(data, "license_key", "") or "")
            manage_url = str(getattr(data, "manage_url", "") or "https://whop.com/hub")
            cancel_at_period_end = getattr(data, "cancel_at_period_end", None)

            if event_type == "membership.activated":
                allowed = {x.strip() for x in settings.whop_allowed_product_ids.split(",") if x.strip()}
                if allowed and product_id and product_id not in allowed:
                    logger.warning("webhook: unrecognized product_id — ignoring activation")
                    return {"accepted": True, "action": "ignored_product"}
                if membership_id:
                    await restore_membership(client, membership_id)
                action = "activated"
                m = {
                    "id": membership_id,
                    "product": {"id": product_id, "title": product_title},
                    "user": {"email": user_email},
                    "license_key": license_key,
                    "manage_url": manage_url,
                    "status": "active",
                }
                await fulfill_membership(
                    membership_id, m, settings.whop_api_key or "", client, "webhook"
                )

            elif event_type == "membership.deactivated":
                if membership_id:
                    await revoke_membership(client, membership_id)
                action = "revoked"

            elif event_type == "membership.cancel_at_period_end_changed":
                logger.info(
                    "webhook: cancel_at_period_end=%s for membership %s",
                    cancel_at_period_end,
                    membership_id[:8] if membership_id else "?",
                )
                action = "cancel_flag_updated"

        elif event_type == "refund.created":
            logger.info("webhook: refund.created received")
            action = "refund_logged"

        elif event_type == "payment.succeeded":
            logger.info("webhook: payment.succeeded received")
            action = "payment_logged"

    finally:
        await client.aclose()

    return {"accepted": True, "action": action}
