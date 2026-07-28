from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any

import httpx
import redis.asyncio as aioredis
from sqlalchemy import text

from app.config import settings
from app.db.session import SessionLocal, ensure_database
from app.services.license_email import send_license_email
from app.services.whop_auth import ACTIVE_STATUSES

logger = logging.getLogger("acap.fulfillment")

_FULFILLMENT_DDL = """
CREATE TABLE IF NOT EXISTS whop_fulfillment (
    membership_id TEXT PRIMARY KEY,
    product_id TEXT NOT NULL,
    status TEXT NOT NULL,
    welcome_email_sent_at TIMESTAMPTZ,
    fulfillment_source TEXT,
    last_checked_at TIMESTAMPTZ DEFAULT NOW(),
    last_error TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_whop_fulfillment_product ON whop_fulfillment(product_id);
CREATE INDEX IF NOT EXISTS idx_whop_fulfillment_sent ON whop_fulfillment(welcome_email_sent_at) WHERE welcome_email_sent_at IS NOT NULL
"""


async def ensure_whop_fulfillment_table() -> None:
    await ensure_database()
    async with SessionLocal() as session:
        for stmt in _FULFILLMENT_DDL.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                await session.execute(text(stmt))
        await session.commit()
    logger.info("fulfillment: whop_fulfillment table ready")


async def is_fulfilled(membership_id: str) -> bool:
    async with SessionLocal() as session:
        row = (await session.execute(
            text("SELECT welcome_email_sent_at FROM whop_fulfillment WHERE membership_id = :mid"),
            {"mid": membership_id},
        )).first()
        return bool(row and row[0] is not None)


async def record_fulfillment(
    membership_id: str,
    product_id: str,
    status: str,
    source: str,
    *,
    sent: bool = False,
    error: str | None = None,
) -> None:
    sent_at = datetime.now(timezone.utc) if sent else None
    async with SessionLocal() as session:
        await session.execute(text("""
            INSERT INTO whop_fulfillment
                (membership_id, product_id, status, welcome_email_sent_at,
                 fulfillment_source, last_checked_at, last_error, updated_at)
            VALUES
                (:mid, :pid, :status, :sent_at, :source, NOW(), :error, NOW())
            ON CONFLICT (membership_id) DO UPDATE SET
                status               = EXCLUDED.status,
                welcome_email_sent_at = COALESCE(
                    whop_fulfillment.welcome_email_sent_at,
                    EXCLUDED.welcome_email_sent_at
                ),
                fulfillment_source   = COALESCE(
                    whop_fulfillment.fulfillment_source,
                    EXCLUDED.fulfillment_source
                ),
                last_checked_at      = NOW(),
                last_error           = EXCLUDED.last_error,
                updated_at           = NOW()
        """), {
            "mid": membership_id,
            "pid": product_id,
            "status": status,
            "sent_at": sent_at,
            "source": source,
            "error": error,
        })
        await session.commit()


async def seed_initial_fulfillments() -> None:
    """Insert pre-fulfilled membership IDs from WHOP_FULFILLMENT_SEEDED_IDS config.

    Called at startup so the reconciliation poller never re-sends welcome emails
    for memberships that were handled manually before the poller was deployed.
    """
    seeded = [x.strip() for x in settings.whop_fulfillment_seeded_ids.split(",") if x.strip()]
    if not seeded:
        return
    async with SessionLocal() as session:
        for mid in seeded:
            await session.execute(text("""
                INSERT INTO whop_fulfillment
                    (membership_id, product_id, status, welcome_email_sent_at, fulfillment_source)
                VALUES (:mid, 'seeded', 'unknown', NOW(), 'seed')
                ON CONFLICT (membership_id) DO NOTHING
            """), {"mid": mid})
        await session.commit()
    for mid in seeded:
        logger.info("fulfillment: seeded %s as pre-fulfilled", mid[:8] + "***")


# ---------------------------------------------------------------------------
# Reconciliation poller
# ---------------------------------------------------------------------------

_PRODUCT_TITLE_CACHE: dict[str, str] = {}


async def _get_product_title(api_key: str, product_id: str) -> str:
    if product_id in _PRODUCT_TITLE_CACHE:
        return _PRODUCT_TITLE_CACHE[product_id]
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                f"https://api.whop.com/api/v1/products/{product_id}",
                headers={"Authorization": f"Bearer {api_key}"},
            )
            if resp.status_code == 200:
                title = resp.json().get("title") or settings.product_name
                _PRODUCT_TITLE_CACHE[product_id] = title
                return title
    except Exception:
        pass
    return settings.product_name


async def _fetch_recent_memberships(api_key: str, after_ts: int) -> list[dict[str, Any]]:
    allowed = {x.strip() for x in settings.whop_allowed_product_ids.split(",") if x.strip()}
    results: list[dict[str, Any]] = []
    cursor: str | None = None

    async with httpx.AsyncClient(timeout=20.0) as client:
        while True:
            params: dict[str, str] = {"limit": "100"}
            if cursor:
                params["after"] = cursor

            resp = await client.get(
                "https://api.whop.com/api/v2/memberships",
                params=params,
                headers={"Authorization": f"Bearer {api_key}"},
            )
            if resp.status_code != 200:
                logger.warning("fulfillment: memberships fetch HTTP %d", resp.status_code)
                break

            data = resp.json()
            memberships = data.get("data", []) or []

            if not memberships:
                break

            reached_cutoff = False
            for m in memberships:
                created_ts = m.get("created_at") or 0
                if isinstance(created_ts, str):
                    try:
                        created_ts = int(datetime.fromisoformat(
                            created_ts.replace("Z", "+00:00")
                        ).timestamp())
                    except Exception:
                        created_ts = 0

                if created_ts < after_ts:
                    reached_cutoff = True
                    break

                product_id = str(m.get("product") or m.get("access_pass") or "")
                if product_id not in allowed:
                    continue

                status = str(m.get("status") or "").lower()
                if status not in ACTIVE_STATUSES:
                    continue

                results.append(m)

            if reached_cutoff:
                break

            page_info = data.get("page_info") or {}
            if not page_info.get("has_next_page"):
                break
            cursor = page_info.get("end_cursor")

    return results


async def _process_one(
    redis_client: aioredis.Redis,
    m: dict[str, Any],
    api_key: str,
) -> None:
    membership_id = str(m.get("id") or "")
    if not membership_id:
        return

    masked = membership_id[:8] + "***"
    product_id = str(m.get("product") or m.get("access_pass") or "")
    status = str(m.get("status") or "").lower()

    try:
        if await is_fulfilled(membership_id):
            return

        lock_key = f"{settings.app_slug}:fulfillment:lock:{membership_id}"
        acquired = await redis_client.set(lock_key, "1", nx=True, ex=300)
        if not acquired:
            return

        # Double-check after acquiring lock
        if await is_fulfilled(membership_id):
            return

        user_email = str(m.get("email") or "")
        license_key = str(m.get("license_key") or "")
        manage_url = str(m.get("manage_url") or "https://whop.com/@me/settings/orders/")

        await record_fulfillment(membership_id, product_id, status, "reconciliation")

        if not user_email:
            logger.warning("fulfillment: no email for %s", masked)
            await record_fulfillment(membership_id, product_id, status, "reconciliation", error="no_email")
            return

        if not license_key:
            logger.warning("fulfillment: no license_key for %s", masked)
            await record_fulfillment(membership_id, product_id, status, "reconciliation", error="no_license_key")
            return

        product_title = await _get_product_title(api_key, product_id)
        await send_license_email(user_email, product_title, license_key, manage_url)
        await record_fulfillment(membership_id, product_id, status, "reconciliation", sent=True)
        logger.info("fulfillment: welcome email sent for %s (reconciliation)", masked)

    except Exception as exc:
        logger.exception("fulfillment: error processing %s: %s", masked, type(exc).__name__)
        try:
            await record_fulfillment(
                membership_id, product_id, status, "reconciliation",
                error=type(exc).__name__,
            )
        except Exception:
            pass


async def _reconcile_once(redis_client: aioredis.Redis) -> None:
    api_key = settings.whop_api_key
    if not api_key:
        return

    start_ts = settings.whop_reconciliation_start_ts
    if not start_ts:
        logger.debug("fulfillment: whop_reconciliation_start_ts not set, skipping tick")
        return

    memberships = await _fetch_recent_memberships(api_key, start_ts)
    for m in memberships:
        await _process_one(redis_client, m, api_key)


async def whop_reconciliation_poller() -> None:
    if not settings.whop_api_key:
        logger.info("fulfillment: WHOP_API_KEY absent — poller disabled")
        return

    await asyncio.sleep(15)  # give startup a head start
    logger.info("fulfillment: reconciliation poller started (interval=300s)")

    client = aioredis.from_url(settings.redis_url, decode_responses=True)
    try:
        while True:
            try:
                await _reconcile_once(client)
            except Exception:
                logger.exception("fulfillment: reconciliation tick failed")
            await asyncio.sleep(300)
    finally:
        await client.aclose()
