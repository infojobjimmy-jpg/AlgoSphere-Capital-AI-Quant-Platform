from __future__ import annotations

import asyncio
import logging
import secrets
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
    """Insert pre-fulfilled membership IDs from WHOP_FULFILLMENT_SEEDED_IDS config."""
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
# Schema helpers — Whop v2 API returns nested objects
# ---------------------------------------------------------------------------

def membership_product_id(m: dict) -> str:
    """Extract product ID from a Whop v2 membership dict (nested or flat fallback)."""
    product = m.get("product")
    if isinstance(product, dict):
        return str(product.get("id") or "")
    if isinstance(product, str):
        return product
    access_pass = m.get("access_pass")
    if isinstance(access_pass, dict):
        return str(access_pass.get("id") or "")
    return str(access_pass or "")


def membership_product_title(m: dict) -> str:
    """Extract product title from a Whop v2 membership dict."""
    product = m.get("product")
    if isinstance(product, dict):
        return str(product.get("title") or "")
    return ""


def membership_user_email(m: dict) -> str:
    """Extract user email from a Whop v2 membership dict (nested or flat fallback)."""
    user = m.get("user")
    if isinstance(user, dict):
        return str(user.get("email") or "")
    return str(m.get("email") or "")


# ---------------------------------------------------------------------------
# Shared fulfillment function
# ---------------------------------------------------------------------------

async def fulfill_membership(
    membership_id: str,
    m: dict,
    api_key: str,
    redis_client: aioredis.Redis,
    source: str,
    *,
    force: bool = False,
) -> tuple[bool, str]:
    """Send welcome email exactly once (or once when forced).

    Returns (sent, reason).
    Uses Redis NX lock with random token to prevent concurrent duplicate sends.
    Token-verified release prevents split-brain on lock expiry.
    """
    masked = (membership_id[:8] + "***") if len(membership_id) > 8 else membership_id

    if not force and await is_fulfilled(membership_id):
        return False, "already_fulfilled"

    lock_key = f"{settings.app_slug}:fulfillment:lock:{membership_id}"
    lock_token = secrets.token_hex(16)
    acquired = await redis_client.set(lock_key, lock_token, nx=True, ex=300)
    if not acquired:
        logger.debug("fulfillment: lock held for %s — skipping", masked)
        return False, "lock_held"

    try:
        if not force and await is_fulfilled(membership_id):
            return False, "already_fulfilled"

        product_id = membership_product_id(m)
        status = str(m.get("status") or "").lower()
        user_email = membership_user_email(m)
        license_key = str(m.get("license_key") or "")
        manage_url = str(m.get("manage_url") or "https://whop.com/@me/settings/orders/")

        if not user_email:
            logger.warning("fulfillment: no email for %s", masked)
            await record_fulfillment(membership_id, product_id, status, source, error="no_email")
            return False, "no_email"

        if not license_key:
            logger.warning("fulfillment: no license_key for %s", masked)
            await record_fulfillment(membership_id, product_id, status, source, error="no_license_key")
            return False, "no_license_key"

        product_title = membership_product_title(m) or await _get_product_title(api_key, product_id)

        await send_license_email(user_email, product_title, license_key, manage_url)
        await record_fulfillment(membership_id, product_id, status, source, sent=True)
        logger.info("fulfillment: welcome email sent for %s (%s)", masked, source)
        return True, "sent"

    except Exception as exc:
        logger.exception("fulfillment: error for %s: %s", masked, type(exc).__name__)
        try:
            await record_fulfillment(
                membership_id, membership_product_id(m),
                str(m.get("status") or "").lower(), source,
                error=type(exc).__name__,
            )
        except Exception:
            pass
        return False, f"error:{type(exc).__name__}"

    finally:
        current = await redis_client.get(lock_key)
        if current == lock_token:
            await redis_client.delete(lock_key)


# ---------------------------------------------------------------------------
# Reconciliation poller helpers
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
    """Fetch memberships from Whop v2 API and return those matching the criteria.

    Uses company_id and first=100 (not limit=100).
    Does NOT stop pagination when an old membership is found — the list may be
    unsorted, so every page is fully scanned before the cutoff check.
    """
    allowed = {x.strip() for x in settings.whop_allowed_product_ids.split(",") if x.strip()}
    results: list[dict[str, Any]] = []
    cursor: str | None = None
    pages_fetched = 0
    max_pages = settings.whop_reconciliation_max_pages

    async with httpx.AsyncClient(timeout=20.0) as client:
        while pages_fetched < max_pages:
            params: dict[str, str] = {"first": "100"}
            if settings.whop_company_id:
                params["company_id"] = settings.whop_company_id
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
            pages_fetched += 1

            if not memberships:
                break

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
                    continue  # skip old memberships; do NOT break (list may be unsorted)

                product_id = membership_product_id(m)
                if product_id not in allowed:
                    continue

                status = str(m.get("status") or "").lower()
                if status not in ACTIVE_STATUSES:
                    continue

                results.append(m)

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
    try:
        await fulfill_membership(membership_id, m, api_key, redis_client, "reconciliation")
    except Exception as exc:
        logger.exception(
            "fulfillment: unhandled error for %s: %s",
            membership_id[:8] + "***",
            type(exc).__name__,
        )


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
