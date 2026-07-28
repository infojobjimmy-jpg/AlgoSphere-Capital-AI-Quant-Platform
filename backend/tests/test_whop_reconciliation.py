"""
Unit tests for the Whop fulfillment reconciliation system.

Tests cover:
  - Schema helpers: nested (v2 API) and flat string fallback
  - _process_one / fulfill_membership: idempotence, lock, no-email, no-license
  - _fetch_recent_memberships: company_id/first=100, non-sorted pagination, max_pages
  - Webhook + poller share fulfillment state (idempotence)
  - Concurrency: two simultaneous fulfill_membership() calls → only one email
  - /account/resend-welcome: permissions, already-sent, force resend, unknown product
  - Integration (skipped without WHOP_API_KEY): live membership schema + seeded guard

No DB or SMTP required — all external dependencies are mocked.

Run:
    cd backend && python -m pytest tests/test_whop_reconciliation.py -v
"""
from __future__ import annotations

import asyncio
import os
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_MEMBERSHIP_ID = "mem_test_recon_001"
_PRODUCT_ID = "prod_bHg2Q9qH34ABM"   # Explorer — in whitelist
_UNKNOWN_PRODUCT = "prod_UNKNOWN_XYZ"

_BASE_MEMBERSHIP: dict = {
    "id": _MEMBERSHIP_ID,
    "status": "trialing",
    "product": {"id": _PRODUCT_ID, "title": "AlgoSphere Explorer — Founder"},
    "user": {"email": "member@example.com"},
    "license_key": "LK-RECON-001",
    "manage_url": "https://whop.com/@me/settings/orders/",
    "created_at": int(time.time()),
}


def _make_membership(**overrides) -> dict:
    return {**_BASE_MEMBERSHIP, **overrides}


@pytest.fixture
def mock_redis():
    client = AsyncMock()
    client.set = AsyncMock(return_value=True)    # NX lock acquired by default
    client.get = AsyncMock(return_value=None)    # no matching token → lock not released (fine for tests)
    client.delete = AsyncMock(return_value=1)
    client.aclose = AsyncMock()
    return client


# ---------------------------------------------------------------------------
# Schema helpers: nested v2 and flat string fallback
# ---------------------------------------------------------------------------

def test_schema_helpers_nested():
    """membership_product_id / membership_user_email handle Whop v2 nested dicts."""
    from app.services.whop_fulfillment import (
        membership_product_id,
        membership_product_title,
        membership_user_email,
    )
    m = _make_membership()
    assert membership_product_id(m) == _PRODUCT_ID
    assert membership_product_title(m) == "AlgoSphere Explorer — Founder"
    assert membership_user_email(m) == "member@example.com"


def test_schema_helpers_flat_fallback():
    """Schema helpers fall back to flat string product and top-level email."""
    from app.services.whop_fulfillment import (
        membership_product_id,
        membership_product_title,
        membership_user_email,
    )
    m = {"product": "prod_flat_123", "email": "flat@example.com"}
    assert membership_product_id(m) == "prod_flat_123"
    assert membership_product_title(m) == ""     # no title from flat string
    assert membership_user_email(m) == "flat@example.com"


def test_schema_helpers_access_pass_fallback():
    """membership_product_id falls back to access_pass dict when product is absent."""
    from app.services.whop_fulfillment import membership_product_id
    m = {"access_pass": {"id": "prod_ap_456"}}
    assert membership_product_id(m) == "prod_ap_456"


# ---------------------------------------------------------------------------
# _process_one / fulfill_membership — core logic
# ---------------------------------------------------------------------------

async def test_process_one_sends_email_and_marks_fulfilled(mock_redis):
    """Happy path: new membership gets email + fulfillment record."""
    from app.services.whop_fulfillment import _process_one

    with (
        patch("app.services.whop_fulfillment.is_fulfilled", new_callable=AsyncMock, return_value=False),
        patch("app.services.whop_fulfillment.record_fulfillment", new_callable=AsyncMock) as mock_record,
        patch("app.services.whop_fulfillment.send_license_email", new_callable=AsyncMock) as mock_mail,
        patch("app.config.settings.whop_allowed_product_ids", "prod_bHg2Q9qH34ABM,prod_tGJzw7gVSEPgi,prod_uqA3jSjF5t3fy"),
    ):
        await _process_one(mock_redis, _make_membership(), "fake_api_key")

    mock_mail.assert_awaited_once()
    last_call = mock_record.call_args_list[-1]
    assert last_call.kwargs.get("sent") is True


async def test_process_one_skips_if_already_fulfilled(mock_redis):
    """Already fulfilled membership is silently skipped."""
    from app.services.whop_fulfillment import _process_one

    with (
        patch("app.services.whop_fulfillment.is_fulfilled", new_callable=AsyncMock, return_value=True),
        patch("app.services.whop_fulfillment.send_license_email", new_callable=AsyncMock) as mock_mail,
    ):
        await _process_one(mock_redis, _make_membership(), "fake_api_key")

    mock_mail.assert_not_awaited()


async def test_process_one_skips_if_lock_held(mock_redis):
    """Redis NX lock already held → skip silently (another worker is sending)."""
    from app.services.whop_fulfillment import _process_one

    mock_redis.set = AsyncMock(return_value=None)   # lock NOT acquired

    with (
        patch("app.services.whop_fulfillment.is_fulfilled", new_callable=AsyncMock, return_value=False),
        patch("app.services.whop_fulfillment.send_license_email", new_callable=AsyncMock) as mock_mail,
    ):
        await _process_one(mock_redis, _make_membership(), "fake_api_key")

    mock_mail.assert_not_awaited()


async def test_process_one_skips_no_email(mock_redis):
    """Membership without user email is logged + recorded but no email sent."""
    from app.services.whop_fulfillment import _process_one

    with (
        patch("app.services.whop_fulfillment.is_fulfilled", new_callable=AsyncMock, return_value=False),
        patch("app.services.whop_fulfillment.record_fulfillment", new_callable=AsyncMock) as mock_record,
        patch("app.services.whop_fulfillment.send_license_email", new_callable=AsyncMock) as mock_mail,
        patch("app.config.settings.whop_allowed_product_ids", "prod_bHg2Q9qH34ABM,prod_tGJzw7gVSEPgi,prod_uqA3jSjF5t3fy"),
    ):
        await _process_one(mock_redis, _make_membership(user={"email": ""}), "fake_api_key")

    mock_mail.assert_not_awaited()
    last_call = mock_record.call_args_list[-1]
    assert last_call.kwargs.get("error") == "no_email"


async def test_process_one_skips_no_license_key(mock_redis):
    """Membership without license_key is logged but no email sent."""
    from app.services.whop_fulfillment import _process_one

    with (
        patch("app.services.whop_fulfillment.is_fulfilled", new_callable=AsyncMock, return_value=False),
        patch("app.services.whop_fulfillment.record_fulfillment", new_callable=AsyncMock) as mock_record,
        patch("app.services.whop_fulfillment.send_license_email", new_callable=AsyncMock) as mock_mail,
        patch("app.config.settings.whop_allowed_product_ids", "prod_bHg2Q9qH34ABM,prod_tGJzw7gVSEPgi,prod_uqA3jSjF5t3fy"),
    ):
        await _process_one(mock_redis, _make_membership(license_key=""), "fake_api_key")

    mock_mail.assert_not_awaited()
    last_call = mock_record.call_args_list[-1]
    assert last_call.kwargs.get("error") == "no_license_key"


# ---------------------------------------------------------------------------
# Concurrency: two simultaneous fulfill_membership() calls → one email only
# ---------------------------------------------------------------------------

async def test_concurrent_two_pollers_only_one_email():
    """Two concurrent fulfill_membership() calls → lock prevents double send."""
    from app.services.whop_fulfillment import fulfill_membership

    m = _make_membership()
    lock_held = [False]

    async def mock_set(key, value, nx=False, ex=None):
        if not lock_held[0]:
            lock_held[0] = True
            return True     # first caller acquires lock
        return None         # second caller blocked

    redis_mock = AsyncMock()
    redis_mock.set = AsyncMock(side_effect=mock_set)
    redis_mock.get = AsyncMock(return_value=None)
    redis_mock.delete = AsyncMock()

    with (
        patch("app.services.whop_fulfillment.is_fulfilled", new_callable=AsyncMock, return_value=False),
        patch("app.services.whop_fulfillment.record_fulfillment", new_callable=AsyncMock),
        patch("app.services.whop_fulfillment.send_license_email", new_callable=AsyncMock) as mock_mail,
    ):
        results = await asyncio.gather(
            fulfill_membership(_MEMBERSHIP_ID, m, "key", redis_mock, "poller_a"),
            fulfill_membership(_MEMBERSHIP_ID, m, "key", redis_mock, "poller_b"),
        )

    assert mock_mail.await_count == 1
    sent_count = sum(1 for sent, _ in results if sent)
    assert sent_count == 1


async def test_concurrent_two_force_resends_only_one_email():
    """Two concurrent force=True fulfill_membership() calls → only one email."""
    from app.services.whop_fulfillment import fulfill_membership

    m = _make_membership()
    lock_held = [False]

    async def mock_set(key, value, nx=False, ex=None):
        if not lock_held[0]:
            lock_held[0] = True
            return True
        return None

    redis_mock = AsyncMock()
    redis_mock.set = AsyncMock(side_effect=mock_set)
    redis_mock.get = AsyncMock(return_value=None)
    redis_mock.delete = AsyncMock()

    with (
        patch("app.services.whop_fulfillment.is_fulfilled", new_callable=AsyncMock, return_value=True),
        patch("app.services.whop_fulfillment.record_fulfillment", new_callable=AsyncMock),
        patch("app.services.whop_fulfillment.send_license_email", new_callable=AsyncMock) as mock_mail,
    ):
        results = await asyncio.gather(
            fulfill_membership(_MEMBERSHIP_ID, m, "key", redis_mock, "owner_1", force=True),
            fulfill_membership(_MEMBERSHIP_ID, m, "key", redis_mock, "owner_2", force=True),
        )

    assert mock_mail.await_count == 1
    sent_count = sum(1 for sent, _ in results if sent)
    assert sent_count == 1


# ---------------------------------------------------------------------------
# _fetch_recent_memberships: API parameters and pagination behavior
# ---------------------------------------------------------------------------

def _make_http_mock_multi(*pages):
    """Return a mock httpx.AsyncClient that serves each page in sequence."""
    resps = []
    for page in pages:
        r = MagicMock()
        r.status_code = 200
        r.json.return_value = page
        resps.append(r)
    mock_client = AsyncMock()
    mock_client.get = AsyncMock(side_effect=resps)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    return mock_client


async def test_fetch_uses_first_and_company_id():
    """_fetch_recent_memberships sends first=100 and company_id when configured."""
    from app.services.whop_fulfillment import _fetch_recent_memberships

    page = {"data": [], "page_info": {"has_next_page": False}}
    mock_client = _make_http_mock_multi(page)

    with (
        patch("app.config.settings.whop_allowed_product_ids", "prod_bHg2Q9qH34ABM"),
        patch("app.config.settings.whop_company_id", "biz_test_company"),
        patch("app.config.settings.whop_reconciliation_max_pages", 20),
        patch("httpx.AsyncClient", return_value=mock_client),
    ):
        await _fetch_recent_memberships("key", after_ts=0)

    params_sent = mock_client.get.call_args.kwargs.get("params", {})
    assert params_sent.get("first") == "100"
    assert params_sent.get("company_id") == "biz_test_company"


async def test_fetch_omits_company_id_when_not_configured():
    """No company_id in params when whop_company_id is empty."""
    from app.services.whop_fulfillment import _fetch_recent_memberships

    page = {"data": [], "page_info": {"has_next_page": False}}
    mock_client = _make_http_mock_multi(page)

    with (
        patch("app.config.settings.whop_allowed_product_ids", "prod_bHg2Q9qH34ABM"),
        patch("app.config.settings.whop_company_id", ""),
        patch("httpx.AsyncClient", return_value=mock_client),
    ):
        await _fetch_recent_memberships("key", after_ts=0)

    params_sent = mock_client.get.call_args.kwargs.get("params", {})
    assert "company_id" not in params_sent


async def test_fetch_forwards_cursor_on_second_page():
    """Pagination cursor from page_info.end_cursor is forwarded as after= param."""
    from app.services.whop_fulfillment import _fetch_recent_memberships

    # Page 1 must be non-empty so the function reads page_info instead of breaking.
    # Use an expired membership so it's filtered but pagination still continues.
    page1 = {
        "data": [_make_membership(id="mem_p1", status="expired")],
        "page_info": {"has_next_page": True, "end_cursor": "cursor_abc"},
    }
    page2 = {"data": [], "page_info": {"has_next_page": False}}
    mock_client = _make_http_mock_multi(page1, page2)

    with (
        patch("app.config.settings.whop_allowed_product_ids", "prod_bHg2Q9qH34ABM"),
        patch("app.config.settings.whop_company_id", ""),
        patch("app.config.settings.whop_reconciliation_max_pages", 20),
        patch("httpx.AsyncClient", return_value=mock_client),
    ):
        await _fetch_recent_memberships("key", after_ts=0)

    assert mock_client.get.call_count == 2
    second_params = mock_client.get.call_args_list[1].kwargs.get("params", {})
    assert second_params.get("after") == "cursor_abc"


async def test_fetch_filters_unknown_product():
    """Memberships for products not in whitelist are excluded."""
    from app.services.whop_fulfillment import _fetch_recent_memberships

    page = {
        "data": [_make_membership(product={"id": _UNKNOWN_PRODUCT, "title": "Other"})],
        "page_info": {"has_next_page": False},
    }
    mock_client = _make_http_mock_multi(page)

    with (
        patch("app.config.settings.whop_allowed_product_ids", "prod_bHg2Q9qH34ABM,prod_tGJzw7gVSEPgi,prod_uqA3jSjF5t3fy"),
        patch("app.config.settings.whop_company_id", ""),
        patch("httpx.AsyncClient", return_value=mock_client),
    ):
        results = await _fetch_recent_memberships("key", after_ts=0)

    assert results == []


async def test_fetch_filters_expired_status():
    """Memberships with status 'expired' are excluded."""
    from app.services.whop_fulfillment import _fetch_recent_memberships

    page = {
        "data": [_make_membership(status="expired")],
        "page_info": {"has_next_page": False},
    }
    mock_client = _make_http_mock_multi(page)

    with (
        patch("app.config.settings.whop_allowed_product_ids", "prod_bHg2Q9qH34ABM"),
        patch("app.config.settings.whop_company_id", ""),
        patch("httpx.AsyncClient", return_value=mock_client),
    ):
        results = await _fetch_recent_memberships("key", after_ts=0)

    assert results == []


async def test_fetch_skips_old_membership_but_continues_pagination():
    """Old memberships are skipped (not stop-pagination) — list may be unsorted."""
    from app.services.whop_fulfillment import _fetch_recent_memberships

    now = int(time.time())
    old_ts = now - 10_000
    after_ts = now - 100

    # Page 1: old membership first; page 2: fresh membership
    page1 = {
        "data": [_make_membership(id="mem_old", created_at=old_ts)],
        "page_info": {"has_next_page": True, "end_cursor": "c1"},
    }
    page2 = {
        "data": [_make_membership(id="mem_new", created_at=now)],
        "page_info": {"has_next_page": False},
    }
    mock_client = _make_http_mock_multi(page1, page2)

    with (
        patch("app.config.settings.whop_allowed_product_ids", "prod_bHg2Q9qH34ABM"),
        patch("app.config.settings.whop_company_id", ""),
        patch("app.config.settings.whop_reconciliation_max_pages", 20),
        patch("httpx.AsyncClient", return_value=mock_client),
    ):
        results = await _fetch_recent_memberships("key", after_ts=after_ts)

    ids = [r["id"] for r in results]
    assert "mem_new" in ids
    assert "mem_old" not in ids
    assert mock_client.get.call_count == 2   # did NOT stop after the old one


async def test_fetch_respects_max_pages():
    """Pagination stops after whop_reconciliation_max_pages pages."""
    from app.services.whop_fulfillment import _fetch_recent_memberships

    # Every page reports has_next_page=True
    pages = [
        {
            "data": [_make_membership(id=f"mem_{i}")],
            "page_info": {"has_next_page": True, "end_cursor": f"c{i}"},
        }
        for i in range(10)
    ]
    mock_client = _make_http_mock_multi(*pages)

    with (
        patch("app.config.settings.whop_allowed_product_ids", "prod_bHg2Q9qH34ABM"),
        patch("app.config.settings.whop_company_id", ""),
        patch("app.config.settings.whop_reconciliation_max_pages", 3),
        patch("httpx.AsyncClient", return_value=mock_client),
    ):
        await _fetch_recent_memberships("key", after_ts=0)

    assert mock_client.get.call_count == 3


# ---------------------------------------------------------------------------
# Webhook idempotence: activated event triggers fulfill_membership
# ---------------------------------------------------------------------------

def _make_webhook_app():
    from app.routers.webhooks import router as wh_router
    mini = FastAPI()
    mini.include_router(wh_router, prefix="/webhooks")
    return mini


_WH_TEST_SECRET = "test-whop-webhook-raw-secret-from-dashboard"


def _sign_with(body: str, secret: str = _WH_TEST_SECRET) -> dict:
    import base64, hashlib, hmac as _hmac, uuid
    mid = f"msg_{uuid.uuid4().hex}"
    ts = str(int(time.time()))
    key = secret.encode("utf-8")
    sig = base64.b64encode(
        _hmac.new(key, f"{mid}.{ts}.{body}".encode(), hashlib.sha256).digest()
    ).decode()
    return {
        "webhook-id": mid, "webhook-timestamp": ts,
        "webhook-signature": f"v1,{sig}", "content-type": "application/json",
    }


def _activated_body(
    membership_id: str = "mem_test_001",
    product_id: str = "prod_bHg2Q9qH34ABM",
    user_email: str = "buyer@example.com",
    license_key: str = "LK-UNIT-999",
) -> str:
    import json as _json, uuid
    return _json.dumps({
        "type": "membership.activated",
        "id": f"evt_{uuid.uuid4().hex}",
        "api_version": "v1",
        "company_id": "biz_test",
        "timestamp": str(int(time.time())),
        "data": {
            "id": membership_id, "license_key": license_key,
            "manage_url": "https://whop.com/hub/test",
            "cancel_at_period_end": False, "status": "active",
            "product": {"id": product_id, "title": "AlgoSphere Explorer", "metadata": None},
            "user": {"id": "usr_test", "email": user_email, "name": "Test", "username": "tst"},
        },
    })


@pytest.fixture
def mock_redis_fixture():
    client = AsyncMock()
    client.exists = AsyncMock(return_value=0)
    client.set = AsyncMock(return_value=True)
    client.get = AsyncMock(return_value=None)
    client.delete = AsyncMock(return_value=1)
    client.aclose = AsyncMock()
    return client


async def test_webhook_writes_fulfillment_on_activation(mock_redis_fixture):
    """membership.activated webhook triggers fulfill_membership and records sent=True."""
    with (
        patch("app.config.settings.whop_webhook_secret", _WH_TEST_SECRET),
        patch("app.config.settings.whop_allowed_product_ids", "prod_bHg2Q9qH34ABM"),
        patch("app.routers.webhooks.aioredis.from_url", return_value=mock_redis_fixture),
        patch("app.routers.webhooks.restore_membership", new_callable=AsyncMock),
        patch("app.services.whop_fulfillment.is_fulfilled", new_callable=AsyncMock, return_value=False),
        patch("app.services.whop_fulfillment.send_license_email", new_callable=AsyncMock),
        patch("app.services.whop_fulfillment.record_fulfillment", new_callable=AsyncMock) as mock_rf,
    ):
        app = _make_webhook_app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            body = _activated_body(license_key="LK-WH-001", user_email="x@y.com")
            r = await client.post("/webhooks/whop", content=body, headers=_sign_with(body))

    assert r.status_code == 200, r.text
    assert r.json()["action"] == "activated"
    mock_rf.assert_awaited()
    last = mock_rf.call_args_list[-1]
    assert last.kwargs.get("sent") is True


# ---------------------------------------------------------------------------
# /account/resend-welcome endpoint
# ---------------------------------------------------------------------------

def _make_account_app():
    from app.routers.account import router as acc_router
    mini = FastAPI()
    mini.include_router(acc_router, prefix="/account")
    return mini


def _owner_token() -> str:
    from app.services.whop_auth import create_session
    with patch("app.config.settings.auth_session_secret", "test-session-secret"):
        return create_session({"role": "owner"}, hours=1)


def _member_token() -> str:
    from app.services.whop_auth import create_session
    with patch("app.config.settings.auth_session_secret", "test-session-secret"):
        return create_session({"role": "member", "membership_id": "mem_x"}, hours=1)


def _mock_whop_membership(**overrides) -> MagicMock:
    """Return a mock httpx context that serves the given membership dict."""
    m = {
        "id": "mem_abc",
        "status": "active",
        "product": {"id": "prod_bHg2Q9qH34ABM", "title": "AlgoSphere Explorer"},
        "user": {"email": "x@y.com"},
        "license_key": "LK-X",
        "manage_url": "https://whop.com/@me/",
        **overrides,
    }
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = m
    mock_http = AsyncMock()
    mock_http.get = AsyncMock(return_value=resp)
    mock_http.__aenter__ = AsyncMock(return_value=mock_http)
    mock_http.__aexit__ = AsyncMock(return_value=False)
    return mock_http


async def test_resend_welcome_forbidden_for_member():
    """Non-owner session → 403."""
    app = _make_account_app()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.post(
            "/account/resend-welcome",
            json={"membership_id": "mem_test"},
            cookies={"algosphere_session": _member_token()},
        )
    assert r.status_code == 403


async def test_resend_welcome_returns_already_sent_when_not_forced():
    """Already-sent membership returns already_sent without re-sending."""
    redis_mock = AsyncMock()
    redis_mock.aclose = AsyncMock()

    with (
        patch("app.config.settings.auth_session_secret", "test-session-secret"),
        patch("app.config.settings.whop_api_key", "fake_key"),
        patch("app.config.settings.whop_allowed_product_ids", "prod_bHg2Q9qH34ABM"),
        patch("app.services.whop_fulfillment.is_fulfilled", new_callable=AsyncMock, return_value=True),
        patch("app.services.whop_fulfillment.send_license_email", new_callable=AsyncMock) as mock_mail,
        patch("app.routers.account.redis.from_url", return_value=redis_mock),
        patch("httpx.AsyncClient", return_value=_mock_whop_membership()),
    ):
        app = _make_account_app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            r = await client.post(
                "/account/resend-welcome",
                json={"membership_id": "mem_abc"},
                cookies={"algosphere_session": _owner_token()},
            )

    assert r.status_code == 200
    assert r.json()["action"] == "already_sent"
    mock_mail.assert_not_awaited()


async def test_resend_welcome_sends_when_forced():
    """force=True re-sends even if already fulfilled; product title from nested dict."""
    redis_mock = AsyncMock()
    redis_mock.set = AsyncMock(return_value=True)
    redis_mock.get = AsyncMock(return_value=None)
    redis_mock.delete = AsyncMock()
    redis_mock.aclose = AsyncMock()

    with (
        patch("app.config.settings.auth_session_secret", "test-session-secret"),
        patch("app.config.settings.whop_api_key", "fake_key"),
        patch("app.config.settings.whop_allowed_product_ids", "prod_bHg2Q9qH34ABM"),
        patch("app.services.whop_fulfillment.is_fulfilled", new_callable=AsyncMock, return_value=True),
        patch("app.services.whop_fulfillment.record_fulfillment", new_callable=AsyncMock),
        patch("app.services.whop_fulfillment.send_license_email", new_callable=AsyncMock) as mock_mail,
        patch("app.routers.account.redis.from_url", return_value=redis_mock),
        patch("httpx.AsyncClient", return_value=_mock_whop_membership()),
    ):
        app = _make_account_app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            r = await client.post(
                "/account/resend-welcome",
                json={"membership_id": "mem_abc", "force": True},
                cookies={"algosphere_session": _owner_token()},
            )

    assert r.status_code == 200
    assert r.json()["action"] == "sent"
    assert r.json()["license_returned"] is False
    mock_mail.assert_awaited_once()


async def test_resend_welcome_rejects_unknown_product():
    """Product not in whitelist → 403."""
    with (
        patch("app.config.settings.auth_session_secret", "test-session-secret"),
        patch("app.config.settings.whop_api_key", "fake_key"),
        patch("app.config.settings.whop_allowed_product_ids", "prod_bHg2Q9qH34ABM"),
        patch("httpx.AsyncClient", return_value=_mock_whop_membership(
            product={"id": "prod_UNKNOWN", "title": "Other"}
        )),
    ):
        app = _make_account_app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            r = await client.post(
                "/account/resend-welcome",
                json={"membership_id": "mem_abc"},
                cookies={"algosphere_session": _owner_token()},
            )

    assert r.status_code == 403


# ---------------------------------------------------------------------------
# Integration test — skipped without WHOP_API_KEY
# ---------------------------------------------------------------------------

@pytest.mark.skipif(
    not os.environ.get("WHOP_API_KEY"),
    reason="WHOP_API_KEY not set — skipping live integration test",
)
async def test_integration_seeded_membership_not_resent():
    """Seeded membership mem_ptowVxQxH6XaeS is already_fulfilled → no email sent."""
    from app.services.whop_fulfillment import fulfill_membership

    membership_id = "mem_ptowVxQxH6XaeS"
    m = _make_membership(id=membership_id)

    redis_mock = AsyncMock()
    redis_mock.set = AsyncMock(return_value=True)
    redis_mock.get = AsyncMock(return_value=None)
    redis_mock.delete = AsyncMock()

    with (
        patch("app.services.whop_fulfillment.is_fulfilled", new_callable=AsyncMock, return_value=True),
        patch("app.services.whop_fulfillment.send_license_email", new_callable=AsyncMock) as mock_mail,
    ):
        sent, reason = await fulfill_membership(
            membership_id, m, os.environ["WHOP_API_KEY"], redis_mock, "integration_test"
        )

    assert sent is False
    assert reason == "already_fulfilled"
    mock_mail.assert_not_awaited()
