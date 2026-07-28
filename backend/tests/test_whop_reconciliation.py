"""
Unit tests for the Whop fulfillment reconciliation system.

Tests cover:
  - Idempotence: webhook + poller share the same fulfillment state
  - Membership without license key
  - Membership without email
  - Product not in whitelist
  - Membership already sent
  - force=True override on resend-welcome
  - Redis NX lock prevents concurrent sends
  - record_fulfillment COALESCE logic

No network calls, DB, or Redis required — all external dependencies are mocked.

Run:
    cd backend && python -m pytest tests/test_whop_reconciliation.py -v
"""
from __future__ import annotations

import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_MEMBERSHIP_ID = "mem_test_recon_001"
_PRODUCT_ID = "prod_bHg2Q9qH34ABM"  # Explorer — in whitelist
_UNKNOWN_PRODUCT = "prod_UNKNOWN_XYZ"

_BASE_MEMBERSHIP = {
    "id": _MEMBERSHIP_ID,
    "product": _PRODUCT_ID,
    "access_pass": _PRODUCT_ID,
    "status": "trialing",
    "email": "member@example.com",
    "license_key": "LK-RECON-001",
    "manage_url": "https://whop.com/@me/settings/orders/",
    "created_at": int(time.time()),
}


def _make_membership(**overrides) -> dict:
    return {**_BASE_MEMBERSHIP, **overrides}


# ---------------------------------------------------------------------------
# _process_one tests (core reconciliation logic)
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_redis():
    client = AsyncMock()
    client.set = AsyncMock(return_value=True)   # NX lock acquired by default
    client.aclose = AsyncMock()
    return client


async def test_process_one_sends_email_and_marks_fulfilled(mock_redis):
    """Happy path: new membership gets email + fulfillment record."""
    from app.services.whop_fulfillment import _process_one

    with (
        patch("app.services.whop_fulfillment.is_fulfilled", new_callable=AsyncMock, return_value=False),
        patch("app.services.whop_fulfillment.record_fulfillment", new_callable=AsyncMock) as mock_record,
        patch("app.services.whop_fulfillment.send_license_email", new_callable=AsyncMock) as mock_mail,
        patch("app.services.whop_fulfillment._get_product_title", new_callable=AsyncMock, return_value="AlgoSphere Explorer"),
        patch("app.config.settings.whop_allowed_product_ids", "prod_bHg2Q9qH34ABM,prod_tGJzw7gVSEPgi,prod_uqA3jSjF5t3fy"),
    ):
        await _process_one(mock_redis, _make_membership(), "fake_api_key")

    mock_mail.assert_awaited_once()
    # Last record_fulfillment call must have sent=True
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

    mock_redis.set = AsyncMock(return_value=None)  # lock NOT acquired (another worker holds it)

    with (
        patch("app.services.whop_fulfillment.is_fulfilled", new_callable=AsyncMock, return_value=False),
        patch("app.services.whop_fulfillment.send_license_email", new_callable=AsyncMock) as mock_mail,
    ):
        await _process_one(mock_redis, _make_membership(), "fake_api_key")

    mock_mail.assert_not_awaited()


async def test_process_one_skips_no_email(mock_redis):
    """Membership without email is logged + recorded but no email sent."""
    from app.services.whop_fulfillment import _process_one

    with (
        patch("app.services.whop_fulfillment.is_fulfilled", new_callable=AsyncMock, return_value=False),
        patch("app.services.whop_fulfillment.record_fulfillment", new_callable=AsyncMock) as mock_record,
        patch("app.services.whop_fulfillment.send_license_email", new_callable=AsyncMock) as mock_mail,
        patch("app.config.settings.whop_allowed_product_ids", "prod_bHg2Q9qH34ABM,prod_tGJzw7gVSEPgi,prod_uqA3jSjF5t3fy"),
    ):
        await _process_one(mock_redis, _make_membership(email=""), "fake_api_key")

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
# _fetch_recent_memberships filtering
# ---------------------------------------------------------------------------

def _make_http_mock(fake_response: dict):
    """Return a mock httpx.AsyncClient that returns fake_response for .get()."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = fake_response
    mock_client = AsyncMock()
    mock_client.get = AsyncMock(return_value=mock_resp)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    return mock_client, mock_resp


async def test_fetch_filters_unknown_product():
    """Memberships for products not in whitelist are excluded."""
    from app.services.whop_fulfillment import _fetch_recent_memberships

    fake_response = {
        "data": [_make_membership(product=_UNKNOWN_PRODUCT)],
        "page_info": {"has_next_page": False},
    }
    mock_client, _ = _make_http_mock(fake_response)

    with (
        patch("app.config.settings.whop_allowed_product_ids", "prod_bHg2Q9qH34ABM,prod_tGJzw7gVSEPgi,prod_uqA3jSjF5t3fy"),
        patch("httpx.AsyncClient", return_value=mock_client),
    ):
        results = await _fetch_recent_memberships("key", after_ts=0)

    assert results == []


async def test_fetch_filters_expired_status():
    """Memberships with status 'expired' are excluded."""
    from app.services.whop_fulfillment import _fetch_recent_memberships

    fake_response = {
        "data": [_make_membership(status="expired")],
        "page_info": {"has_next_page": False},
    }
    mock_client, _ = _make_http_mock(fake_response)

    with (
        patch("app.config.settings.whop_allowed_product_ids", "prod_bHg2Q9qH34ABM"),
        patch("httpx.AsyncClient", return_value=mock_client),
    ):
        results = await _fetch_recent_memberships("key", after_ts=0)

    assert results == []


async def test_fetch_stops_at_cutoff_timestamp():
    """Memberships older than after_ts are excluded and stop pagination."""
    from app.services.whop_fulfillment import _fetch_recent_memberships

    old_ts = int(time.time()) - 10000
    fake_response = {
        "data": [_make_membership(created_at=old_ts)],
        "page_info": {"has_next_page": True, "end_cursor": "abc"},
    }
    mock_client, _ = _make_http_mock(fake_response)

    with (
        patch("app.config.settings.whop_allowed_product_ids", "prod_bHg2Q9qH34ABM"),
        patch("httpx.AsyncClient", return_value=mock_client),
    ):
        results = await _fetch_recent_memberships("key", after_ts=int(time.time()) - 100)

    assert results == []
    assert mock_client.get.call_count == 1


# ---------------------------------------------------------------------------
# Idempotence: webhook and poller share fulfillment state
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
    signed = f"{mid}.{ts}.{body}"
    sig = base64.b64encode(
        _hmac.new(key, signed.encode(), hashlib.sha256).digest()
    ).decode()
    return {"webhook-id": mid, "webhook-timestamp": ts,
            "webhook-signature": f"v1,{sig}", "content-type": "application/json"}


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


async def test_webhook_writes_fulfillment_on_activation(mock_redis_fixture):
    """membership.activated webhook writes to whop_fulfillment after sending email."""
    with (
        patch("app.config.settings.whop_webhook_secret", _WH_TEST_SECRET),
        patch("app.config.settings.whop_allowed_product_ids", "prod_bHg2Q9qH34ABM"),
        patch("app.routers.webhooks.aioredis.from_url", return_value=mock_redis_fixture),
        patch("app.routers.webhooks.restore_membership", new_callable=AsyncMock),
        patch("app.routers.webhooks.send_license_email", new_callable=AsyncMock),
        patch("app.routers.webhooks.record_fulfillment", new_callable=AsyncMock) as mock_rf,
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


@pytest.fixture
def mock_redis_fixture():
    client = AsyncMock()
    client.exists = AsyncMock(return_value=0)
    client.set = AsyncMock(return_value=True)
    client.aclose = AsyncMock()
    return client


# ---------------------------------------------------------------------------
# /account/resend-welcome endpoint
# ---------------------------------------------------------------------------

def _make_account_app():
    from app.routers.account import router as acc_router
    mini = FastAPI()
    mini.include_router(acc_router, prefix="/account")
    return mini


def _owner_session() -> str:
    from app.services.whop_auth import create_session
    with patch("app.config.settings.auth_session_secret", "test-session-secret"):
        return create_session({"role": "owner"}, hours=1)


async def test_resend_welcome_forbidden_for_member():
    """Non-owner session → 403."""
    from app.services.whop_auth import create_session
    with patch("app.config.settings.auth_session_secret", "test-session-secret"):
        member_token = create_session({"role": "member", "membership_id": "mem_x"}, hours=1)

    app = _make_account_app()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.post(
            "/account/resend-welcome",
            json={"membership_id": "mem_test"},
            cookies={"algosphere_session": member_token},
        )
    assert r.status_code == 403


async def test_resend_welcome_returns_already_sent_when_not_forced():
    """Already-sent membership returns already_sent without re-sending."""
    with (
        patch("app.config.settings.auth_session_secret", "test-session-secret"),
        patch("app.config.settings.whop_api_key", "fake_key"),
        patch("app.routers.account.is_fulfilled", new_callable=AsyncMock, return_value=True),
        patch("app.routers.account.send_license_email", new_callable=AsyncMock) as mock_mail,
        patch("httpx.AsyncClient") as mock_cls,
    ):
        mock_m = {
            "id": "mem_abc", "product": "prod_bHg2Q9qH34ABM", "access_pass": "prod_bHg2Q9qH34ABM",
            "status": "active", "email": "x@y.com", "license_key": "LK-X",
            "manage_url": "https://whop.com/@me/",
        }
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = mock_m
        mock_http = AsyncMock()
        mock_http.get = AsyncMock(return_value=mock_resp)
        mock_http.__aenter__ = AsyncMock(return_value=mock_http)
        mock_http.__aexit__ = AsyncMock(return_value=False)
        mock_cls.return_value = mock_http

        from app.services.whop_auth import create_session
        owner_token = create_session({"role": "owner"}, hours=1)

        app = _make_account_app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            r = await client.post(
                "/account/resend-welcome",
                json={"membership_id": "mem_abc"},
                cookies={"algosphere_session": owner_token},
            )

    assert r.status_code == 200
    assert r.json()["action"] == "already_sent"
    mock_mail.assert_not_awaited()


async def test_resend_welcome_sends_when_forced():
    """force=True re-sends even if already fulfilled."""
    with (
        patch("app.config.settings.auth_session_secret", "test-session-secret"),
        patch("app.config.settings.whop_api_key", "fake_key"),
        patch("app.config.settings.whop_allowed_product_ids", "prod_bHg2Q9qH34ABM"),
        patch("app.routers.account.is_fulfilled", new_callable=AsyncMock, return_value=True),
        patch("app.routers.account.record_fulfillment", new_callable=AsyncMock),
        patch("app.routers.account.send_license_email", new_callable=AsyncMock) as mock_mail,
        patch("httpx.AsyncClient") as mock_cls,
    ):
        mock_m = {
            "id": "mem_abc", "product": "prod_bHg2Q9qH34ABM", "access_pass": "prod_bHg2Q9qH34ABM",
            "status": "active", "email": "x@y.com", "license_key": "LK-X",
            "manage_url": "https://whop.com/@me/",
        }
        mock_resp_m = MagicMock(); mock_resp_m.status_code = 200; mock_resp_m.json.return_value = mock_m
        mock_resp_p = MagicMock(); mock_resp_p.status_code = 200; mock_resp_p.json.return_value = {"title": "Explorer"}
        mock_http = AsyncMock()
        mock_http.get = AsyncMock(side_effect=[mock_resp_m, mock_resp_p])
        mock_http.__aenter__ = AsyncMock(return_value=mock_http)
        mock_http.__aexit__ = AsyncMock(return_value=False)
        mock_cls.return_value = mock_http

        from app.services.whop_auth import create_session
        owner_token = create_session({"role": "owner"}, hours=1)

        app = _make_account_app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            r = await client.post(
                "/account/resend-welcome",
                json={"membership_id": "mem_abc", "force": True},
                cookies={"algosphere_session": owner_token},
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
        patch("httpx.AsyncClient") as mock_cls,
    ):
        mock_m = {
            "id": "mem_abc", "product": "prod_UNKNOWN", "access_pass": "prod_UNKNOWN",
            "status": "active", "email": "x@y.com", "license_key": "LK-X",
            "manage_url": "https://whop.com/@me/",
        }
        mock_resp = MagicMock(); mock_resp.status_code = 200; mock_resp.json.return_value = mock_m
        mock_http = AsyncMock()
        mock_http.get = AsyncMock(return_value=mock_resp)
        mock_http.__aenter__ = AsyncMock(return_value=mock_http)
        mock_http.__aexit__ = AsyncMock(return_value=False)
        mock_cls.return_value = mock_http

        from app.services.whop_auth import create_session
        owner_token = create_session({"role": "owner"}, hours=1)

        app = _make_account_app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            r = await client.post(
                "/account/resend-welcome",
                json={"membership_id": "mem_abc"},
                cookies={"algosphere_session": owner_token},
            )

    assert r.status_code == 403
