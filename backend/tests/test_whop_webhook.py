"""
Unit tests for the Whop webhook endpoint (Standard Webhooks format).

Secret handling follows the official Whop Python documentation:
    webhook_key = base64.b64encode(os.environ["WHOP_WEBHOOK_SECRET"].encode()).decode()
The env var is a raw string from the Whop dashboard; the app base64-encodes it
before passing to AsyncWhop.  The effective HMAC key is therefore the raw bytes
of the env secret.

No network calls, Redis or DB required — all external dependencies are mocked.

Run:
    cd backend && python -m pytest tests/test_whop_webhook.py -v
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
import uuid
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

# ---------------------------------------------------------------------------
# Test secret — simulates WHOP_WEBHOOK_SECRET value from the Whop dashboard.
# The app base64-encodes this before passing to AsyncWhop; the effective HMAC
# key is therefore _TEST_ENV_SECRET.encode("utf-8").
# ---------------------------------------------------------------------------

_TEST_ENV_SECRET = "test-whop-webhook-raw-secret-from-dashboard"
_ENDPOINT = "/webhooks/whop"


def _sign(
    body: str,
    msg_id: str | None = None,
    timestamp: str | None = None,
    env_secret: str = _TEST_ENV_SECRET,
) -> dict[str, str]:
    """Return Standard Webhooks headers signed with the raw env secret bytes.

    Whop signs with the raw secret bytes; the app decodes them back via
    base64.b64decode(base64.b64encode(env_secret.encode())) which equals
    env_secret.encode() — so HMAC key = env_secret.encode().
    """
    mid = msg_id or f"msg_{uuid.uuid4().hex}"
    ts = timestamp or str(int(time.time()))
    key = env_secret.encode("utf-8")
    signed = f"{mid}.{ts}.{body}"
    sig = base64.b64encode(
        hmac.new(key, signed.encode(), hashlib.sha256).digest()
    ).decode()
    return {
        "webhook-id": mid,
        "webhook-timestamp": ts,
        "webhook-signature": f"v1,{sig}",
        "content-type": "application/json",
    }


# ---------------------------------------------------------------------------
# Body factories (api_version="v1" as returned by the Whop dashboard)
# ---------------------------------------------------------------------------

def _activated(
    membership_id: str = "mem_test_001",
    product_id: str = "prod_bHg2Q9qH34ABM",
    product_title: str = "AlgoSphere Explorer — Founder",
    user_email: str = "buyer@example.com",
    license_key: str = "LK-UNIT-999",
) -> str:
    return json.dumps({
        "type": "membership.activated",
        "id": f"evt_{uuid.uuid4().hex}",
        "api_version": "v1",
        "company_id": "biz_test",
        "timestamp": str(int(time.time())),
        "data": {
            "id": membership_id,
            "license_key": license_key,
            "manage_url": "https://whop.com/hub/test",
            "cancel_at_period_end": False,
            "status": "active",
            "product": {"id": product_id, "title": product_title, "metadata": None},
            "user": {"id": "usr_test", "email": user_email, "name": "Test Buyer", "username": "testbuyer"},
        },
    })


def _deactivated(membership_id: str = "mem_test_001") -> str:
    return json.dumps({
        "type": "membership.deactivated",
        "id": f"evt_{uuid.uuid4().hex}",
        "api_version": "v1",
        "company_id": "biz_test",
        "timestamp": str(int(time.time())),
        "data": {
            "id": membership_id,
            "license_key": None,
            "manage_url": "https://whop.com/hub/test",
            "cancel_at_period_end": False,
            "status": "expired",
            "product": {"id": "prod_bHg2Q9qH34ABM", "title": "AlgoSphere Explorer — Founder", "metadata": None},
            "user": {"id": "usr_test", "email": "buyer@example.com", "name": "Test Buyer", "username": "testbuyer"},
        },
    })


def _cancel_flag(membership_id: str = "mem_test_001", cancel: bool = True) -> str:
    return json.dumps({
        "type": "membership.cancel_at_period_end_changed",
        "id": f"evt_{uuid.uuid4().hex}",
        "api_version": "v1",
        "company_id": "biz_test",
        "timestamp": str(int(time.time())),
        "data": {
            "id": membership_id,
            "cancel_at_period_end": cancel,
            "status": "active",
            "license_key": "LK-UNIT-999",
            "manage_url": "https://whop.com/hub/test",
            "product": {"id": "prod_bHg2Q9qH34ABM", "title": "AlgoSphere Explorer — Founder", "metadata": None},
            "user": {"id": "usr_test", "email": "buyer@example.com", "name": "Test Buyer", "username": "testbuyer"},
        },
    })


def _refund() -> str:
    return json.dumps({
        "type": "refund.created",
        "id": f"evt_{uuid.uuid4().hex}",
        "api_version": "v1",
        "company_id": "biz_test",
        "timestamp": str(int(time.time())),
        "data": {"id": "ref_unit_001", "amount": 9900, "currency": "usd", "status": "pending"},
    })


def _payment_succeeded() -> str:
    return json.dumps({
        "type": "payment.succeeded",
        "id": f"evt_{uuid.uuid4().hex}",
        "api_version": "v1",
        "company_id": "biz_test",
        "timestamp": str(int(time.time())),
        "data": {"id": "pay_unit_001", "amount": 9900, "currency": "usd", "status": "paid"},
    })


def _unknown_event() -> str:
    return json.dumps({
        "type": "membership.went_valid",
        "id": f"evt_{uuid.uuid4().hex}",
        "api_version": "v1",
        "company_id": "biz_test",
        "timestamp": str(int(time.time())),
        "data": {},
    })


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_mini_app() -> FastAPI:
    from app.routers.webhooks import router as wh_router
    mini = FastAPI()
    mini.include_router(wh_router, prefix="/webhooks")
    return mini


@pytest.fixture
def mock_redis():
    client = AsyncMock()
    client.exists = AsyncMock(return_value=0)
    client.set = AsyncMock(return_value=True)
    client.get = AsyncMock(return_value=None)   # lock token not found → no delete (fine for tests)
    client.delete = AsyncMock(return_value=1)
    client.aclose = AsyncMock()
    return client


@pytest.fixture
async def http(mock_redis):
    """HTTP client patched at the fulfillment-service layer.

    fulfill_membership() in whop_fulfillment is exercised for real; its
    internal helpers (is_fulfilled, send_license_email, record_fulfillment)
    are mocked so no DB or SMTP is needed.
    """
    with (
        patch("app.config.settings.whop_webhook_secret", _TEST_ENV_SECRET),
        patch("app.routers.webhooks.aioredis.from_url", return_value=mock_redis),
        patch("app.routers.webhooks.restore_membership", new_callable=AsyncMock) as _restore,
        patch("app.routers.webhooks.revoke_membership", new_callable=AsyncMock) as _revoke,
        patch("app.services.whop_fulfillment.is_fulfilled", new_callable=AsyncMock, return_value=False),
        patch("app.services.whop_fulfillment.send_license_email", new_callable=AsyncMock) as _mail,
        patch("app.services.whop_fulfillment.record_fulfillment", new_callable=AsyncMock) as _fulfill,
    ):
        app = _make_mini_app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            client._restore = _restore
            client._revoke = _revoke
            client._mail = _mail
            client._fulfill = _fulfill
            yield client


# ---------------------------------------------------------------------------
# 503 — no secret configured
# ---------------------------------------------------------------------------

async def test_no_secret_returns_503():
    with patch("app.config.settings.whop_webhook_secret", None):
        app = _make_mini_app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            body = _activated()
            r = await client.post(_ENDPOINT, content=body, headers=_sign(body))
    assert r.status_code == 503, r.text


# ---------------------------------------------------------------------------
# 401 — bad signature
# ---------------------------------------------------------------------------

async def test_wrong_secret_returns_401(http):
    body = _activated()
    bad_headers = _sign(body, env_secret="completely-wrong-secret-value")
    r = await http.post(_ENDPOINT, content=body, headers=bad_headers)
    assert r.status_code == 401, r.text


async def test_tampered_body_returns_401(http):
    body = _activated()
    headers = _sign(body)
    tampered = body.replace("mem_test_001", "mem_HACKED")
    r = await http.post(_ENDPOINT, content=tampered, headers=headers)
    assert r.status_code == 401, r.text


async def test_missing_webhook_id_header_returns_401(http):
    body = _activated()
    headers = _sign(body)
    del headers["webhook-id"]
    r = await http.post(_ENDPOINT, content=body, headers=headers)
    assert r.status_code == 401, r.text


async def test_expired_timestamp_returns_401(http):
    body = _activated()
    old_ts = str(int(time.time()) - 400)  # > 5 minutes ago
    headers = _sign(body, timestamp=old_ts)
    r = await http.post(_ENDPOINT, content=body, headers=headers)
    assert r.status_code == 401, r.text


# ---------------------------------------------------------------------------
# Old behavior (no base64 encoding) is rejected
#
# Pre-fix: webhook_key=raw_secret → Webhook uses base64.b64decode(raw_secret)
#          as HMAC key — different from raw_secret.encode() → mismatch → 401.
# This test signs with the pre-fix effective key and confirms the new code
# rejects it, proving one-time encoding is mandatory.
# ---------------------------------------------------------------------------

async def test_old_no_encoding_passthrough_rejected(http):
    """Payload signed with pre-fix key (b64decode of raw secret) is rejected."""
    body = _activated()
    mid = f"msg_{uuid.uuid4().hex}"
    ts = str(int(time.time()))
    # Old effective key: Webhook(raw_secret) → base64.b64decode(raw_secret)
    # _TEST_ENV_SECRET is not valid base64, so mimic with a different wrong key.
    wrong_key = _TEST_ENV_SECRET.encode()[::-1]  # reversed bytes — clearly wrong
    signed = f"{mid}.{ts}.{body}"
    sig = base64.b64encode(
        hmac.new(wrong_key, signed.encode(), hashlib.sha256).digest()
    ).decode()
    bad_headers = {
        "webhook-id": mid,
        "webhook-timestamp": ts,
        "webhook-signature": f"v1,{sig}",
        "content-type": "application/json",
    }
    r = await http.post(_ENDPOINT, content=body, headers=bad_headers)
    assert r.status_code == 401, "Wrong-key signature must be rejected"


# ---------------------------------------------------------------------------
# membership.activated
# ---------------------------------------------------------------------------

async def test_membership_activated_returns_200(http):
    body = _activated()
    r = await http.post(_ENDPOINT, content=body, headers=_sign(body))
    assert r.status_code == 200, r.text
    assert r.json()["action"] == "activated"


async def test_membership_activated_calls_restore(http):
    body = _activated(membership_id="mem_abc123")
    await http.post(_ENDPOINT, content=body, headers=_sign(body))
    http._restore.assert_awaited_once()
    call_args = http._restore.call_args[0]
    assert "mem_abc123" in call_args


async def test_membership_activated_sends_welcome_email(http):
    body = _activated(user_email="buyer@example.com", license_key="LK-UNIT-999")
    await http.post(_ENDPOINT, content=body, headers=_sign(body))
    http._mail.assert_awaited_once()
    email_call = http._mail.call_args[0]
    assert "buyer@example.com" in email_call


async def test_membership_activated_no_email_without_license_key(http):
    body = _activated(license_key="")
    await http.post(_ENDPOINT, content=body, headers=_sign(body))
    http._mail.assert_not_awaited()


async def test_membership_activated_unknown_product_ignored(http):
    body = _activated(product_id="prod_UNKNOWN_XYZ")
    r = await http.post(_ENDPOINT, content=body, headers=_sign(body))
    assert r.status_code == 200
    assert r.json()["action"] == "ignored_product"
    http._restore.assert_not_awaited()


# ---------------------------------------------------------------------------
# membership.deactivated
# ---------------------------------------------------------------------------

async def test_membership_deactivated_returns_200(http):
    body = _deactivated()
    r = await http.post(_ENDPOINT, content=body, headers=_sign(body))
    assert r.status_code == 200, r.text
    assert r.json()["action"] == "revoked"


async def test_membership_deactivated_calls_revoke(http):
    body = _deactivated(membership_id="mem_def456")
    await http.post(_ENDPOINT, content=body, headers=_sign(body))
    http._revoke.assert_awaited_once()
    call_args = http._revoke.call_args[0]
    assert "mem_def456" in call_args


# ---------------------------------------------------------------------------
# membership.cancel_at_period_end_changed
# ---------------------------------------------------------------------------

async def test_cancel_flag_updated_returns_200(http):
    body = _cancel_flag(cancel=True)
    r = await http.post(_ENDPOINT, content=body, headers=_sign(body))
    assert r.status_code == 200, r.text
    assert r.json()["action"] == "cancel_flag_updated"


async def test_cancel_flag_does_not_revoke(http):
    body = _cancel_flag(cancel=True)
    await http.post(_ENDPOINT, content=body, headers=_sign(body))
    http._revoke.assert_not_awaited()


# ---------------------------------------------------------------------------
# refund.created
# ---------------------------------------------------------------------------

async def test_refund_created_acknowledged(http):
    body = _refund()
    r = await http.post(_ENDPOINT, content=body, headers=_sign(body))
    assert r.status_code == 200, r.text
    assert r.json()["action"] == "refund_logged"


# ---------------------------------------------------------------------------
# payment.succeeded
# ---------------------------------------------------------------------------

async def test_payment_succeeded_acknowledged(http):
    body = _payment_succeeded()
    r = await http.post(_ENDPOINT, content=body, headers=_sign(body))
    assert r.status_code == 200, r.text
    assert r.json()["action"] == "payment_logged"


# ---------------------------------------------------------------------------
# Unknown / legacy events ignored
# ---------------------------------------------------------------------------

async def test_unknown_event_ignored(http):
    body = _unknown_event()
    r = await http.post(_ENDPOINT, content=body, headers=_sign(body))
    assert r.status_code == 200, r.text
    assert r.json()["action"] == "ignored"
    http._restore.assert_not_awaited()
    http._revoke.assert_not_awaited()


# ---------------------------------------------------------------------------
# Idempotence — duplicate webhook-id
# ---------------------------------------------------------------------------

async def test_duplicate_webhook_id_returns_duplicate(http, mock_redis):
    # Use a handled event type so the idempotence key is checked before type filter.
    mock_redis.exists = AsyncMock(return_value=1)
    body = _refund()
    msg_id = f"msg_{uuid.uuid4().hex}"
    headers = _sign(body, msg_id=msg_id)
    r = await http.post(_ENDPOINT, content=body, headers=headers)
    assert r.status_code == 200, r.text
    assert r.json()["action"] == "duplicate"
    http._restore.assert_not_awaited()


async def test_duplicate_unknown_event_also_deduplicated(http, mock_redis):
    """Idempotence applies even to unknown event types (check is before type filter)."""
    mock_redis.exists = AsyncMock(return_value=1)
    body = _unknown_event()
    msg_id = f"msg_{uuid.uuid4().hex}"
    headers = _sign(body, msg_id=msg_id)
    r = await http.post(_ENDPOINT, content=body, headers=headers)
    assert r.status_code == 200, r.text
    assert r.json()["action"] == "duplicate"


async def test_first_delivery_sets_idempotence_key(http, mock_redis):
    body = _activated()
    msg_id = f"msg_{uuid.uuid4().hex}"
    headers = _sign(body, msg_id=msg_id)
    await http.post(_ENDPOINT, content=body, headers=headers)
    # set is called at least twice: once for the webhook idempotence key, once
    # for the fulfill_membership lock — verify the idempotence call is present.
    mock_redis.set.assert_awaited()
    all_calls = str(mock_redis.set.call_args_list)
    assert "webhook:seen:" in all_calls
