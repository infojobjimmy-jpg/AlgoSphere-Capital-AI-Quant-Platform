"""
Tests for revocation enforcement across all protected endpoints.

Verifies that a valid cookie belonging to a revoked membership is rejected by:
  - GET /auth/status  → authenticated=false, Set-Cookie deletes session
  - GET /auth/me      → 403
  - GET /account/preferences → 403
  - POST /account/preferences → 403

Owner cookies are never subject to the revocation check.
Redis unavailability for non-owner members → 503.

Run:
    cd backend && python -m pytest tests/test_auth_revocation.py -v
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

_SECRET = "test-auth-session-secret-value"
_MEMBERSHIP_ID = "mem_test_rev_001"
_REVOKED_KEY = "acap:members:revoked"


def _make_token(payload: dict, secret: str = _SECRET) -> str:
    body = dict(payload)
    body.setdefault("exp", int(time.time()) + 3600)
    body.setdefault("validated_at", int(time.time()))

    def b64(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).decode().rstrip("=")

    encoded = b64(json.dumps(body, separators=(",", ":"), sort_keys=True).encode())
    sig = b64(hmac.new(secret.encode(), encoded.encode(), hashlib.sha256).digest())
    return f"{encoded}.{sig}"


def _member_token(membership_id: str = _MEMBERSHIP_ID) -> str:
    return _make_token({
        "membership_id": membership_id,
        "product_id": "prod_test",
        "plan": "business",
        "status": "active",
        "user_id": "usr_test",
        "username": "test@example.com",
        "_lk": "LK-TEST-001",
        "remember_me": True,
    })


def _owner_token() -> str:
    return _make_token({
        "membership_id": "owner",
        "product_id": "algosphere-owner",
        "plan": "owner",
        "status": "active",
        "user_id": "algosphere-owner",
        "username": "owner@example.com",
        "role": "owner",
    })


def _make_app() -> FastAPI:
    from app.routers.auth import router as auth_router
    from app.routers.account import router as acc_router
    mini = FastAPI()
    mini.include_router(auth_router, prefix="/auth")
    mini.include_router(acc_router, prefix="/account")
    return mini


@pytest.fixture
def mock_redis_clean():
    """Redis that reports the membership as NOT revoked."""
    client = AsyncMock()
    client.sismember = AsyncMock(return_value=0)
    client.get = AsyncMock(return_value=None)
    client.set = AsyncMock(return_value=True)
    client.sadd = AsyncMock(return_value=1)
    client.srem = AsyncMock(return_value=1)
    client.aclose = AsyncMock()
    return client


@pytest.fixture
def mock_redis_revoked():
    """Redis that reports the membership as revoked."""
    client = AsyncMock()
    client.sismember = AsyncMock(return_value=1)
    client.get = AsyncMock(return_value=None)
    client.set = AsyncMock(return_value=True)
    client.aclose = AsyncMock()
    return client


@pytest.fixture
def mock_redis_down():
    """Redis that raises an exception (simulates unavailability)."""
    client = AsyncMock()
    client.sismember = AsyncMock(side_effect=ConnectionError("Redis down"))
    client.aclose = AsyncMock()
    return client


# ─────────────────────────────────────────────
# /auth/status
# ─────────────────────────────────────────────

async def test_status_active_membership_authenticated(mock_redis_clean):
    with (
        patch("app.config.settings.auth_session_secret", _SECRET),
        patch("app.config.settings.whop_api_key", "key_test"),
        patch("app.routers.auth.redis.from_url", return_value=mock_redis_clean),
    ):
        app = _make_app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            token = _member_token()
            r = await client.get("/auth/status", cookies={"algosphere_session": token})
    assert r.status_code == 200
    assert r.json()["authenticated"] is True


async def test_status_revoked_membership_returns_not_authenticated(mock_redis_revoked):
    with (
        patch("app.config.settings.auth_session_secret", _SECRET),
        patch("app.config.settings.whop_api_key", "key_test"),
        patch("app.routers.auth.redis.from_url", return_value=mock_redis_revoked),
    ):
        app = _make_app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            token = _member_token()
            r = await client.get("/auth/status", cookies={"algosphere_session": token})
    assert r.status_code == 200
    assert r.json()["authenticated"] is False
    assert r.json()["member"] is None


async def test_status_revoked_membership_deletes_cookie(mock_redis_revoked):
    with (
        patch("app.config.settings.auth_session_secret", _SECRET),
        patch("app.config.settings.whop_api_key", "key_test"),
        patch("app.routers.auth.redis.from_url", return_value=mock_redis_revoked),
    ):
        app = _make_app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            token = _member_token()
            r = await client.get("/auth/status", cookies={"algosphere_session": token})
    set_cookie = r.headers.get("set-cookie", "")
    assert "algosphere_session" in set_cookie
    assert "max-age=0" in set_cookie.lower() or "expires=" in set_cookie.lower()


async def test_status_redis_down_returns_503(mock_redis_down):
    with (
        patch("app.config.settings.auth_session_secret", _SECRET),
        patch("app.config.settings.whop_api_key", "key_test"),
        patch("app.routers.auth.redis.from_url", return_value=mock_redis_down),
    ):
        app = _make_app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            token = _member_token()
            r = await client.get("/auth/status", cookies={"algosphere_session": token})
    assert r.status_code == 503


async def test_status_owner_bypasses_revocation_check():
    """Owner cookies must never hit Redis revocation check."""
    redis_mock = AsyncMock()
    redis_mock.sismember = AsyncMock(side_effect=AssertionError("should not be called for owner"))
    redis_mock.aclose = AsyncMock()
    with (
        patch("app.config.settings.auth_session_secret", _SECRET),
        patch("app.config.settings.whop_api_key", "key_test"),
        patch("app.config.settings.owner_access_code", "owner-code-test"),
        patch("app.routers.auth.redis.from_url", return_value=redis_mock),
    ):
        app = _make_app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            token = _owner_token()
            r = await client.get("/auth/status", cookies={"algosphere_session": token})
    assert r.status_code == 200
    assert r.json()["authenticated"] is True
    assert r.json()["member"]["role"] == "owner"


async def test_status_no_cookie_returns_not_authenticated():
    with patch("app.config.settings.auth_session_secret", _SECRET):
        app = _make_app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            r = await client.get("/auth/status")
    assert r.status_code == 200
    assert r.json()["authenticated"] is False


# ─────────────────────────────────────────────
# /auth/me
# ─────────────────────────────────────────────

async def test_me_active_membership_returns_200(mock_redis_clean):
    with (
        patch("app.config.settings.auth_session_secret", _SECRET),
        patch("app.config.settings.whop_api_key", "key_test"),
        patch("app.routers.auth.redis.from_url", return_value=mock_redis_clean),
    ):
        app = _make_app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            token = _member_token()
            r = await client.get("/auth/me", cookies={"algosphere_session": token})
    assert r.status_code == 200
    assert r.json()["authenticated"] is True


async def test_me_revoked_membership_returns_403(mock_redis_revoked):
    with (
        patch("app.config.settings.auth_session_secret", _SECRET),
        patch("app.config.settings.whop_api_key", "key_test"),
        patch("app.routers.auth.redis.from_url", return_value=mock_redis_revoked),
    ):
        app = _make_app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            token = _member_token()
            r = await client.get("/auth/me", cookies={"algosphere_session": token})
    assert r.status_code == 403


async def test_me_revoked_deletes_cookie(mock_redis_revoked):
    with (
        patch("app.config.settings.auth_session_secret", _SECRET),
        patch("app.config.settings.whop_api_key", "key_test"),
        patch("app.routers.auth.redis.from_url", return_value=mock_redis_revoked),
    ):
        app = _make_app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            token = _member_token()
            r = await client.get("/auth/me", cookies={"algosphere_session": token})
    set_cookie = r.headers.get("set-cookie", "")
    assert "algosphere_session" in set_cookie


async def test_me_no_cookie_returns_401():
    with patch("app.config.settings.auth_session_secret", _SECRET):
        app = _make_app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            r = await client.get("/auth/me")
    assert r.status_code == 401


async def test_me_redis_down_returns_503(mock_redis_down):
    with (
        patch("app.config.settings.auth_session_secret", _SECRET),
        patch("app.config.settings.whop_api_key", "key_test"),
        patch("app.routers.auth.redis.from_url", return_value=mock_redis_down),
    ):
        app = _make_app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            token = _member_token()
            r = await client.get("/auth/me", cookies={"algosphere_session": token})
    assert r.status_code == 503


async def test_me_owner_bypasses_revocation():
    redis_mock = AsyncMock()
    redis_mock.sismember = AsyncMock(side_effect=AssertionError("should not be called"))
    redis_mock.aclose = AsyncMock()
    with (
        patch("app.config.settings.auth_session_secret", _SECRET),
        patch("app.config.settings.whop_api_key", "key_test"),
        patch("app.config.settings.owner_access_code", "owner-code-test"),
        patch("app.routers.auth.redis.from_url", return_value=redis_mock),
    ):
        app = _make_app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            token = _owner_token()
            r = await client.get("/auth/me", cookies={"algosphere_session": token})
    assert r.status_code == 200


# ─────────────────────────────────────────────
# /account/preferences
# ─────────────────────────────────────────────

@pytest.fixture
def mock_redis_prefs(mock_redis_clean):
    """Redis mock that also supports preferences storage."""
    mock_redis_clean.get = AsyncMock(return_value=None)
    mock_redis_clean.set = AsyncMock(return_value=True)
    mock_redis_clean.expire = AsyncMock(return_value=1)
    mock_redis_clean.ttl = AsyncMock(return_value=-1)
    return mock_redis_clean


async def test_preferences_get_active_member_returns_200(mock_redis_prefs):
    with (
        patch("app.config.settings.auth_session_secret", _SECRET),
        patch("app.config.settings.whop_api_key", "key_test"),
        patch("app.services.whop_auth.aioredis.from_url", return_value=mock_redis_prefs),
        patch("app.routers.account.redis.from_url", return_value=mock_redis_prefs),
    ):
        app = _make_app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            token = _member_token()
            r = await client.get("/account/preferences", cookies={"algosphere_session": token})
    assert r.status_code == 200
    assert "favorites" in r.json()


async def test_preferences_get_revoked_member_returns_403():
    redis_mock = AsyncMock()
    redis_mock.sismember = AsyncMock(return_value=1)
    redis_mock.get = AsyncMock(return_value=None)
    redis_mock.aclose = AsyncMock()
    with (
        patch("app.config.settings.auth_session_secret", _SECRET),
        patch("app.config.settings.whop_api_key", "key_test"),
        patch("app.services.whop_auth.aioredis.from_url", return_value=redis_mock),
        patch("app.routers.account.redis.from_url", return_value=redis_mock),
    ):
        app = _make_app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            token = _member_token()
            r = await client.get("/account/preferences", cookies={"algosphere_session": token})
    assert r.status_code == 403


async def test_preferences_get_no_cookie_returns_401():
    with patch("app.config.settings.auth_session_secret", _SECRET):
        app = _make_app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            r = await client.get("/account/preferences")
    assert r.status_code == 401


async def test_preferences_post_revoked_member_returns_403():
    redis_mock = AsyncMock()
    redis_mock.sismember = AsyncMock(return_value=1)
    redis_mock.get = AsyncMock(return_value=None)
    redis_mock.aclose = AsyncMock()
    with (
        patch("app.config.settings.auth_session_secret", _SECRET),
        patch("app.config.settings.whop_api_key", "key_test"),
        patch("app.services.whop_auth.aioredis.from_url", return_value=redis_mock),
        patch("app.routers.account.redis.from_url", return_value=redis_mock),
    ):
        app = _make_app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            token = _member_token()
            r = await client.post(
                "/account/preferences",
                json={"favorites": [], "alerts": []},
                cookies={"algosphere_session": token},
            )
    assert r.status_code == 403


async def test_preferences_post_no_cookie_returns_401():
    with patch("app.config.settings.auth_session_secret", _SECRET):
        app = _make_app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            r = await client.post(
                "/account/preferences",
                json={"favorites": [], "alerts": []},
            )
    assert r.status_code == 401


# ─────────────────────────────────────────────
# Active member access works end-to-end
# ─────────────────────────────────────────────

async def test_full_active_member_flow(mock_redis_prefs):
    """Active member: status authenticated, me 200, preferences 200."""
    with (
        patch("app.config.settings.auth_session_secret", _SECRET),
        patch("app.config.settings.whop_api_key", "key_test"),
        patch("app.routers.auth.redis.from_url", return_value=mock_redis_prefs),
        patch("app.services.whop_auth.aioredis.from_url", return_value=mock_redis_prefs),
        patch("app.routers.account.redis.from_url", return_value=mock_redis_prefs),
    ):
        app = _make_app()
        token = _member_token()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            r_status = await client.get("/auth/status", cookies={"algosphere_session": token})
            r_me = await client.get("/auth/me", cookies={"algosphere_session": token})
            r_prefs = await client.get("/account/preferences", cookies={"algosphere_session": token})
    assert r_status.json()["authenticated"] is True
    assert r_me.status_code == 200
    assert r_prefs.status_code == 200


async def test_full_revoked_member_flow():
    """Revoked member: status unauthenticated, me 403, preferences 403."""
    redis_mock = AsyncMock()
    redis_mock.sismember = AsyncMock(return_value=1)
    redis_mock.get = AsyncMock(return_value=None)
    redis_mock.aclose = AsyncMock()
    with (
        patch("app.config.settings.auth_session_secret", _SECRET),
        patch("app.config.settings.whop_api_key", "key_test"),
        patch("app.routers.auth.redis.from_url", return_value=redis_mock),
        patch("app.services.whop_auth.aioredis.from_url", return_value=redis_mock),
        patch("app.routers.account.redis.from_url", return_value=redis_mock),
    ):
        app = _make_app()
        token = _member_token()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            r_status = await client.get("/auth/status", cookies={"algosphere_session": token})
            r_me = await client.get("/auth/me", cookies={"algosphere_session": token})
            r_prefs = await client.get("/account/preferences", cookies={"algosphere_session": token})
    assert r_status.json()["authenticated"] is False
    assert r_me.status_code == 403
    assert r_prefs.status_code == 403
