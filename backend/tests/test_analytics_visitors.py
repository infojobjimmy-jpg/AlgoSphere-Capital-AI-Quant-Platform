"""
Integration tests for the visitor presence system.

Requirements:
  - PostgreSQL reachable (uses app settings, e.g. DATABASE_URL from environment)
  - Redis reachable (uses app settings)
  - AUTH_SESSION_SECRET set

Run:
    pytest backend/tests/test_analytics_visitors.py -v

Skip if services unavailable:
    pytest backend/tests/test_analytics_visitors.py -v -m "not integration"
"""
from __future__ import annotations

import json
import uuid

import pytest
import pytest_asyncio
import redis.asyncio as aioredis
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text

# Tests are auto-detected as async via pytest.ini asyncio_mode = auto.


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_vsid() -> str:
    return uuid.uuid4().hex


def _owner_cookie() -> dict[str, str]:
    try:
        from app.services.whop_auth import create_session
        return {"algosphere_session": create_session({"role": "owner"}, hours=1)}
    except Exception as exc:
        pytest.skip(f"Cannot create owner session: {exc}")


def _member_cookie(plan: str = "explorer") -> dict[str, str]:
    try:
        from app.services.whop_auth import create_session
        return {
            "algosphere_session": create_session(
                {
                    "role": "member",
                    "membership_id": f"test_mem_{uuid.uuid4().hex[:8]}",
                    "username": "test_user",
                    "product_id": "prod_test_integration",
                    "plan": plan,
                    "status": "active",
                },
                hours=1,
            )
        }
    except Exception as exc:
        pytest.skip(f"Cannot create member session: {exc}")


async def _delete_vsid(db_session, vsid: str) -> None:
    await db_session.execute(
        text("DELETE FROM visitor_sessions WHERE visitor_session_id = :vsid"),
        {"vsid": vsid},
    )
    await db_session.commit()


async def _get_vs_row(db_session, vsid: str):
    return (
        await db_session.execute(
            text("SELECT * FROM visitor_sessions WHERE visitor_session_id = :vsid"),
            {"vsid": vsid},
        )
    ).mappings().first()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture(scope="module")
async def http():
    """HTTP client wired to the FastAPI app with full lifespan."""
    try:
        from app.main import app
    except Exception as exc:
        pytest.skip(f"Cannot import app: {exc}")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client


@pytest_asyncio.fixture(scope="module")
async def redis_cl():
    try:
        from app.config import settings
        client = aioredis.from_url(settings.redis_url, decode_responses=True)
        yield client
        await client.aclose()
    except Exception as exc:
        pytest.skip(f"Redis unavailable: {exc}")


@pytest_asyncio.fixture(scope="module")
async def db():
    try:
        from app.db.session import SessionLocal
        async with SessionLocal() as session:
            yield session
    except Exception as exc:
        pytest.skip(f"DB unavailable: {exc}")


# ---------------------------------------------------------------------------
# DDL sync test (no services required)
# ---------------------------------------------------------------------------

def test_ddl_sync_between_sql_file_and_analytics_py():
    """
    The canonical SQL file and the inline DDL in analytics.py must define
    the same columns. This test reads both and checks that all column names
    from the SQL file are present in the Python string.
    """
    from pathlib import Path
    from app.routers.analytics import _VISITOR_SESSIONS_DDL

    sql_file = Path(__file__).parents[2] / "infra" / "sql" / "005_visitor_sessions.sql"
    assert sql_file.exists(), f"SQL file not found: {sql_file}"
    sql_text = sql_file.read_text(encoding="utf-8")

    # Extract column names from the CREATE TABLE block of the SQL file.
    import re
    col_matches = re.findall(r"^\s{4}(\w+)\s", sql_text, re.MULTILINE)
    # Filter out known non-column tokens
    skip = {"id", "CREATE", "ALTER", "UNIQUE", "INDEX"}
    columns = [c for c in col_matches if c not in skip and not c.startswith("idx")]

    for col in columns:
        assert col in _VISITOR_SESSIONS_DDL, (
            f"Column '{col}' present in 005_visitor_sessions.sql but missing from "
            f"analytics.py _VISITOR_SESSIONS_DDL. Keep both files in sync."
        )


# ---------------------------------------------------------------------------
# Consent + Redis + DB tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_accepted_consent_anonymous_creates_full_record(http, db, redis_cl):
    """Accepted consent + anonymous: full Redis presence + full DB record."""
    from app.config import settings
    vsid = _make_vsid()
    try:
        r = await http.post("/analytics/heartbeat", json={
            "visitor_session_id": vsid,
            "page": "/",
            "device_type": "desktop",
            "referrer_domain": "google.com",
            "utm_source": "google",
            "utm_campaign": "spring",
            "consent": "accepted",
        })
        assert r.status_code == 200
        data = r.json()
        assert data["redis"] is True
        assert data["db"] is True

        # Redis: full presence including marketing fields
        presence_raw = await redis_cl.get(f"{settings.app_slug}:presence:{vsid}")
        assert presence_raw is not None
        p = json.loads(presence_raw)
        assert p["device_type"] == "desktop"
        assert p["utm_source"] == "google"

        # DB: full row including marketing fields
        row = await _get_vs_row(db, vsid)
        assert row is not None
        assert row["page_views"] == 1
        assert row["heartbeat_count"] == 1
        assert row["utm_source"] == "google"
        assert row["referrer_domain"] == "google.com"
        assert row["device_type"] == "desktop"
        assert row["consent"] == "accepted"
    finally:
        await _delete_vsid(db, vsid)


@pytest.mark.asyncio
async def test_declined_consent_anonymous_no_db_record(http, db, redis_cl):
    """Declined consent + anonymous: Redis minimal presence only, NO DB row."""
    from app.config import settings
    vsid = _make_vsid()
    try:
        r = await http.post("/analytics/heartbeat", json={
            "visitor_session_id": vsid,
            "page": "/test",
            "device_type": "mobile",
            "referrer_domain": "facebook.com",
            "utm_source": "fb",
            "utm_campaign": "summer",
            "consent": "declined",
        })
        assert r.status_code == 200
        data = r.json()
        assert data["redis"] is True
        assert data["db"] is None  # None = skipped (not an error)

        # Redis: marketing fields stripped
        presence_raw = await redis_cl.get(f"{settings.app_slug}:presence:{vsid}")
        assert presence_raw is not None
        p = json.loads(presence_raw)
        assert p["device_type"] == "unknown"
        assert p["utm_source"] == ""

        # DB: absolutely no row
        row = await _get_vs_row(db, vsid)
        assert row is None, "Anonymous declined-consent visitor must not have a DB record"
    finally:
        await _delete_vsid(db, vsid)
        from app.config import settings as s
        await redis_cl.delete(f"{s.app_slug}:presence:{vsid}")


@pytest.mark.asyncio
async def test_redis_ttl_set_to_90s(http, redis_cl):
    """Heartbeat sets Redis key with TTL ≤ 90 seconds."""
    from app.config import settings
    vsid = _make_vsid()
    await http.post("/analytics/heartbeat", json={
        "visitor_session_id": vsid,
        "page": "/",
        "consent": "declined",
    })
    key = f"{settings.app_slug}:presence:{vsid}"
    ttl = await redis_cl.ttl(key)
    assert 0 < ttl <= 90, f"Expected TTL in (0, 90], got {ttl}"
    await redis_cl.delete(key)


# ---------------------------------------------------------------------------
# page_views logic
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_10_heartbeats_same_page_gives_1_page_view(http, db):
    """10 heartbeats on the same page → page_views=1, heartbeat_count=10."""
    vsid = _make_vsid()
    try:
        for _ in range(10):
            r = await http.post("/analytics/heartbeat", json={
                "visitor_session_id": vsid,
                "page": "/dashboard",
                "consent": "accepted",
            })
            assert r.status_code == 200

        row = await _get_vs_row(db, vsid)
        assert row is not None
        assert row["page_views"] == 1, f"Expected 1 page view, got {row['page_views']}"
        assert row["heartbeat_count"] == 10, f"Expected 10 heartbeats, got {row['heartbeat_count']}"
    finally:
        await _delete_vsid(db, vsid)


@pytest.mark.asyncio
async def test_3_distinct_pages_gives_3_page_views(http, db):
    """Heartbeat on 3 different pages → page_views=3."""
    vsid = _make_vsid()
    try:
        for page in ["/", "/map", "/about"]:
            r = await http.post("/analytics/heartbeat", json={
                "visitor_session_id": vsid,
                "page": page,
                "consent": "accepted",
            })
            assert r.status_code == 200

        row = await _get_vs_row(db, vsid)
        assert row is not None
        assert row["page_views"] == 3, f"Expected 3 page views, got {row['page_views']}"
    finally:
        await _delete_vsid(db, vsid)


@pytest.mark.asyncio
async def test_same_page_repeated_then_new_page(http, db):
    """5 heartbeats on / then 1 on /new → page_views=2, heartbeat_count=6."""
    vsid = _make_vsid()
    try:
        for _ in range(5):
            await http.post("/analytics/heartbeat", json={
                "visitor_session_id": vsid, "page": "/", "consent": "accepted",
            })
        await http.post("/analytics/heartbeat", json={
            "visitor_session_id": vsid, "page": "/new", "consent": "accepted",
        })

        row = await _get_vs_row(db, vsid)
        assert row is not None
        assert row["page_views"] == 2
        assert row["heartbeat_count"] == 6
    finally:
        await _delete_vsid(db, vsid)


# ---------------------------------------------------------------------------
# Authenticated member
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_member_stored_with_plan(http, db):
    """Member heartbeat stored with correct plan and authenticated=True."""
    vsid = _make_vsid()
    cookies = _member_cookie(plan="pro")
    try:
        r = await http.post("/analytics/heartbeat", json={
            "visitor_session_id": vsid,
            "page": "/dashboard",
            "consent": "accepted",
        }, cookies=cookies)
        assert r.status_code == 200
        assert r.json()["db"] is True

        row = await _get_vs_row(db, vsid)
        assert row is not None
        assert row["authenticated"] is True
        assert row["plan"] == "pro"
        assert row["login_completed"] is True
    finally:
        await _delete_vsid(db, vsid)


@pytest.mark.asyncio
async def test_member_declined_consent_stored_operationally_no_marketing(http, db):
    """Member with declined consent: DB row created, but no UTM/device/referrer."""
    vsid = _make_vsid()
    cookies = _member_cookie()
    try:
        r = await http.post("/analytics/heartbeat", json={
            "visitor_session_id": vsid,
            "page": "/",
            "device_type": "mobile",
            "utm_source": "google",
            "referrer_domain": "google.com",
            "consent": "declined",
        }, cookies=cookies)
        assert r.status_code == 200
        assert r.json()["db"] is True  # Always stored for authenticated members

        row = await _get_vs_row(db, vsid)
        assert row is not None
        assert row["authenticated"] is True
        assert row["utm_source"] is None, "UTM must not be stored without consent"
        assert row["device_type"] is None, "Device must not be stored without consent"
        assert row["referrer_domain"] is None, "Referrer must not be stored without consent"
    finally:
        await _delete_vsid(db, vsid)


# ---------------------------------------------------------------------------
# Authorization
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_live_visitors_forbidden_for_anonymous(http):
    """Unauthenticated request to /analytics/live-visitors → 403."""
    r = await http.get("/analytics/live-visitors")
    assert r.status_code == 403


@pytest.mark.asyncio
async def test_live_visitors_forbidden_for_member(http):
    """Member request to /analytics/live-visitors → 403."""
    r = await http.get("/analytics/live-visitors", cookies=_member_cookie())
    assert r.status_code == 403


@pytest.mark.asyncio
async def test_visitor_history_forbidden_for_member(http):
    """Member request to /analytics/visitor-history → 403."""
    r = await http.get("/analytics/visitor-history", cookies=_member_cookie())
    assert r.status_code == 403


@pytest.mark.asyncio
async def test_live_visitors_allowed_for_owner(http):
    """Owner can access /analytics/live-visitors."""
    r = await http.get("/analytics/live-visitors", cookies=_owner_cookie())
    assert r.status_code == 200
    data = r.json()
    assert "total_online" in data
    assert "visitors" in data
    assert "members_online" in data


@pytest.mark.asyncio
async def test_visitor_history_allowed_for_owner(http):
    """Owner can access /analytics/visitor-history."""
    r = await http.get("/analytics/visitor-history", cookies=_owner_cookie())
    assert r.status_code == 200
    data = r.json()
    assert "total" in data
    assert "sessions" in data


# ---------------------------------------------------------------------------
# No secrets in responses
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_live_visitors_vsid_truncated(http, db):
    """visitor_session_id in live-visitors response is truncated, never full."""
    vsid = _make_vsid()
    try:
        await http.post("/analytics/heartbeat", json={
            "visitor_session_id": vsid, "page": "/", "consent": "accepted",
        })
        r = await http.get("/analytics/live-visitors", cookies=_owner_cookie())
        assert r.status_code == 200
        for visitor in r.json()["visitors"]:
            vsid_shown = visitor.get("visitor_session_id", "")
            assert len(vsid_shown) <= 17, f"visitor_session_id not truncated: {vsid_shown!r}"
            assert vsid not in vsid_shown, "Full visitor_session_id must not appear in response"
    finally:
        await _delete_vsid(db, vsid)


@pytest.mark.asyncio
async def test_no_sensitive_fields_in_live_visitors(http):
    """live-visitors response contains no secret or PII field names."""
    r = await http.get("/analytics/live-visitors", cookies=_owner_cookie())
    assert r.status_code == 200
    body = r.text.lower()
    for forbidden in ["_lk", "otp", "smtp", "password", "webhook_secret", "api_key", "whop_api"]:
        assert forbidden not in body, f"Sensitive field '{forbidden}' found in live-visitors response"


@pytest.mark.asyncio
async def test_no_sensitive_fields_in_visitor_history(http):
    """visitor-history response contains no secret or PII field names."""
    r = await http.get("/analytics/visitor-history", cookies=_owner_cookie())
    assert r.status_code == 200
    body = r.text.lower()
    for forbidden in ["_lk", "otp", "smtp", "password", "webhook_secret", "api_key", "whop_api"]:
        assert forbidden not in body, f"Sensitive field '{forbidden}' found in visitor-history response"


# ---------------------------------------------------------------------------
# CSV export
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_csv_export_returns_correct_content_type(http, db):
    """Owner CSV export returns text/csv with attachment header."""
    vsid = _make_vsid()
    try:
        await http.post("/analytics/heartbeat", json={
            "visitor_session_id": vsid, "page": "/", "consent": "accepted",
        })
        r = await http.get("/analytics/visitor-history?format=csv", cookies=_owner_cookie())
        assert r.status_code == 200
        assert "text/csv" in r.headers.get("content-type", ""), "Expected text/csv content type"
        assert "attachment" in r.headers.get("content-disposition", "")
    finally:
        await _delete_vsid(db, vsid)


@pytest.mark.asyncio
async def test_csv_export_forbidden_for_member(http):
    """Member cannot export CSV."""
    r = await http.get("/analytics/visitor-history?format=csv", cookies=_member_cookie())
    assert r.status_code == 403
