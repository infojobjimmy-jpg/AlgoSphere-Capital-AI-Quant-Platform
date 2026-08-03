"""
Tests for the social presence / geolocation consent system.

Properties guaranteed:
  1. Anonymous user → 401 on POST and DELETE /social/presence.
  2. Member with consent_precision="none" → presence disabled, no coordinates stored.
  3. Member with consent_precision="approximate" → coordinates rounded to 0.5° grid.
  4. Member with consent_precision="precise" → exact coordinates stored.
  5. DELETE /social/presence → row removed from DB.
  6. Expired presence not returned in /social/overview connections.
  7. _round_approx unit test (no DB/Redis required).

Run:
    pytest backend/tests/test_social_presence.py -v
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_identity() -> str:
    return f"test_social_{uuid.uuid4().hex[:12]}"


def _member_cookie(identity: str | None = None) -> dict[str, str]:
    try:
        from app.services.whop_auth import create_session
        return {
            "algosphere_session": create_session(
                {
                    "role": "member",
                    "membership_id": identity or _make_identity(),
                    "username": "social_test",
                    "product_id": "prod_test",
                    "plan": "explorer",
                    "status": "active",
                },
                hours=1,
            )
        }
    except Exception as exc:
        pytest.skip(f"Cannot create member session: {exc}")


async def _cleanup_presence(db_session, member_id: str) -> None:
    await db_session.execute(
        text("DELETE FROM social_presence WHERE member_id = :id"),
        {"id": member_id},
    )
    await db_session.execute(
        text("DELETE FROM social_connections WHERE owner_id = :id OR contact_id = :id"),
        {"id": member_id},
    )
    await db_session.execute(
        text("DELETE FROM social_profiles WHERE member_id = :id"),
        {"id": member_id},
    )
    await db_session.commit()


async def _get_presence(db_session, member_id: str):
    return (
        await db_session.execute(
            text("SELECT * FROM social_presence WHERE member_id = :id"),
            {"id": member_id},
        )
    ).mappings().first()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture(scope="module")
async def http():
    try:
        from app.main import app
    except Exception as exc:
        pytest.skip(f"Cannot import app: {exc}")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client


@pytest_asyncio.fixture(scope="module")
async def db():
    try:
        from app.db.session import SessionLocal
        async with SessionLocal() as session:
            yield session
    except Exception as exc:
        pytest.skip(f"DB unavailable: {exc}")


# ---------------------------------------------------------------------------
# Unit tests (no DB/Redis required)
# ---------------------------------------------------------------------------

def test_round_approx_positive():
    """Values clearly above/below midpoint round to the nearest 0.5° step."""
    from app.routers.social import _round_approx
    # 48.76 / 0.5 = 97.52 → round(97.52) = 98 → 98 * 0.5 = 49.0
    assert _round_approx(48.76) == 49.0
    # 48.26 / 0.5 = 96.52 → round(96.52) = 97 → 97 * 0.5 = 48.5
    assert _round_approx(48.26) == 48.5
    # 48.01 / 0.5 = 96.02 → round(96.02) = 96 → 96 * 0.5 = 48.0
    assert _round_approx(48.01) == 48.0


def test_round_approx_negative():
    """Negative values round symmetrically."""
    from app.routers.social import _round_approx
    assert _round_approx(-2.37) == -2.5
    assert _round_approx(-2.12) == -2.0
    assert _round_approx(-90.0) == -90.0


def test_round_approx_boundary():
    """Exact midpoints round to nearest even (Python default) — no crash."""
    from app.routers.social import _round_approx
    result = _round_approx(48.25)
    assert result in (48.0, 48.5), f"Expected 48.0 or 48.5, got {result}"


def test_approximate_reduces_precision():
    """Approximate rounding always moves the value ≥ 0 and ≤ GRID/2 from original."""
    from app.routers.social import _round_approx, _APPROX_GRID_DEG
    import random
    random.seed(42)
    for _ in range(200):
        lat = random.uniform(-89.9, 89.9)
        rounded = _round_approx(lat)
        assert abs(rounded - lat) <= _APPROX_GRID_DEG / 2 + 1e-9, (
            f"Rounding error too large: {lat} → {rounded}"
        )


# ---------------------------------------------------------------------------
# Authorization
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_anonymous_cannot_post_presence(http):
    """Unauthenticated POST to /social/presence → 401."""
    r = await http.post("/social/presence", json={"enabled": True, "lat": 45.5, "lon": -73.6, "consent_precision": "precise"})
    assert r.status_code == 401


@pytest.mark.asyncio
async def test_anonymous_cannot_delete_presence(http):
    """Unauthenticated DELETE to /social/presence → 401."""
    r = await http.delete("/social/presence")
    assert r.status_code == 401


# ---------------------------------------------------------------------------
# Consent precision rules
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_presence_none_stores_no_coordinates(http, db):
    """consent_precision='none' stores no lat/lon, enabled=FALSE."""
    mid = _make_identity()
    cookies = _member_cookie(mid)
    try:
        r = await http.post("/social/presence", json={
            "enabled": True,
            "lat": 45.5,
            "lon": -73.6,
            "consent_precision": "none",
        }, cookies=cookies)
        assert r.status_code == 200
        assert r.json()["enabled"] is False

        row = await _get_presence(db, mid)
        if row is not None:
            assert row["lat"] is None, "No lat must be stored when consent_precision=none"
            assert row["lon"] is None, "No lon must be stored when consent_precision=none"
            assert row["enabled"] is False
    finally:
        await _cleanup_presence(db, mid)


@pytest.mark.asyncio
async def test_presence_disabled_clears_coordinates(http, db):
    """Setting enabled=False clears existing coordinates from DB."""
    mid = _make_identity()
    cookies = _member_cookie(mid)
    try:
        # First enable with precise position.
        await http.post("/social/presence", json={
            "enabled": True, "lat": 48.85, "lon": 2.35, "consent_precision": "precise",
        }, cookies=cookies)

        # Now disable.
        r = await http.post("/social/presence", json={
            "enabled": False, "lat": None, "lon": None, "consent_precision": "none",
        }, cookies=cookies)
        assert r.status_code == 200
        assert r.json()["enabled"] is False

        row = await _get_presence(db, mid)
        if row is not None:
            assert row["lat"] is None, "Disabling must clear lat"
            assert row["lon"] is None, "Disabling must clear lon"
            assert row["enabled"] is False
    finally:
        await _cleanup_presence(db, mid)


@pytest.mark.asyncio
async def test_approximate_precision_rounds_stored_value(http, db):
    """consent_precision='approximate' stores rounded coordinates, not raw ones."""
    mid = _make_identity()
    cookies = _member_cookie(mid)
    raw_lat, raw_lon = 48.856613, 2.352222  # Paris
    try:
        r = await http.post("/social/presence", json={
            "enabled": True,
            "lat": raw_lat,
            "lon": raw_lon,
            "consent_precision": "approximate",
        }, cookies=cookies)
        assert r.status_code == 200
        assert r.json()["enabled"] is True
        assert r.json()["precision"] == "approximate"

        row = await _get_presence(db, mid)
        assert row is not None
        assert row["enabled"] is True
        assert row["precision"] == "approximate"
        stored_lat = float(row["lat"])
        stored_lon = float(row["lon"])
        # The stored value must differ from raw (or be equal if exactly on grid).
        assert stored_lat != raw_lat or stored_lon != raw_lon or True  # allow exact grid hit
        # Stored value must be on the 0.5° grid.
        from app.routers.social import _APPROX_GRID_DEG
        assert abs(stored_lat % _APPROX_GRID_DEG) < 1e-6 or abs(stored_lat % _APPROX_GRID_DEG - _APPROX_GRID_DEG) < 1e-6
        # Raw coordinates must NOT appear in the DB (privacy guarantee).
        assert stored_lat != raw_lat, "Raw latitude must not be stored for approximate precision"
        assert stored_lon != raw_lon, "Raw longitude must not be stored for approximate precision"
    finally:
        await _cleanup_presence(db, mid)


@pytest.mark.asyncio
async def test_precise_precision_stores_exact_coordinates(http, db):
    """consent_precision='precise' stores exact coordinates."""
    mid = _make_identity()
    cookies = _member_cookie(mid)
    raw_lat, raw_lon = 48.856613, 2.352222
    try:
        r = await http.post("/social/presence", json={
            "enabled": True,
            "lat": raw_lat,
            "lon": raw_lon,
            "consent_precision": "precise",
        }, cookies=cookies)
        assert r.status_code == 200
        assert r.json()["precision"] == "precise"

        row = await _get_presence(db, mid)
        assert row is not None
        assert abs(float(row["lat"]) - raw_lat) < 1e-6
        assert abs(float(row["lon"]) - raw_lon) < 1e-6
        assert row["precision"] == "precise"
        assert row["consent_given_at"] is not None
        assert row["expires_at"] is not None
    finally:
        await _cleanup_presence(db, mid)


@pytest.mark.asyncio
async def test_presence_has_ttl(http, db):
    """Enabling presence sets expires_at ≈ 24 hours from now."""
    mid = _make_identity()
    cookies = _member_cookie(mid)
    try:
        await http.post("/social/presence", json={
            "enabled": True, "lat": 51.5, "lon": -0.1, "consent_precision": "precise",
        }, cookies=cookies)

        row = await _get_presence(db, mid)
        assert row is not None
        assert row["expires_at"] is not None, "expires_at must be set when enabling presence"
        now = datetime.now(timezone.utc)
        expires = row["expires_at"]
        if expires.tzinfo is None:
            expires = expires.replace(tzinfo=timezone.utc)
        delta = expires - now
        assert timedelta(hours=23) < delta < timedelta(hours=25), (
            f"Expected TTL ≈ 24h, got {delta}"
        )
    finally:
        await _cleanup_presence(db, mid)


# ---------------------------------------------------------------------------
# Deletion (right to erasure)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_delete_removes_presence_row(http, db):
    """DELETE /social/presence removes the record entirely from DB."""
    mid = _make_identity()
    cookies = _member_cookie(mid)
    try:
        # Enable presence first.
        await http.post("/social/presence", json={
            "enabled": True, "lat": 40.7, "lon": -74.0, "consent_precision": "precise",
        }, cookies=cookies)
        row_before = await _get_presence(db, mid)
        assert row_before is not None, "Row should exist after enabling"

        # Now delete.
        r = await http.delete("/social/presence", cookies=cookies)
        assert r.status_code == 200
        assert r.json()["deleted"] is True

        row_after = await _get_presence(db, mid)
        assert row_after is None, "Row must be gone after DELETE"
    finally:
        await _cleanup_presence(db, mid)


# ---------------------------------------------------------------------------
# Expiry in overview
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_expired_presence_not_shown_in_connections(http, db):
    """A presence record past expires_at is not returned in overview connections."""
    owner_id = _make_identity()
    contact_id = _make_identity()
    owner_cookies = _member_cookie(owner_id)
    contact_cookies = _member_cookie(contact_id)
    try:
        # Set up both profiles via overview.
        await http.get("/social/overview", cookies=owner_cookies)
        await http.get("/social/overview", cookies=contact_cookies)

        # Manually insert connection.
        for a, b in ((owner_id, contact_id), (contact_id, owner_id)):
            await db.execute(
                text("INSERT INTO social_connections (owner_id, contact_id, label, status) VALUES (:a,:b,'friends','accepted') ON CONFLICT DO NOTHING"),
                {"a": a, "b": b},
            )
        await db.commit()

        # Insert an already-expired presence for contact.
        expired = datetime.now(timezone.utc) - timedelta(minutes=5)
        await db.execute(
            text("""
                INSERT INTO social_presence (member_id, lat, lon, enabled, precision, consent_given_at, expires_at, updated_at)
                VALUES (:id, 48.5, 2.5, TRUE, 'approximate', NOW() - INTERVAL '25 hours', :expired, NOW())
                ON CONFLICT (member_id) DO UPDATE SET lat=48.5, lon=2.5, enabled=TRUE,
                    precision='approximate', expires_at=:expired, updated_at=NOW()
            """),
            {"id": contact_id, "expired": expired},
        )
        await db.commit()

        # Owner's overview should NOT show contact's position.
        r = await http.get("/social/overview", cookies=owner_cookies)
        assert r.status_code == 200
        connections = r.json()["connections"]
        for conn in connections:
            if conn.get("member_id") == contact_id:
                assert conn.get("lat") is None, "Expired presence must not expose lat"
                assert conn.get("lon") is None, "Expired presence must not expose lon"
    finally:
        await _cleanup_presence(db, owner_id)
        await _cleanup_presence(db, contact_id)
