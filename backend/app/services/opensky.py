from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_HEADERS: dict[str, str] = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*",
}

# After each 429: wait 2s, then 4s, then 8s before the next attempt (3 retries, 4 tries total).
_OPENSKY_429_BACKOFF_SEC = (2.0, 4.0, 8.0)

# OpenSky public REST: https://openskynetwork.github.io/opensky-api/rest.html
OPENSKY_URL = "https://opensky-network.org/api/states/all"
# Public anonymous tier: keep at least 10s between states/all calls (slightly above to reduce 429s).
OPENSKY_MIN_POLL_INTERVAL_SEC = 12.0


def _iso_from_unix(ts: Any) -> str:
    if ts is None:
        return datetime.now(timezone.utc).isoformat()
    try:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()
    except (TypeError, ValueError, OSError):
        return datetime.now(timezone.utc).isoformat()


async def _fetch_opensky_json() -> Any:
    """GET states/all; on HTTP 429 only, exponential backoff 2s/4s/8s (max 3 retries)."""
    async with httpx.AsyncClient(
        timeout=40.0,
        headers=_HEADERS,
        follow_redirects=True,
    ) as client:
        for attempt in range(4):
            r = await client.get(OPENSKY_URL)
            if r.status_code == 429:
                if attempt < 3:
                    delay = _OPENSKY_429_BACKOFF_SEC[attempt]
                    logger.warning(
                        "OpenSky HTTP 429; backing off %.0fs before retry %d/3",
                        delay,
                        attempt + 1,
                    )
                    await asyncio.sleep(delay)
                    continue
                logger.warning("OpenSky HTTP 429; retries exhausted")
            r.raise_for_status()
            return r.json()


def _parse_states(data: Any) -> list[dict[str, Any]]:
    states = (data or {}).get("states") or []
    out: list[dict[str, Any]] = []
    for s in states:
        if not s or not isinstance(s, (list, tuple)) or len(s) < 11:
            continue
        icao24 = s[0]
        callsign = (s[1] or "").strip() if s[1] else ""
        time_pos = s[3]
        last_contact = s[4]
        lon = s[5]
        lat = s[6]
        baro_alt = s[7]
        on_ground = s[8]
        vel = s[9]
        true_track = s[10]
        squawk = s[14] if len(s) > 14 else None
        if lat is None or lon is None:
            continue
        try:
            lat_f = float(lat)
            lon_f = float(lon)
        except (TypeError, ValueError):
            continue
        obs = _iso_from_unix(time_pos if time_pos is not None else last_contact)
        out.append(
            {
                "id": str(icao24),
                "callsign": callsign or str(icao24),
                "lat": lat_f,
                "lon": lon_f,
                "alt_m": float(baro_alt) if baro_alt is not None else None,
                "velocity_mps": float(vel) if vel is not None else None,
                "heading_deg": float(true_track) if true_track is not None else None,
                "on_ground": bool(on_ground),
                "squawk": squawk,
                "observed_at": obs,
                "ingest_type": "aircraft",
                "source": "opensky",
            }
        )
    return out


async def fetch_aircraft_states() -> list[dict[str, Any]]:
    try:
        data = await _fetch_opensky_json()
        return _parse_states(data)
    except Exception:
        logger.exception("OpenSky fetch failed; publishing an empty aircraft layer")
        return []
