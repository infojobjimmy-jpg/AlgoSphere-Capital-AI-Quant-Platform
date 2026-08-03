from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timezone
from typing import Any

import httpx

_AIRLINE_CALLSIGN = re.compile(r"^[A-Z]{3}\d{1,4}[A-Z]?$")
_EMERGENCY_SQUAWKS = {"7500", "7600", "7700"}


def _aircraft_class_from(callsign: str, squawk: str | None) -> str | None:
    sq = str(squawk or "").strip()
    if sq in _EMERGENCY_SQUAWKS:
        return "emergency"
    if sq == "1200":
        return "private"
    if callsign and _AIRLINE_CALLSIGN.match(callsign):
        return "commercial"
    return None


def _flight_phase(baro_alt: Any, on_ground: bool) -> str | None:
    if on_ground:
        return "ground"
    try:
        alt = float(baro_alt)
    except (TypeError, ValueError):
        return None
    if alt < 3_000:
        return "low"
    if alt < 7_500:
        return "medium"
    return "high"

logger = logging.getLogger(__name__)

_HEADERS: dict[str, str] = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*",
}

# After each 429: wait 2s, then 4s, then 8s before the next attempt (3 retries, 4 tries total).
_OPENSKY_429_BACKOFF_SEC = (2.0, 4.0, 8.0)

# OpenSky public REST: https://openskynetwork.github.io/opensky-api/rest.html
OPENSKY_URL = "https://opensky-network.org/api/states/all"
ADSB_LOL_URLS = [
    "https://api.adsb.lol/v2/lat/43.65/lon/-79.38/dist/250",
    "https://api.adsb.lol/v2/lat/45.50/lon/-73.57/dist/250",
    "https://api.adsb.lol/v2/lat/40.71/lon/-74.00/dist/250",
    "https://api.adsb.lol/v2/lat/41.88/lon/-87.63/dist/250",
    "https://api.adsb.lol/v2/lat/33.75/lon/-84.39/dist/250",
    "https://api.adsb.lol/v2/lat/29.76/lon/-95.37/dist/250",
    "https://api.adsb.lol/v2/lat/34.05/lon/-118.24/dist/250",
    "https://api.adsb.lol/v2/lat/49.28/lon/-123.12/dist/250",
]
# Anonymous access has a small daily allowance. A two-minute interval and a
# regional bounding box avoid exhausting it within minutes.
OPENSKY_MIN_POLL_INTERVAL_SEC = 120.0
NORTH_AMERICA_BOUNDS = {"lamin": 15, "lomin": -170, "lamax": 75, "lomax": -50}


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
            r = await client.get(OPENSKY_URL, params=NORTH_AMERICA_BOUNDS)
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
        sq = str(squawk or "").strip() or None
        cls = _aircraft_class_from(callsign, sq)
        phase = _flight_phase(baro_alt, bool(on_ground))
        row: dict[str, Any] = {
            "id": str(icao24),
            "callsign": callsign or str(icao24),
            "lat": lat_f,
            "lon": lon_f,
            "alt_m": float(baro_alt) if baro_alt is not None else None,
            "velocity_mps": float(vel) if vel is not None else None,
            "heading_deg": float(true_track) if true_track is not None else None,
            "on_ground": bool(on_ground),
            "squawk": sq,
            "observed_at": obs,
            "ingest_type": "aircraft",
            "source": "opensky",
        }
        if cls is not None:
            row["aircraft_class"] = cls
        if phase is not None:
            row["flight_phase"] = phase
        out.append(row)
    return out


async def fetch_aircraft_states() -> list[dict[str, Any]]:
    try:
        data = await _fetch_opensky_json()
        return _parse_states(data)
    except Exception:
        logger.warning("OpenSky unavailable; using the open ODbL adsb.lol regional fallback")
    out: dict[str, dict[str, Any]] = {}
    try:
        async with httpx.AsyncClient(timeout=35.0, headers={**_HEADERS, "User-Agent": "AlgoSphereGlobal/1.0"}) as client:
            responses = await asyncio.gather(*(client.get(url) for url in ADSB_LOL_URLS), return_exceptions=True)
        for response in responses:
            if isinstance(response, Exception) or response.status_code != 200:
                continue
            for item in response.json().get("ac", []):
                lat, lon = item.get("lat"), item.get("lon")
                if lat is None or lon is None:
                    continue
                ident = str(item.get("hex") or "").lower()
                if not ident:
                    continue
                flags = int(item.get("dbFlags") or 0)
                squawk_raw = str(item.get("squawk") or "").strip() or None
                category = "government" if flags & 1 else "emergency" if squawk_raw in _EMERGENCY_SQUAWKS else "commercial" if item.get("flight") else "private"
                alt_baro = item.get("alt_baro")
                on_gnd = alt_baro == "ground"
                alt_m = float(alt_baro) * 0.3048 if isinstance(alt_baro, (int, float)) else None
                phase = _flight_phase(alt_m, on_gnd)
                entry: dict[str, Any] = {
                    "id": ident,
                    "callsign": str(item.get("flight") or ident).strip(),
                    "registration": item.get("r"),
                    "aircraft_type": item.get("t"),
                    "aircraft_class": category,
                    "lat": float(lat),
                    "lon": float(lon),
                    "alt_m": alt_m,
                    "velocity_mps": float(item.get("gs")) * 0.514444 if item.get("gs") is not None else None,
                    "heading_deg": item.get("track"),
                    "on_ground": on_gnd,
                    "squawk": squawk_raw,
                    "observed_at": datetime.now(timezone.utc).isoformat(),
                    "ingest_type": "aircraft",
                    "source": "adsb.lol",
                    "license": "ODbL-1.0",
                }
                if phase is not None:
                    entry["flight_phase"] = phase
                out[ident] = entry
        return list(out.values())[:2500]
    except Exception:
        logger.exception("aircraft fallback failed; publishing an empty aircraft layer")
        return []
