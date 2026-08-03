from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

import httpx

# ICAO transponder codes defined in ICAO Doc 8168 / Annex 2
_EMERGENCY_SQUAWKS = {"7500", "7600", "7700"}
# 1200 is the North-American VFR code (FAA JO 7110.65); not a class by itself.
_VFR_SQUAWK_NA = "1200"


def _classify_aircraft(squawk: str | None) -> dict[str, str] | None:
    """Classify an aircraft only from confirmed data (squawk transponder codes).

    Callsign-pattern heuristics are intentionally excluded: matching a
    3-letter ICAO airline code is not the same as confirming the operator
    through an authorised airline database.  Returns None when the class
    cannot be reliably determined.
    """
    sq = str(squawk or "").strip()
    if sq in _EMERGENCY_SQUAWKS:
        return {
            "aircraft_class": "emergency",
            "aircraft_class_derived_from": "squawk",
            "aircraft_class_confidence": "high",
        }
    if sq == _VFR_SQUAWK_NA:
        return {
            "aircraft_class": "general_aviation",
            "aircraft_class_derived_from": "squawk_vfr_north_america",
            "aircraft_class_confidence": "high",
        }
    return None


def _altitude_band(baro_alt: Any, on_ground: bool) -> dict[str, str] | None:
    """Derive altitude band from barometric altitude.

    This is an altitude observation, not a flight-phase determination.
    CLIMBING / DESCENDING / LEVEL require vertical_rate and are handled
    separately by _derive_flight_phase().
    """
    if on_ground:
        return {
            "altitude_band": "ground",
            "altitude_band_derived_from": "on_ground_flag",
            "altitude_band_confidence": "high",
        }
    try:
        alt = float(baro_alt)
    except (TypeError, ValueError):
        return None
    band = "low" if alt < 3_000 else "medium" if alt < 7_500 else "high"
    return {
        "altitude_band": band,
        "altitude_band_derived_from": "baro_altitude_m",
        "altitude_band_confidence": "high",
    }


def _derive_flight_phase(vertical_rate_mps: float | None, on_ground: bool) -> dict[str, str] | None:
    """Derive flight phase only when vertical_rate is available and unambiguous.

    Thresholds (ICAO-aligned, conservative):
      CLIMBING   : v ≥  1.0 m/s (~200 ft/min)
      DESCENDING : v ≤ -1.0 m/s
      LEVEL      : |v| ≤ 0.25 m/s (~50 ft/min) — aircraft in flight, truly level
    Rates between ±0.25 and ±1.0 m/s are transitional and left unclassified.
    """
    if on_ground or vertical_rate_mps is None:
        return None
    v = vertical_rate_mps
    if v >= 1.0:
        conf = "high" if v >= 5.0 else "medium"
        return {"flight_phase": "CLIMBING", "flight_phase_derived_from": "vertical_rate_mps", "flight_phase_confidence": conf}
    if v <= -1.0:
        conf = "high" if v <= -5.0 else "medium"
        return {"flight_phase": "DESCENDING", "flight_phase_derived_from": "vertical_rate_mps", "flight_phase_confidence": conf}
    if abs(v) <= 0.25:
        return {"flight_phase": "LEVEL", "flight_phase_derived_from": "vertical_rate_mps", "flight_phase_confidence": "high"}
    return None  # transitional rate — no reliable classification

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
        # s[11] = vertical_rate (m/s, baro), available in OpenSky state vectors
        vertical_rate_raw = s[11] if len(s) > 11 else None
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
        on_gnd = bool(on_ground)
        vr_mps = float(vertical_rate_raw) if vertical_rate_raw is not None else None
        row: dict[str, Any] = {
            "id": str(icao24),
            "callsign": callsign or str(icao24),
            "lat": lat_f,
            "lon": lon_f,
            "alt_m": float(baro_alt) if baro_alt is not None else None,
            "velocity_mps": float(vel) if vel is not None else None,
            "vertical_rate_mps": vr_mps,
            "heading_deg": float(true_track) if true_track is not None else None,
            "on_ground": on_gnd,
            "squawk": sq,
            "observed_at": obs,
            "ingest_type": "aircraft",
            "source": "opensky",
        }
        cls_meta = _classify_aircraft(sq)
        if cls_meta:
            row.update(cls_meta)
        band_meta = _altitude_band(baro_alt, on_gnd)
        if band_meta:
            row.update(band_meta)
        phase_meta = _derive_flight_phase(vr_mps, on_gnd)
        if phase_meta:
            row.update(phase_meta)
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
                alt_baro = item.get("alt_baro")
                on_gnd = alt_baro == "ground"
                alt_m = float(alt_baro) * 0.3048 if isinstance(alt_baro, (int, float)) else None
                # baro_rate from adsb.lol is in ft/min; convert to m/s (1 ft/min = 0.00508 m/s)
                baro_rate_raw = item.get("baro_rate")
                vr_mps = float(baro_rate_raw) * 0.00508 if isinstance(baro_rate_raw, (int, float)) else None
                entry: dict[str, Any] = {
                    "id": ident,
                    "callsign": str(item.get("flight") or ident).strip(),
                    "registration": item.get("r"),
                    "aircraft_type": item.get("t"),
                    "lat": float(lat),
                    "lon": float(lon),
                    "alt_m": alt_m,
                    "velocity_mps": float(item.get("gs")) * 0.514444 if item.get("gs") is not None else None,
                    "vertical_rate_mps": vr_mps,
                    "heading_deg": item.get("track"),
                    "on_ground": on_gnd,
                    "squawk": squawk_raw,
                    "observed_at": datetime.now(timezone.utc).isoformat(),
                    "ingest_type": "aircraft",
                    "source": "adsb.lol",
                    "license": "ODbL-1.0",
                }
                # dbFlags bit 0 = military/government (confirmed by adsb.lol/OpenSky database)
                if flags & 1:
                    entry.update({"aircraft_class": "government", "aircraft_class_derived_from": "adsb_db_flags_military", "aircraft_class_confidence": "high"})
                else:
                    cls_meta = _classify_aircraft(squawk_raw)
                    if cls_meta:
                        entry.update(cls_meta)
                band_meta = _altitude_band(alt_m, on_gnd)
                if band_meta:
                    entry.update(band_meta)
                phase_meta = _derive_flight_phase(vr_mps, on_gnd)
                if phase_meta:
                    entry.update(phase_meta)
                out[ident] = entry
        return list(out.values())[:2500]
    except Exception:
        logger.exception("aircraft fallback failed; publishing an empty aircraft layer")
        return []
