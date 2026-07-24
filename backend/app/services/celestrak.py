from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any

import httpx
from skyfield.api import EarthSatellite, load

from app.services.resilient_http import get_text

logger = logging.getLogger(__name__)

# The full active catalog is rate-limited by CelesTrak and returns 403 when
# polled frequently from a shared cloud address. The stations group is a
# stable, official, compact TLE feed suitable for the public globe.
STATIONS_TLE = "https://celestrak.org/NORAD/elements/gp.php?GROUP=stations&FORMAT=tle"
ISS_POSITION = "https://api.wheretheiss.at/v1/satellites/25544"
SATNOGS_TLE = "https://db.satnogs.org/api/tle/?format=json"
SATNOGS_SATELLITES = "https://db.satnogs.org/api/satellites/?format=json"

_catalog_cache: tuple[float, list[tuple[str, str, str]], dict[int, dict[str, str]]] | None = None


def _parse_tle_blocks(text: str) -> list[tuple[str, str, str]]:
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    blocks: list[tuple[str, str, str]] = []
    i = 0
    while i + 2 < len(lines):
        name = lines[i]
        if lines[i + 1].startswith("1 ") and lines[i + 2].startswith("2 "):
            blocks.append((name, lines[i + 1], lines[i + 2]))
            i += 3
        else:
            i += 1
    return blocks


def _gp_json_to_tle_blocks(data: Any, *, max_sats: int = 200) -> list[tuple[str, str, str]]:
    """Extract NORAD two-line elements from Celestrak GP JSON."""
    blocks: list[tuple[str, str, str]] = []
    if not isinstance(data, list):
        return blocks
    for row in data:
        if not isinstance(row, dict):
            continue
        name = str(row.get("OBJECT_NAME") or row.get("OBJECT_ID") or "SAT").strip()
        l1 = row.get("TLE_LINE1") or row.get("tle_line1")
        l2 = row.get("TLE_LINE2") or row.get("tle_line2")
        if not l1 or not l2:
            continue
        l1s, l2s = str(l1).strip(), str(l2).strip()
        if not l1s.startswith("1 ") or not l2s.startswith("2 "):
            continue
        blocks.append((name, l1s, l2s))
        if len(blocks) >= max_sats:
            break
    return blocks


def propagate_positions(
    blocks: list[tuple[str, str, str]], when: datetime | None = None
) -> list[dict[str, Any]]:
    when = when or datetime.now(timezone.utc)
    ts = load.timescale()
    t = ts.from_datetime(when.astimezone(timezone.utc))
    out: list[dict[str, Any]] = []
    for name, l1, l2 in blocks[:2000]:
        try:
            sat = EarthSatellite(l1, l2, name, ts)
            geo = sat.at(t)
            sub = geo.subpoint()
            alt_km = geo.distance().km - 6371.0
            out.append(
                {
                    "id": str(sat.model.satnum),
                    "label": name.strip(),
                    "lat": float(sub.latitude.degrees),
                    "lon": float(sub.longitude.degrees),
                    "alt_km": float(alt_km),
                    "orbit": "LEO" if alt_km < 2000 else ("MEO" if alt_km < 30000 else "GEO"),
                }
            )
        except Exception:
            logger.debug("invalid orbital elements for %s", name, exc_info=True)
    return out


def _function_from_name(name: str) -> str:
    value = name.upper()
    if "ISS" in value or "TIANGONG" in value:
        return "Space station"
    if any(token in value for token in ("GPS", "GLONASS", "GALILEO", "BEIDOU", "NAVSTAR")):
        return "Navigation"
    if any(token in value for token in ("NOAA", "METEOR", "METOP", "GOES", "WEATHER")):
        return "Weather"
    if any(token in value for token in ("STARLINK", "ONEWEB", "IRIDIUM", "INTELSAT", "EUTELSAT")):
        return "Communications"
    if any(token in value for token in ("CUBE", "AMSAT", "OSCAR")):
        return "Amateur / education"
    return "Other"


async def _satnogs_catalog() -> tuple[list[tuple[str, str, str]], dict[int, dict[str, str]]]:
    global _catalog_cache
    now = time.monotonic()
    if _catalog_cache and now - _catalog_cache[0] < 1800:
        return _catalog_cache[1], _catalog_cache[2]
    headers = {"User-Agent": "AlgoSphereGlobal/1.0 (satellite display; contact via algosphereglobal.com)"}
    async with httpx.AsyncClient(timeout=45.0, headers=headers, follow_redirects=True) as client:
        tle_response = await client.get(SATNOGS_TLE)
        tle_response.raise_for_status()
        tle_rows = tle_response.json()
        metadata: dict[int, dict[str, str]] = {}
        try:
            sat_response = await client.get(SATNOGS_SATELLITES)
            sat_response.raise_for_status()
            for row in sat_response.json():
                norad = row.get("norad_cat_id")
                if norad is not None:
                    metadata[int(norad)] = {
                        "country": str(row.get("countries") or "Unknown"),
                        "operator": str(row.get("operator") or ""),
                    }
        except Exception:
            logger.warning("SatNOGS satellite metadata unavailable; continuing with orbital data", exc_info=True)
    blocks = [
        (
            str(row.get("tle0") or "SAT").removeprefix("0 ").strip(),
            str(row.get("tle1") or "").strip(),
            str(row.get("tle2") or "").strip(),
        )
        for row in tle_rows
        if str(row.get("tle1") or "").startswith("1 ") and str(row.get("tle2") or "").startswith("2 ")
    ]
    if not blocks:
        raise ValueError("SatNOGS returned no valid orbital elements")
    _catalog_cache = (now, blocks, metadata)
    return blocks, metadata


async def fetch_satellites() -> list[dict[str, Any]]:
    when = datetime.now(timezone.utc)
    blocks: list[tuple[str, str, str]] = []
    metadata: dict[int, dict[str, str]] = {}
    source = "SatNOGS DB / Space-Track.org"

    try:
        blocks, metadata = await _satnogs_catalog()
    except Exception:
        logger.exception("SatNOGS orbital catalog failed")
        try:
            text = await get_text(STATIONS_TLE, timeout=50.0)
            blocks = _parse_tle_blocks(text)
            source = "CelesTrak"
        except Exception:
            logger.exception("CelesTrak stations TLE feed failed")

    if not blocks:
        logger.warning("CelesTrak unavailable; using live ISS position fallback")
        try:
            async with httpx.AsyncClient(timeout=20.0, headers={"User-Agent": "AlgoSphereGlobal/1.0"}) as client:
                response = await client.get(ISS_POSITION)
                response.raise_for_status()
                item = response.json()
            return [{
                "id": "25544",
                "label": "ISS (ZARYA)",
                "lat": float(item["latitude"]),
                "lon": float(item["longitude"]),
                "alt_km": float(item.get("altitude") or 0),
                "country": "International",
                "function": "Space station",
                "orbit": "LEO",
                "observed_at": datetime.now(timezone.utc).isoformat(),
                "ingest_type": "satellite",
                "source": "wheretheiss.at",
            }]
        except Exception:
            logger.exception("ISS fallback unavailable; publishing an empty satellite layer")
            return []

    positions = propagate_positions(blocks, when=when)
    if not positions:
        logger.warning("satellite propagation returned empty; publishing an empty satellite layer")
        return []
    ts_iso = when.isoformat()
    for p in positions:
        details = metadata.get(int(p["id"]), {})
        p["country"] = details.get("country", "Unknown")
        p["operator"] = details.get("operator", "")
        p["function"] = _function_from_name(str(p["label"]))
        p["observed_at"] = ts_iso
        p["ingest_type"] = "satellite"
        p["source"] = source
        p["tle_source"] = source
    return positions
