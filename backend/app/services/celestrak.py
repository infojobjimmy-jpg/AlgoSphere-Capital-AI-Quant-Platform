from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from skyfield.api import EarthSatellite, load

from app.services.resilient_http import get_json, get_text

logger = logging.getLogger(__name__)

# GP JSON (active catalog) — primary source per ops requirement
ACTIVE_GP_JSON = "https://celestrak.org/NORAD/elements/gp.php?GROUP=active&FORMAT=json"
# Classic TLE text — fallback if JSON unavailable
STATIONS_TLE = "https://celestrak.org/NORAD/elements/stations.txt"


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
    for name, l1, l2 in blocks[:150]:
        sat = EarthSatellite(l1, l2, name, ts)
        geo = sat.at(t)
        sub = geo.subpoint()
        alt_km = geo.distance().km - 6371.0
        out.append(
            {
                "id": name.strip().replace(" ", "_"),
                "label": name.strip(),
                "lat": float(sub.latitude.degrees),
                "lon": float(sub.longitude.degrees),
                "alt_km": float(alt_km),
            }
        )
    return out


async def fetch_satellites() -> list[dict[str, Any]]:
    when = datetime.now(timezone.utc)
    blocks: list[tuple[str, str, str]] = []

    try:
        data = await get_json(ACTIVE_GP_JSON, timeout=45.0)
        blocks = _gp_json_to_tle_blocks(data)
    except Exception:
        logger.exception("celestrak active GP JSON failed; trying stations TLE text")

    if not blocks:
        try:
            text = await get_text(STATIONS_TLE, timeout=50.0)
            blocks = _parse_tle_blocks(text)
        except Exception:
            logger.exception("celestrak stations TLE fallback failed")

    if not blocks:
        logger.warning("satellite sources unavailable; publishing an empty satellite layer")
        return []

    positions = propagate_positions(blocks, when=when)
    if not positions:
        logger.warning("satellite propagation returned empty; publishing an empty satellite layer")
        return []
    ts_iso = when.isoformat()
    for p in positions:
        p["observed_at"] = ts_iso
        p["ingest_type"] = "satellite"
        p.setdefault("tle_source", "celestrak_gp_or_tle")
    return positions
