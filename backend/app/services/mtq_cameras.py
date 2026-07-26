"""MTQ WFS traffic camera connector — Gouvernement du Québec, CC-BY 4.0."""

from __future__ import annotations

import logging
import time
from math import atan, degrees, exp, pi
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_MTQ_WFS_URL = "https://ws.mapserver.transports.gouv.qc.ca/swtq"
_MTQ_WFS_PARAMS = {
    "SERVICE": "WFS",
    "VERSION": "1.1.0",
    "REQUEST": "GetFeature",
    "TYPENAME": "ms:infos_cameras",
    "OUTPUTFORMAT": "geojson",
    "maxfeatures": "9999",
}

_CACHE_TTL = 3600.0
_cache_data: list[dict[str, Any]] = []
_cache_ts: float = 0.0


def _merc_to_wgs84(x: float, y: float) -> tuple[float, float]:
    lon = x / 20037508.342 * 180.0
    lat = degrees(2.0 * atan(exp(y * pi / 20037508.342)) - pi / 2.0)
    return lat, lon


async def fetch_mtq_cameras() -> list[dict[str, Any]]:
    global _cache_data, _cache_ts
    if _cache_data and (time.monotonic() - _cache_ts) < _CACHE_TTL:
        return _cache_data

    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        resp = await client.get(_MTQ_WFS_URL, params=_MTQ_WFS_PARAMS)
        resp.raise_for_status()
        payload = resp.json()

    features = payload.get("features") or []
    out: list[dict[str, Any]] = []

    for feat in features:
        props = feat.get("properties") or {}
        geom = feat.get("geometry") or {}
        coords = geom.get("coordinates")

        cam_id = str(props.get("IDEcamera") or props.get("id_camera") or "")
        if not cam_id:
            continue

        # Resolve coordinates: geometry (EPSG:3857) first, then property fallback
        lat: float | None = None
        lon: float | None = None
        if coords and len(coords) >= 2:
            try:
                x, y = float(coords[0]), float(coords[1])
                # MTQ WFS returns EPSG:3857 — check range to confirm
                if abs(x) > 180 or abs(y) > 90:
                    lat, lon = _merc_to_wgs84(x, y)
                else:
                    lat, lon = y, x
            except (TypeError, ValueError):
                pass

        if lat is None:
            lat_raw = props.get("LatitudeDecimal") or props.get("latitude")
            lon_raw = props.get("LongitudeDecimal") or props.get("longitude")
            if lat_raw is not None and lon_raw is not None:
                try:
                    lat, lon = float(lat_raw), float(lon_raw)
                except (TypeError, ValueError):
                    pass

        if lat is None or lon is None:
            continue

        if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
            continue

        label = (
            str(props.get("DescriptionLocalisationFr") or props.get("description_localisation_fr") or cam_id)
        )
        info_url = str(props.get("URL_FLUX_DONNEE") or props.get("url_flux_donnee") or "")

        out.append(
            {
                "id": f"mtq_{cam_id}",
                "label": label,
                "lat": round(lat, 6),
                "lon": round(lon, 6),
                "preview_url": f"https://images.quebec511.info/camera/{cam_id}/last.jpg",
                "info_url": info_url or None,
                "stream_url": None,
                "stream_category": "traffic",
                "health_state": "available",
                "jurisdiction": "QC",
                "attribution": "Données ouvertes | Gouvernement du Québec CC-BY 4.0",
                "source": "MTQ_WFS",
                "ingest_type": "camera",
            }
        )

    if out:
        _cache_data = out
        _cache_ts = time.monotonic()
        logger.info("MTQ WFS: cached %d cameras", len(out))
    else:
        logger.warning("MTQ WFS: response parsed but 0 valid cameras extracted")

    return out
