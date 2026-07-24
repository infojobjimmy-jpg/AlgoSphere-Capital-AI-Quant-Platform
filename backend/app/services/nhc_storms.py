"""Active tropical cyclones from the US National Hurricane Center."""

from __future__ import annotations

from typing import Any

from app.services.resilient_http import get_json

NHC_CURRENT_STORMS = "https://www.nhc.noaa.gov/CurrentStorms.json"


async def fetch_active_storms() -> list[dict[str, Any]]:
    payload = await get_json(NHC_CURRENT_STORMS, timeout=30.0)
    rows: list[dict[str, Any]] = []
    for storm in payload.get("activeStorms", []):
        lat = storm.get("latitudeNumeric")
        lon = storm.get("longitudeNumeric")
        if lat is None or lon is None:
            continue
        advisory = storm.get("publicAdvisory") or {}
        rows.append(
            {
                "id": f"nhc_{storm.get('id')}",
                "label": storm.get("name") or storm.get("id"),
                "classification": storm.get("classification"),
                "lat": float(lat),
                "lon": float(lon),
                "wind_kn": float(storm.get("intensity") or 0),
                "pressure_mb": float(storm.get("pressure") or 0),
                "movement_deg": storm.get("movementDir"),
                "movement_kn": storm.get("movementSpeed"),
                "observed_at": storm.get("lastUpdate"),
                "advisory_url": advisory.get("url"),
                "ingest_type": "storm",
                "source": "noaa_nhc",
            }
        )
    return rows
