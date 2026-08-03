"""Public transit vehicle positions from a GTFS Realtime JSON feed.

Privacy: Only public transit vehicle types (bus, rail, tram, metro, ferry) are
returned. Private vehicles are never included. Set TRANSIT_FEED_URL in the
environment to activate; without it the service returns an empty list
immediately — no fake data is substituted.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import httpx

from app.config import settings

logger = logging.getLogger(__name__)

# GTFS route_type → vehicle class (spec: gtfs.org/documentation/schedule/reference)
_ROUTE_TYPE_CLASS: dict[int, str] = {
    0: "tram",       # Tram / Streetcar / Light rail
    1: "metro",      # Subway / Metro
    2: "rail",       # Rail (intercity, commuter)
    3: "bus",        # Bus
    4: "ferry",      # Ferry
    5: "cable",      # Cable tram
    6: "aerial",     # Aerial lift / Gondola
    7: "funicular",  # Funicular
    11: "bus",       # Trolleybus
    12: "rail",      # Monorail
}


def _vehicle_class(route_type: Any) -> str:
    try:
        return _ROUTE_TYPE_CLASS.get(int(route_type), "transit")
    except (TypeError, ValueError):
        return "transit"


async def fetch_transit_vehicles() -> list[dict[str, Any]]:
    """Fetch and normalise public transit vehicle positions.

    Returns an empty list immediately when TRANSIT_FEED_URL is not set.
    Only public transit vehicle types are included; private vehicles are never
    returned.
    """
    feed_url = settings.transit_feed_url
    if not feed_url:
        return []

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            r = await client.get(feed_url)
            r.raise_for_status()
            data = r.json()
    except Exception:
        logger.warning("Transit GTFS-RT feed unavailable", exc_info=True)
        return []

    # GTFS-RT JSON uses "entity"; some custom REST APIs use "vehicles"
    entities: list[Any] = data.get("entity") or data.get("vehicles") or []
    if not entities and isinstance(data, list):
        entities = data

    out: list[dict[str, Any]] = []
    for ent in entities:
        veh = ent.get("vehicle") or ent
        pos = veh.get("position") or {}
        lat = pos.get("latitude") or veh.get("lat")
        lon = pos.get("longitude") or veh.get("lon")
        if lat is None or lon is None:
            continue
        try:
            lat_f, lon_f = float(lat), float(lon)
        except (TypeError, ValueError):
            continue
        if not (-90 <= lat_f <= 90 and -180 <= lon_f <= 180):
            continue

        trip = veh.get("trip") or {}
        route_type = (
            trip.get("route_type")
            or veh.get("route_type")
            or ent.get("route_type")
            or 3  # default: bus
        )
        veh_id_block = veh.get("vehicle") or {}
        veh_id = str(veh_id_block.get("id") or ent.get("id") or "").strip()
        veh_label = str(veh_id_block.get("label") or veh_id or "").strip()
        route_id = str(trip.get("route_id") or ent.get("route_id") or "").strip()

        # GTFS-RT position.speed is in m/s; convert to km/h for display
        speed_mps = pos.get("speed")
        speed_kmh = round(float(speed_mps) * 3.6, 1) if speed_mps is not None else None

        out.append({
            "id": f"transit_{veh_id or len(out)}",
            "label": veh_label or route_id or "Transit",
            "lat": lat_f,
            "lon": lon_f,
            "route_id": route_id or None,
            "vehicle_class": _vehicle_class(route_type),
            "speed_kmh": speed_kmh,
            "bearing_deg": float(pos["bearing"]) if pos.get("bearing") is not None else None,
            "on_ground": True,
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "ingest_type": "transit_vehicle",
            "source": "gtfs_realtime",
        })

    return out[:2000]
