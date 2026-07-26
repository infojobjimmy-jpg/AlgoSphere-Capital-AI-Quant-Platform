from __future__ import annotations

import asyncio
import math
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Query

from app.config import settings

router = APIRouter()

ONTARIO_511 = {
    "cameras": "https://511on.ca/api/v2/get/cameras",
    "inspection_stations": "https://511on.ca/api/v2/get/inspectionstations",
    "truck_rest_areas": "https://511on.ca/api/v2/get/truckrestareas",
    "road_conditions": "https://511on.ca/api/v3/get/roadconditions",
    "events": "https://511on.ca/api/v2/get/event",
}


def _coordinates(item: dict[str, Any]) -> tuple[float, float] | None:
    for lat_key, lon_key in (("Latitude", "Longitude"), ("latitude", "longitude"), ("lat", "lon")):
        try:
            lat, lon = float(item[lat_key]), float(item[lon_key])
            if -90 <= lat <= 90 and -180 <= lon <= 180:
                return lat, lon
        except (KeyError, TypeError, ValueError):
            continue
    return None


def _step_instruction(maneuver_type: str, maneuver_modifier: str, street_name: str) -> str:
    mod_fr: dict[str, str] = {
        "left": "à gauche", "right": "à droite",
        "slight left": "légèrement à gauche", "slight right": "légèrement à droite",
        "sharp left": "fortement à gauche", "sharp right": "fortement à droite",
        "uturn": "demi-tour", "straight": "tout droit",
    }
    mod = mod_fr.get(maneuver_modifier, maneuver_modifier)
    road = f" sur {street_name}" if street_name else ""
    if maneuver_type == "depart":
        return f"Démarrez{road}" if street_name else "Démarrez"
    if maneuver_type == "arrive":
        return "Vous êtes arrivé à destination"
    if maneuver_type in ("turn", "new name"):
        return f"Tournez {mod}{road}"
    if maneuver_type == "merge":
        return f"Rejoignez{road}" if street_name else "Rejoignez la route"
    if maneuver_type == "on ramp":
        return f"Prenez la bretelle{' vers ' + street_name if street_name else ''}"
    if maneuver_type == "off ramp":
        return f"Prenez la sortie{' vers ' + street_name if street_name else ''}"
    if maneuver_type == "fork":
        return f"Gardez {mod} à la bifurcation" if mod else "Gardez votre voie à la bifurcation"
    if maneuver_type == "roundabout":
        return f"Prenez le rond-point{' vers ' + street_name if street_name else ''}"
    if maneuver_type == "continue":
        return f"Continuez{road}" if street_name else "Continuez tout droit"
    return f"Continuez{road}" if street_name else "Continuez"


def _distance_km(a_lat: float, a_lon: float, b_lat: float, b_lon: float) -> float:
    radius = 6371.0088
    p1, p2 = math.radians(a_lat), math.radians(b_lat)
    dp, dl = math.radians(b_lat - a_lat), math.radians(b_lon - a_lon)
    h = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return radius * 2 * math.atan2(math.sqrt(h), math.sqrt(1 - h))


async def _fetch(client: httpx.AsyncClient, category: str, url: str) -> tuple[str, list[dict[str, Any]], str]:
    try:
        response = await client.get(url, params={"format": "json", "lang": "en"})
        response.raise_for_status()
        payload = response.json()
        rows = payload if isinstance(payload, list) else payload.get("items", payload.get("data", []))
        return category, rows if isinstance(rows, list) else [], "available"
    except Exception:
        return category, [], "unavailable"


@router.get("/status")
async def status() -> dict:
    return {
        "car_routing": "available",
        "truck_routing": "advisory",
        "coverage": ["Canada", "United States"],
        "sources": [
            {"id": "openstreetmap", "label": "OpenStreetMap / OSRM", "status": "available"},
            {"id": "ontario511", "label": "Ontario 511", "status": "available"},
            {"id": "us_nbi", "label": "US National Bridge Inventory", "status": "planned"},
            {"id": "cbsa_cbp", "label": "CBSA / CBP border waits", "status": "planned"},
        ],
        "warning": "Truck restrictions are advisory until all jurisdictions are connected.",
    }


@router.get("/route")
async def route(
    origin_lat: float = Query(ge=-90, le=90),
    origin_lon: float = Query(ge=-180, le=180),
    destination_lat: float = Query(ge=-90, le=90),
    destination_lon: float = Query(ge=-180, le=180),
    mode: str = Query(default="car", pattern="^(car|truck)$"),
) -> dict:
    url = (
        f"{settings.navigation_osrm_base_url.rstrip('/')}/route/v1/driving/"
        f"{origin_lon},{origin_lat};{destination_lon},{destination_lat}"
    )
    try:
        async with httpx.AsyncClient(timeout=settings.navigation_http_timeout_sec) as client:
            response = await client.get(url, params={"overview": "full", "geometries": "geojson", "steps": "true"})
            response.raise_for_status()
            payload = response.json()
    except Exception as exc:
        raise HTTPException(status_code=502, detail="Routing source unavailable") from exc
    routes = payload.get("routes") or []
    if not routes:
        raise HTTPException(status_code=404, detail="No route found")
    selected = routes[0]
    steps: list[dict[str, Any]] = []
    for leg in selected.get("legs", []):
        for step in leg.get("steps", []):
            maneuver = step.get("maneuver", {})
            mtype = maneuver.get("type", "")
            mmod = maneuver.get("modifier", "")
            loc = maneuver.get("location", [None, None])
            name = step.get("name") or ""
            steps.append({
                "instruction": _step_instruction(mtype, mmod, name),
                "street_name": name,
                "distance_m": step.get("distance", 0),
                "duration_s": step.get("duration", 0),
                "maneuver_type": mtype,
                "location": {
                    "lat": loc[1] if len(loc) > 1 else None,
                    "lon": loc[0] if len(loc) > 0 else None,
                },
            })
    return {
        "mode": mode,
        "distance_m": selected.get("distance"),
        "duration_s": selected.get("duration"),
        "geometry": selected.get("geometry"),
        "legs": selected.get("legs", []),
        "steps": steps,
        "truck_restrictions_applied": False,
        "warning": (
            "Truck profile is advisory only; posted restrictions take priority."
            if mode == "truck"
            else None
        ),
        "source": "OpenStreetMap / OSRM",
    }


@router.get("/search")
async def search_destination(q: str = Query(min_length=3, max_length=160)) -> dict:
    try:
        async with httpx.AsyncClient(
            timeout=settings.navigation_http_timeout_sec,
            headers={"User-Agent": "AlgoSphereGlobal/1.0 (https://algosphereglobal.com)"},
        ) as client:
            response = await client.get(
                "https://nominatim.openstreetmap.org/search",
                params={"q": q, "format": "jsonv2", "limit": 5, "countrycodes": "ca,us"},
            )
            response.raise_for_status()
            rows = response.json()
    except Exception as exc:
        raise HTTPException(status_code=502, detail="Address search is unavailable") from exc
    return {
        "results": [
            {
                "label": item.get("display_name"),
                "lat": float(item["lat"]),
                "lon": float(item["lon"]),
                "type": item.get("type"),
            }
            for item in rows
            if item.get("lat") and item.get("lon")
        ]
    }


@router.get("/nearby")
async def nearby(
    lat: float = Query(ge=-90, le=90),
    lon: float = Query(ge=-180, le=180),
    radius_km: float = Query(default=100, ge=1, le=500),
) -> dict:
    async with httpx.AsyncClient(timeout=settings.navigation_http_timeout_sec) as client:
        results = await asyncio.gather(*(_fetch(client, category, url) for category, url in ONTARIO_511.items()))
    layers: dict[str, list[dict[str, Any]]] = {}
    statuses: dict[str, str] = {}
    for category, rows, source_status in results:
        statuses[category] = source_status
        nearby_rows = []
        for row in rows:
            coords = _coordinates(row)
            if coords is None:
                continue
            distance = _distance_km(lat, lon, coords[0], coords[1])
            if distance <= radius_km:
                nearby_rows.append({**row, "_distance_km": round(distance, 1)})
        layers[category] = sorted(nearby_rows, key=lambda item: item["_distance_km"])[:100]
    return {"source": "Ontario 511", "statuses": statuses, "layers": layers}
