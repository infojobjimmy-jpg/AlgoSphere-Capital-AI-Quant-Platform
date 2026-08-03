"""Server-side AISStream client.

The API key stays in the ingestion container. Only normalized vessel positions
are published to Kafka and the browser.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

import websockets

from app.config import settings

logger = logging.getLogger(__name__)

AISSTREAM_URL = "wss://stream.aisstream.io/v0/stream"
Publish = Callable[[list[dict[str, Any]]], Awaitable[None]]

def vessel_country(mmsi: str) -> str:
    mid = str(mmsi)[:3]
    if mid == "316":
        return "Canada"
    if mid in {"303", "338", "366", "367", "368", "369"}:
        return "USA"
    return "Other"


def vessel_class(code: Any) -> str:
    try:
        value = int(code)
    except (TypeError, ValueError):
        return "other"
    if value == 30:
        return "fishing"
    if value in {31, 32}:
        return "towing"
    if value in {35, 55, 56, 57, 59}:
        return "government"
    if value == 36:
        return "sailing"
    if value == 37:
        return "pleasure"
    if 40 <= value <= 49:
        return "highspeed"
    if value in {50, 53, 54, 58}:
        return "service"
    if value == 51:
        return "government"
    if value == 52:
        return "tug"
    if 60 <= value <= 69:
        return "passenger"
    if 70 <= value <= 79:
        return "cargo"
    if 80 <= value <= 89:
        return "tanker"
    return "other"


def normalize_message(message: dict[str, Any], known: dict[str, Any] | None = None) -> dict[str, Any] | None:
    # AISStream uses ``MetaData`` (capital D). Keep the older spelling for
    # compatibility with recorded fixtures and previously stored messages.
    metadata = message.get("MetaData") or message.get("Metadata") or {}
    lat = metadata.get("latitude", metadata.get("Latitude"))
    lon = metadata.get("longitude", metadata.get("Longitude"))
    mmsi = metadata.get("MMSI", metadata.get("mmsi"))
    if lat is None or lon is None or mmsi is None:
        return None
    try:
        lat_f, lon_f = float(lat), float(lon)
    except (TypeError, ValueError):
        return None
    if not (-90 <= lat_f <= 90 and -180 <= lon_f <= 180):
        return None

    body = message.get("Message") or {}
    report = next((v for v in body.values() if isinstance(v, dict)), {})
    ship_type = report.get("Type", report.get("TypeAndCargo"))
    nav_status_raw = report.get("NavigationStatus")
    details = dict(known or {})
    if ship_type is not None:
        details["ship_type_code"] = ship_type
        details["ship_class"] = vessel_class(ship_type)
    nav_status = int(nav_status_raw) if nav_status_raw is not None else None
    return {
        "id": f"mmsi_{mmsi}",
        "mmsi": str(mmsi),
        "label": str(metadata.get("ShipName") or metadata.get("ship_name") or mmsi).strip(),
        "lat": lat_f,
        "lon": lon_f,
        "sog_kn": report.get("Sog", report.get("SpeedOverGround")),
        "cog_deg": report.get("Cog", report.get("CourseOverGround")),
        "heading_deg": report.get("TrueHeading"),
        "nav_status": nav_status,
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "ingest_type": "ship",
        "source": "aisstream",
        "country": vessel_country(str(mmsi)),
        **details,
    }


async def run_ais_stream(publish: Publish) -> None:
    """Continuously stream verified AIS positions, reconnecting with backoff."""
    if not settings.aisstream_api_key:
        logger.warning("AISSTREAM_API_KEY is not configured; ship layer is unavailable")
        await publish([])
        while True:
            await asyncio.sleep(300)

    latest: dict[str, dict[str, Any]] = {}
    vessel_details: dict[str, dict[str, Any]] = {}
    delay = 2.0
    while True:
        try:
            async with websockets.connect(
                AISSTREAM_URL,
                open_timeout=20,
                ping_interval=20,
                ping_timeout=20,
                max_queue=4096,
            ) as websocket:
                await websocket.send(
                    json.dumps(
                        {
                            "APIKey": settings.aisstream_api_key,
                            # Launch region: Canada and the continental United
                            # States, including Alaska and adjacent waters.
                            "BoundingBoxes": [[[15, -170], [75, -50]]],
                            "FilterMessageTypes": [
                                "PositionReport",
                                "StandardClassBPositionReport",
                                "ExtendedClassBPositionReport",
                                "ShipStaticData",
                                "StaticDataReport",
                            ],
                        }
                    )
                )
                delay = 2.0
                last_publish = asyncio.get_running_loop().time()
                async for raw in websocket:
                    message = json.loads(raw)
                    metadata = message.get("MetaData") or message.get("Metadata") or {}
                    mmsi = str(metadata.get("MMSI", metadata.get("mmsi", "")))
                    body = message.get("Message") or {}
                    report = next((v for v in body.values() if isinstance(v, dict)), {})
                    ship_type = report.get("Type", report.get("TypeAndCargo"))
                    if mmsi and ship_type is not None:
                        vessel_details[mmsi] = {
                            "ship_type_code": ship_type,
                            "ship_class": vessel_class(ship_type),
                            "destination": report.get("Destination"),
                        }
                        existing = latest.get(f"mmsi_{mmsi}")
                        if existing:
                            existing.update(vessel_details[mmsi])
                    row = normalize_message(message, vessel_details.get(mmsi))
                    if row:
                        latest[row["id"]] = row
                    now = asyncio.get_running_loop().time()
                    if now - last_publish >= 5:
                        # Keep each Kafka message comfortably below the broker's
                        # 1 MB limit while retaining a dense regional picture.
                        await publish(list(latest.values())[-2500:])
                        last_publish = now
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("AIS stream disconnected; retrying in %.0fs", delay)
            await publish([])
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60.0)
