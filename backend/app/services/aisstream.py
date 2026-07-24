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


def normalize_message(message: dict[str, Any]) -> dict[str, Any] | None:
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
    return {
        "id": f"mmsi_{mmsi}",
        "mmsi": str(mmsi),
        "label": str(metadata.get("ShipName") or metadata.get("ship_name") or mmsi).strip(),
        "lat": lat_f,
        "lon": lon_f,
        "sog_kn": report.get("Sog", report.get("SpeedOverGround")),
        "cog_deg": report.get("Cog", report.get("CourseOverGround")),
        "heading_deg": report.get("TrueHeading"),
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "ingest_type": "ship",
        "source": "aisstream",
    }


async def run_ais_stream(publish: Publish) -> None:
    """Continuously stream verified AIS positions, reconnecting with backoff."""
    if not settings.aisstream_api_key:
        logger.warning("AISSTREAM_API_KEY is not configured; ship layer is unavailable")
        await publish([])
        while True:
            await asyncio.sleep(300)

    latest: dict[str, dict[str, Any]] = {}
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
                            ],
                        }
                    )
                )
                delay = 2.0
                last_publish = asyncio.get_running_loop().time()
                async for raw in websocket:
                    row = normalize_message(json.loads(raw))
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
