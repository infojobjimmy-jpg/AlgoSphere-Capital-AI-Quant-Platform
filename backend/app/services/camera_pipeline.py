from __future__ import annotations

from datetime import datetime, timezone
import logging
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.services.camera_store import bootstrap_from_json, list_enabled_cameras
from app.services.windy_webcams import fetch_windy_webcams

logger = logging.getLogger(__name__)


async def fetch_cameras_for_telemetry(session: AsyncSession) -> list[dict[str, Any]]:
    if settings.windy_webcams_api_key:
        try:
            return await fetch_windy_webcams()
        except Exception:
            logger.exception("Windy Webcams unavailable; falling back to approved manual registry")
    await bootstrap_from_json(session)
    rows = await list_enabled_cameras(session)
    ts = datetime.now(timezone.utc).isoformat()
    out: list[dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "id": r.slug,
                "label": r.name,
                "lat": r.lat,
                "lon": r.lon,
                "stream_url": r.stream_url,
                "info_url": r.info_url,
                "stream_category": r.stream_category,
                "health_state": r.health_state,
                "probe_latency_ms": r.probe_latency_ms,
                "http_status": r.http_status,
                "classification_confidence": r.classification_confidence,
                "jurisdiction": r.jurisdiction,
                "observed_at": ts,
                "ingest_type": "camera",
            }
        )
    return out
