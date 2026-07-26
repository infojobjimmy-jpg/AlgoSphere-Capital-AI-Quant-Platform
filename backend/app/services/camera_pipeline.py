from __future__ import annotations

from datetime import datetime, timezone
import logging
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.services.camera_store import bootstrap_from_json, list_enabled_cameras
from app.services.mtq_cameras import fetch_mtq_cameras
from app.services.windy_webcams import fetch_windy_webcams

logger = logging.getLogger(__name__)

# Module-level source status — read by snapshot enrichment
camera_source_status: dict[str, Any] = {
    "status": "unavailable",
    "count": 0,
    "provider": "none",
    "error_code": None,
    "updated_at": "",
}


async def fetch_cameras_for_telemetry(session: AsyncSession) -> list[dict[str, Any]]:
    global camera_source_status
    ts = datetime.now(timezone.utc).isoformat()

    # 1) MTQ official WFS — free, CC-BY 4.0, 675 cameras
    try:
        mtq = await fetch_mtq_cameras()
        if mtq:
            camera_source_status = {
                "status": "live",
                "count": len(mtq),
                "provider": "MTQ_WFS",
                "error_code": None,
                "updated_at": ts,
            }
            return mtq
        logger.warning("MTQ WFS returned 0 cameras")
    except Exception:
        logger.exception("MTQ WFS unavailable")

    # 2) Windy Webcams V3 — secondary, requires API key, 50-camera cap
    if settings.windy_webcams_api_key:
        try:
            windy = await fetch_windy_webcams()
            if windy:
                camera_source_status = {
                    "status": "degraded",
                    "count": len(windy),
                    "provider": "windy_webcams_v3",
                    "error_code": "MTQ_WFS_FAILED",
                    "updated_at": ts,
                }
                return windy
        except Exception:
            logger.exception("Windy Webcams unavailable; falling back to approved manual registry")

    # 3) DB registry — offline fallback
    try:
        await bootstrap_from_json(session)
        rows = await list_enabled_cameras(session)
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
        if out:
            camera_source_status = {
                "status": "degraded",
                "count": len(out),
                "provider": "manual_registry",
                "error_code": "ALL_LIVE_SOURCES_FAILED",
                "updated_at": ts,
            }
            return out
    except Exception:
        logger.exception("camera DB fallback failed")

    camera_source_status = {
        "status": "unavailable",
        "count": 0,
        "provider": "none",
        "error_code": "ALL_SOURCES_FAILED",
        "updated_at": ts,
    }
    return []
