"""Windy Webcams API V3 adapter (server-side only)."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import httpx

from app.config import settings

logger = logging.getLogger(__name__)
WINDY_WEBCAMS_URL = "https://api.windy.com/webcams/api/v3/webcams"


async def fetch_windy_webcams(limit: int = 50) -> list[dict[str, Any]]:
    if not settings.windy_webcams_api_key:
        return []
    params = {
        # Windy Webcams V3 rejects list requests above 50 items.
        "limit": max(1, min(limit, 50)),
        "include": "images,location,player,urls",
    }
    headers = {"x-windy-api-key": settings.windy_webcams_api_key}
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        response = await client.get(WINDY_WEBCAMS_URL, params=params, headers=headers)
        response.raise_for_status()
        payload = response.json()

    rows = payload.get("webcams") or payload.get("result", {}).get("webcams") or []
    observed_at = datetime.now(timezone.utc).isoformat()
    out: list[dict[str, Any]] = []
    for row in rows:
        location = row.get("location") or {}
        lat = location.get("latitude", row.get("latitude"))
        lon = location.get("longitude", row.get("longitude"))
        if lat is None or lon is None:
            continue
        images = row.get("images") or {}
        current = images.get("current") or {}
        preview = current.get("preview") or current.get("thumbnail") or current.get("icon")
        urls = row.get("urls") or {}
        player = row.get("player") or {}
        webcam_id = str(row.get("webcamId") or row.get("id") or "")
        if not webcam_id:
            continue
        out.append(
            {
                "id": f"windy_{webcam_id}",
                "label": str(row.get("title") or location.get("city") or webcam_id),
                "lat": float(lat),
                "lon": float(lon),
                "stream_url": player.get("live") or player.get("day"),
                "preview_url": preview,
                "info_url": urls.get("detail") or row.get("url"),
                "stream_category": "webcam",
                "health_state": "available",
                "observed_at": observed_at,
                "ingest_type": "camera",
                "source": "windy_webcams_v3",
            }
        )
    return out
