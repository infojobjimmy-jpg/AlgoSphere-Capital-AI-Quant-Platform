from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db.models import CameraStream

logger = logging.getLogger(__name__)


def _slugify(s: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9_\-]+", "_", s.strip())[:120]
    return s or "camera"


def _merge_registry_with_defaults(file_cams: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep only explicitly approved camera entries; never create placeholder cameras."""
    seen: set[str] = set()
    merged: list[dict[str, Any]] = []
    for c in file_cams:
        cid = str(c.get("id", "")).strip()
        if cid and cid not in seen:
            merged.append(c)
            seen.add(cid)
    return merged


async def bootstrap_from_json(session: AsyncSession) -> int:
    path = Path(settings.webcam_registry_path)
    if not path.is_file():
        path = Path(__file__).resolve().parents[2] / "config" / "webcams.example.json"
    file_cams: list[dict[str, Any]] = []
    if path.is_file():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            file_cams = data.get("cameras") or []
        except Exception:
            logger.exception("approved webcam registry could not be parsed")
    else:
        logger.warning("approved webcam registry file not found at %s", path)
    cams = _merge_registry_with_defaults(file_cams)
    n = 0
    for c in cams:
        slug = _slugify(str(c.get("id", f"cam_{n}")))
        stream_url = c.get("stream_url")
        info_url = c.get("info_url")
        url = stream_url or info_url
        if not url:
            continue
        stmt = (
            insert(CameraStream)
            .values(
                slug=slug,
                name=str(c.get("label", slug))[:500],
                stream_url=str(stream_url)[:2000] if stream_url else None,
                info_url=str(info_url)[:2000] if info_url else None,
                lat=float(c["lat"]) if c.get("lat") is not None else None,
                lon=float(c["lon"]) if c.get("lon") is not None else None,
                jurisdiction=str(c.get("jurisdiction") or "")[:200] or None,
                stream_category="unknown",
                health_state="unknown",
                extra_metadata={"notes": c.get("notes"), "seed": True},
            )
            .on_conflict_do_update(
                index_elements=[CameraStream.slug],
                set_={
                    "name": str(c.get("label", slug))[:500],
                    "stream_url": str(stream_url)[:2000] if stream_url else None,
                    "info_url": str(info_url)[:2000] if info_url else None,
                    "lat": float(c["lat"]) if c.get("lat") is not None else None,
                    "lon": float(c["lon"]) if c.get("lon") is not None else None,
                    "updated_at": func.now(),
                },
            )
        )
        await session.execute(stmt)
        n += 1
    await session.commit()
    return n


async def list_enabled_cameras(session: AsyncSession) -> list[CameraStream]:
    res = await session.execute(
        select(CameraStream).where(CameraStream.enabled.is_(True)).order_by(CameraStream.slug.asc())
    )
    return list(res.scalars().all())


async def update_camera_probe(
    session: AsyncSession,
    slug: str,
    *,
    health_state: str,
    latency_ms: float | None,
    http_status: int | None,
    stream_category: str | None,
    confidence: float | None,
) -> None:
    await session.execute(
        text(
            """
            UPDATE camera_streams
            SET health_state = :hs,
                probe_latency_ms = :lat,
                http_status = :st,
                stream_category = COALESCE(:cat, stream_category),
                classification_confidence = COALESCE(:conf, classification_confidence),
                last_probe_at = NOW(),
                updated_at = NOW()
            WHERE slug = :slug
            """
        ),
        {
            "hs": health_state[:32],
            "lat": latency_ms,
            "st": http_status,
            "cat": stream_category,
            "conf": confidence,
            "slug": slug,
        },
    )
    await session.commit()
