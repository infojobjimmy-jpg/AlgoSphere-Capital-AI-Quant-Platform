from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import json

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


async def persist_fusion_events(session: AsyncSession, events: list[dict[str, Any]]) -> None:
    if not events:
        return
    now = datetime.now(timezone.utc)
    for e in events[:200]:
        await session.execute(
            text(
                """
                INSERT INTO fusion_events (time, h3_cell, event_type, severity, summary, payload)
                VALUES (:time, :h3, :etype, :sev, :summary, CAST(:payload AS jsonb))
                """
            ),
            {
                "time": now,
                "h3": str(e.get("h3_cell", ""))[:32],
                "etype": str(e.get("event_type", "unknown"))[:64],
                "sev": str(e.get("severity", "info"))[:32],
                "summary": str(e.get("summary", ""))[:2000],
                "payload": json.dumps(e.get("payload") or {}),
            },
        )
    await session.commit()
