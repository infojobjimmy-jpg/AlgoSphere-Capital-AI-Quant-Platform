from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


async def persist_decisions(session: AsyncSession, decisions: list[dict[str, Any]]) -> None:
    if not decisions:
        return
    now = datetime.now(timezone.utc)
    for d in decisions[:200]:
        await session.execute(
            text(
                """
                INSERT INTO decisions (time, event_id, severity, action, confidence, reasoning)
                VALUES (:time, :eid, :sev, :action, :conf, CAST(:reason AS jsonb))
                """
            ),
            {
                "time": now,
                "eid": str(d.get("event_id", "unknown"))[:128],
                "sev": str(d.get("severity", "low"))[:32],
                "action": str(d.get("action", "NOOP"))[:2000],
                "conf": float(d.get("confidence", 0.0)),
                "reason": json.dumps(
                    {
                        "rationale": d.get("rationale", ""),
                        "sources": d.get("sources", []),
                    }
                ),
            },
        )
    await session.commit()
