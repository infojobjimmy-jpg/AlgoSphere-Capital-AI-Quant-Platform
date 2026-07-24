from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


async def record_cycle(session: AsyncSession, snap: dict[str, Any]) -> None:
    """
    Time-series telemetry for model governance (threshold drift, anomaly rates).
    """
    try:
        events = snap.get("events") or []
        decisions = snap.get("decisions") or []
        alerts = snap.get("alerts") or []
        meta = snap.get("meta") or {}
        learn = meta.get("learning") or {}

        metrics = [
            ("fusion_engine", "events_emitted", float(len(events)), {}),
            ("strategic_agent", "decisions_emitted", float(len(decisions)), {}),
            ("watcher_stack", "alerts_active", float(len(alerts)), {}),
            ("adaptive", "air_density_z", float(learn.get("air_density_z", 0.0)), {}),
            ("adaptive", "iso_contamination", float(learn.get("iso_contamination", 0.0)), {}),
        ]
        now = datetime.now(timezone.utc)
        for model_name, metric_name, value, meta_obj in metrics:
            await session.execute(
                text(
                    """
                    INSERT INTO model_performance (time, model_name, metric_name, metric_value, meta)
                    VALUES (:time, :mn, :metric, :val, CAST(:meta AS jsonb))
                    """
                ),
                {
                    "time": now,
                    "mn": model_name[:120],
                    "metric": metric_name[:120],
                    "val": float(value),
                    "meta": json.dumps(meta_obj),
                },
            )
        await session.commit()
    except Exception:
        logger.exception("record_cycle metrics failed")
        await session.rollback()
