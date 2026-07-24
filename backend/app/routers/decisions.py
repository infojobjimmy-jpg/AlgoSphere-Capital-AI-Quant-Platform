from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_session

router = APIRouter()


@router.get("")
async def list_decisions(limit: int = 80, session: AsyncSession = Depends(get_session)):
    lim = max(1, min(limit, 300))
    res = await session.execute(
        text(
            """
            SELECT time, event_id, severity, action, confidence, reasoning
            FROM decisions
            ORDER BY time DESC
            LIMIT :lim
            """
        ),
        {"lim": lim},
    )
    rows = res.mappings().all()
    return {
        "decisions": [
            {
                "time": r["time"].isoformat(),
                "event_id": r["event_id"],
                "severity": r["severity"],
                "action": r["action"],
                "confidence": float(r["confidence"]),
                "reasoning": r["reasoning"],
            }
            for r in rows
        ]
    }
