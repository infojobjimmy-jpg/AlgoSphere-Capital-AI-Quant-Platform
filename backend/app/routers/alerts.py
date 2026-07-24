import json
from datetime import datetime, timezone

import redis.asyncio as redis
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db.models import Alert
from app.db.session import get_session

router = APIRouter()


class AlertCreate(BaseModel):
    severity: str
    title: str
    body: str | None = None
    payload: dict | None = None


@router.get("")
async def list_alerts(limit: int = 50, session: AsyncSession = Depends(get_session)):
    res = await session.execute(
        select(Alert).order_by(Alert.created_at.desc()).limit(limit)
    )
    rows = res.scalars().all()
    return [
        {
            "id": r.id,
            "severity": r.severity,
            "title": r.title,
            "body": r.body,
            "payload": r.payload,
            "created_at": r.created_at.isoformat(),
        }
        for r in rows
    ]


@router.post("")
async def create_alert(
    body: AlertCreate,
    session: AsyncSession = Depends(get_session),
):
    alert = Alert(
        severity=body.severity,
        title=body.title,
        body=body.body,
        payload=body.payload or {},
    )
    session.add(alert)
    await session.commit()
    await session.refresh(alert)

    client = redis.from_url(settings.redis_url, decode_responses=True)
    try:
        snap_raw = await client.get(settings.redis_snapshot_key())
        snap = json.loads(snap_raw) if snap_raw else {"meta": {}, "layers": {}, "alerts": []}
        snap.setdefault("alerts", []).insert(
            0,
            {
                "id": alert.id,
                "severity": alert.severity,
                "title": alert.title,
                "body": alert.body,
                "created_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        snap["alerts"] = snap["alerts"][:200]
        await client.set(settings.redis_snapshot_key(), json.dumps(snap))
    finally:
        await client.aclose()

    return {"id": alert.id}
