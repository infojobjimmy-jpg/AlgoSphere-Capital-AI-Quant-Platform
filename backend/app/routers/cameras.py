from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import CameraStream
from app.db.session import get_session
from app.services.camera_pipeline import fetch_cameras_for_telemetry
from app.services.camera_store import bootstrap_from_json

router = APIRouter()


class CameraRegister(BaseModel):
    slug: str = Field(..., min_length=2, max_length=120)
    name: str = Field(..., min_length=2, max_length=500)
    stream_url: str | None = None
    info_url: str | None = None
    lat: float | None = None
    lon: float | None = None
    jurisdiction: str | None = None


@router.get("")
async def list_cameras(session: AsyncSession = Depends(get_session)):
    items = await fetch_cameras_for_telemetry(session)
    return {"cameras": items}


@router.post("/bootstrap")
async def bootstrap(session: AsyncSession = Depends(get_session)):
    n = await bootstrap_from_json(session)
    return {"inserted_or_updated": n}


@router.post("/register")
async def register(body: CameraRegister, session: AsyncSession = Depends(get_session)):
    stmt = (
        insert(CameraStream)
        .values(
            slug=body.slug,
            name=body.name,
            stream_url=body.stream_url,
            info_url=body.info_url,
            lat=body.lat,
            lon=body.lon,
            jurisdiction=body.jurisdiction,
            stream_category="unknown",
            health_state="unknown",
            extra_metadata={"registered": True},
        )
        .on_conflict_do_update(
            index_elements=[CameraStream.slug],
            set_={
                "name": body.name,
                "stream_url": body.stream_url,
                "info_url": body.info_url,
                "lat": body.lat,
                "lon": body.lon,
                "jurisdiction": body.jurisdiction,
                "updated_at": func.now(),
            },
        )
    )
    await session.execute(stmt)
    await session.commit()
    return {"status": "ok", "slug": body.slug}
