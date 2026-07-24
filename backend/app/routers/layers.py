import json

import redis.asyncio as redis
from fastapi import APIRouter, Request

from app.config import settings
from app.services.whop_auth import configured, current_member

router = APIRouter()


def public_preview(payload: dict) -> dict:
    layers = payload.get("layers") or {}
    preview_layers = {
        name: list(rows or [])[:12]
        for name, rows in layers.items()
        if name in {"aircraft", "ships", "satellites", "weather", "storms", "cameras"}
    }
    meta = dict(payload.get("meta") or {})
    meta["access"] = "preview"
    return {"meta": meta, "layers": preview_layers, "alerts": []}


@router.get("/snapshot")
async def snapshot(request: Request) -> dict:
    client = redis.from_url(settings.redis_url, decode_responses=True)
    try:
        raw = await client.get(settings.redis_snapshot_key())
        if not raw:
            return {"meta": {}, "layers": {}, "alerts": []}
        payload = json.loads(raw)
        if configured() and current_member(request) is None:
            return public_preview(payload)
        return payload
    finally:
        await client.aclose()
