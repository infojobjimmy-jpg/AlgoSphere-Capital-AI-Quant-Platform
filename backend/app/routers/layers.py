import json

import redis.asyncio as redis
from fastapi import APIRouter

from app.config import settings

router = APIRouter()


@router.get("/snapshot")
async def snapshot() -> dict:
    client = redis.from_url(settings.redis_url, decode_responses=True)
    try:
        raw = await client.get(settings.redis_snapshot_key())
        if not raw:
            return {"meta": {}, "layers": {}, "alerts": []}
        return json.loads(raw)
    finally:
        await client.aclose()
