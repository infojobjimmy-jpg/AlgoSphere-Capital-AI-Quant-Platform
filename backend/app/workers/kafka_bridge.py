import asyncio
import json
import logging
from collections import deque

import numpy as np
import redis.asyncio as redis

from app.actions.dispatcher import dispatch_for_snap
from app.config import settings
from app.cortex.cycle import run_kafka_intel_tick
from app.meta_learning.evolution_agent import schedule_evolution_tick
from app.db.session import ensure_database
from app.kafka_bus import make_consumer, wait_for_kafka
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("acap.bridge")


async def main() -> None:
    await ensure_database()
    await wait_for_kafka()
    consumer = await make_consumer(
        settings.kafka_telemetry_topic,
        group_id=f"{settings.app_slug}-bridge",
    )
    r = redis.from_url(settings.redis_url, decode_responses=True)
    layers: dict[str, list] = {
        "aircraft": [],
        "satellites": [],
        "weather": [],
        "storms": [],
        "ships": [],
        "cameras": [],
        "transit": [],
        "market_crypto": [],
        "market_forex": [],
        "market_equities": [],
    }
    ac_hist: deque[int] = deque(maxlen=120)
    sh_hist: deque[int] = deque(maxlen=120)
    ewma_state: dict[str, float] = {}
    feat_history: list[np.ndarray] = []
    try:
        while True:
            try:
                msg = await consumer.getmany(timeout_ms=1000)
                for _, batch in msg.items():
                    for record in batch:
                        try:
                            payload = json.loads(record.value.decode("utf-8"))
                        except Exception:
                            continue
                        layer = payload.get("layer")
                        items = payload.get("items") or []
                        if layer in layers:
                            layers[layer] = items
                            if layer == "aircraft":
                                ac_hist.append(len(items))
                            if layer == "ships":
                                sh_hist.append(len(items))
            except Exception:
                logger.exception("kafka read failed")

            snap, ewma_state, feat_history = await run_kafka_intel_tick(
                r=r,
                layers=layers,
                ac_hist=ac_hist,
                sh_hist=sh_hist,
                ewma_state=ewma_state,
                feat_history=feat_history,
            )
            schedule_evolution_tick(r)

            raw = json.dumps(snap, separators=(",", ":"))
            await r.set(settings.redis_snapshot_key(), raw)
            await r.lpush(settings.redis_timeline_key(), raw)
            await r.ltrim(settings.redis_timeline_key(), 0, 299)
            await r.set(settings.redis_hist_aircraft_key(), json.dumps(list(ac_hist)))
            await r.set(settings.redis_hist_ships_key(), json.dumps(list(sh_hist)))

            try:
                await dispatch_for_snap(snap)
            except Exception:
                logger.exception("action dispatch failed")
    finally:
        await consumer.stop()
        await r.aclose()


if __name__ == "__main__":
    asyncio.run(main())
