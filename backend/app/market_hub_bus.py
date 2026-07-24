"""Market Hub WebSocket: debounced fan-out + Redis pub/sub (bump channel + acap keyspace)."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from fastapi import WebSocket

from app.config import settings
from app.db.session import SessionLocal
from app.trading.snapshot_io import assemble_trading_lab_ws_payload

logger = logging.getLogger(__name__)

BUMP_CHANNEL = f"{settings.app_slug}:market_hub:bump"
KEYSPACE_PATTERN = "__keyspace@0__:acap:*"


class MarketHubBroadcaster:
    def __init__(self) -> None:
        self._clients: set[WebSocket] = set()
        self._client_lock = asyncio.Lock()
        self._debounce_lock = asyncio.Lock()
        self._flush_task: asyncio.Task[None] | None = None
        self._listener_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._listener_task is None or self._listener_task.done():
            self._listener_task = asyncio.create_task(self._redis_listener(), name="market_hub_redis_listener")

    async def stop(self) -> None:
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
            self._listener_task = None

    async def register(self, ws: WebSocket) -> None:
        async with self._client_lock:
            self._clients.add(ws)

    async def unregister(self, ws: WebSocket) -> None:
        async with self._client_lock:
            self._clients.discard(ws)

    def schedule_flush(self) -> None:
        if self._flush_task and not self._flush_task.done():
            return
        self._flush_task = asyncio.create_task(self._debounced_flush(), name="market_hub_flush")

    async def _debounced_flush(self) -> None:
        await asyncio.sleep(0.35)
        async with self._debounce_lock:
            try:
                async with SessionLocal() as session:
                    import redis.asyncio as redis

                    r = redis.from_url(settings.redis_url, decode_responses=True)
                    try:
                        payload = await assemble_trading_lab_ws_payload(r, session)
                    finally:
                        await r.aclose()
                msg = json.dumps({"type": "market_hub", **payload}, default=str)
                async with self._client_lock:
                    snapshot = list(self._clients)
                dead: list[WebSocket] = []
                for ws in snapshot:
                    try:
                        await ws.send_text(msg)
                    except Exception:
                        dead.append(ws)
                if dead:
                    async with self._client_lock:
                        for ws in dead:
                            self._clients.discard(ws)
            except Exception:
                logger.exception("market_hub flush failed")
            finally:
                self._flush_task = None

    async def send_immediate(self, ws: WebSocket) -> None:
        async with SessionLocal() as session:
            import redis.asyncio as redis

            r = redis.from_url(settings.redis_url, decode_responses=True)
            try:
                payload = await assemble_trading_lab_ws_payload(r, session)
            finally:
                await r.aclose()
        await ws.send_text(json.dumps({"type": "market_hub", **payload}, default=str))

    async def _redis_listener(self) -> None:
        import redis.asyncio as redis

        while True:
            client = redis.from_url(settings.redis_url, decode_responses=True)
            pubsub = client.pubsub()
            try:
                await pubsub.subscribe(BUMP_CHANNEL)
                await pubsub.psubscribe(KEYSPACE_PATTERN)
                logger.info(
                    "Market Hub Redis listener subscribed bump=%s pattern=%s",
                    BUMP_CHANNEL,
                    KEYSPACE_PATTERN,
                )
                while True:
                    msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=60.0)
                    if msg is None:
                        continue
                    mtype = msg.get("type")
                    if mtype in ("message", "pmessage"):
                        self.schedule_flush()
            except asyncio.CancelledError:
                try:
                    await pubsub.unsubscribe(BUMP_CHANNEL)
                    await pubsub.punsubscribe(KEYSPACE_PATTERN)
                except Exception:
                    pass
                try:
                    await pubsub.close()
                except Exception:
                    pass
                await client.aclose()
                raise
            except Exception:
                logger.exception("market_hub redis listener error; reconnecting in 5s")
                try:
                    await pubsub.close()
                except Exception:
                    pass
                try:
                    await client.aclose()
                except Exception:
                    pass
                await asyncio.sleep(5.0)


market_hub_broadcaster = MarketHubBroadcaster()
