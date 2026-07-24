import asyncio
import gzip
import json
import logging
import secrets
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from prometheus_fastapi_instrumentator import Instrumentator

from app.config import settings
from app.db.session import ensure_database
from app.routers import account, alerts, auth, cameras, decisions, discovery, goals, health, layers, metrics as metrics_router
from app.routers import navigation, self_code, system, trading
from app.market_hub_bus import market_hub_broadcaster
from app.websocket_manager import ConnectionManager

logging.basicConfig(level=getattr(logging, settings.log_level.upper(), logging.INFO))
logger = logging.getLogger("acap")

manager = ConnectionManager()


async def redis_snapshot_poller() -> None:
    import redis.asyncio as redis

    client = redis.from_url(settings.redis_url, decode_responses=True)
    try:
        while True:
            try:
                raw = await client.get(settings.redis_snapshot_key())
                if raw:
                    await manager.broadcast_snapshot(raw)
            except Exception:
                logger.exception("snapshot poller tick failed")
            await asyncio.sleep(1.0)
    finally:
        await client.aclose()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    await ensure_database()
    poller = asyncio.create_task(redis_snapshot_poller())
    await market_hub_broadcaster.start()
    yield
    await market_hub_broadcaster.stop()
    poller.cancel()
    try:
        await poller
    except asyncio.CancelledError:
        pass


app = FastAPI(title="Algosphere Capital API", version="1.0.0", lifespan=lifespan)
allowed_origins = [
    origin.strip()
    for origin in settings.cors_allowed_origins.split(",")
    if origin.strip()
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def protect_mutating_routes(request: Request, call_next):
    if request.method not in {"GET", "HEAD", "OPTIONS"}:
        public_mutations = {"/auth/license", "/auth/logout", "/account/preferences"}
        if request.url.path in public_mutations:
            return await call_next(request)
        expected = settings.api_admin_key
        supplied = request.headers.get("x-admin-key", "")
        if not expected:
            return JSONResponse(
                status_code=503,
                content={"detail": "API_ADMIN_KEY is required for mutating routes"},
            )
        if not secrets.compare_digest(supplied, expected):
            return JSONResponse(status_code=403, content={"detail": "Forbidden"})
    return await call_next(request)

Instrumentator().instrument(app).expose(app, include_in_schema=False)

app.include_router(health.router, prefix="/health", tags=["health"])
app.include_router(auth.router, prefix="/auth", tags=["auth"])
app.include_router(account.router, prefix="/account", tags=["account"])
app.include_router(navigation.router, prefix="/navigation", tags=["navigation"])
app.include_router(system.router, prefix="/system", tags=["system"])
app.include_router(layers.router, prefix="/layers", tags=["layers"])
app.include_router(alerts.router, prefix="/alerts", tags=["alerts"])
app.include_router(discovery.router, prefix="/discovery", tags=["discovery"])
app.include_router(cameras.router, prefix="/cameras", tags=["cameras"])
app.include_router(decisions.router, prefix="/decisions", tags=["decisions"])
app.include_router(goals.router, prefix="/goals", tags=["goals"])
app.include_router(metrics_router.router, prefix="/intel", tags=["intel"])
if not settings.public_geospatial_mode:
    app.include_router(trading.router, prefix="/trading", tags=["trading"])
    app.include_router(self_code.router, prefix="/self-code", tags=["self-code"])


@app.websocket("/ws/market-hub")
async def websocket_market_hub(ws: WebSocket) -> None:
    await ws.accept()
    await market_hub_broadcaster.register(ws)
    try:
        await market_hub_broadcaster.send_immediate(ws)
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        await market_hub_broadcaster.unregister(ws)


@app.websocket("/ws/live")
async def websocket_live(ws: WebSocket) -> None:
    await manager.connect(ws)
    try:
        import redis.asyncio as redis

        client = redis.from_url(settings.redis_url, decode_responses=True)
        snap = await client.get(settings.redis_snapshot_key())
        await client.aclose()
        if snap:
            if settings.ws_compress_snapshots:
                await ws.send_bytes(b"\x02" + gzip.compress(snap.encode("utf-8"), compresslevel=6))
            else:
                await ws.send_text(snap)
        while True:
            msg = await ws.receive_text()
            try:
                data = json.loads(msg)
            except json.JSONDecodeError:
                continue
            if data.get("type") == "hello":
                manager.set_compress(ws, bool(data.get("compress", True)))
                await ws.send_json({"type": "hello_ack", "compress": bool(data.get("compress", True))})
            if data.get("type") == "ping":
                await ws.send_json({"type": "pong"})
    except WebSocketDisconnect:
        manager.disconnect(ws)
