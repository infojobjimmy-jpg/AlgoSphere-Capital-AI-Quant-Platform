from __future__ import annotations

import gzip
import logging
from typing import Any

from fastapi import WebSocket

from app.config import settings

logger = logging.getLogger(__name__)


class ConnectionManager:
    def __init__(self) -> None:
        self.active: dict[WebSocket, dict[str, Any]] = {}

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        self.active[ws] = {"compress": bool(settings.ws_compress_snapshots)}

    def disconnect(self, ws: WebSocket) -> None:
        self.active.pop(ws, None)

    def set_compress(self, ws: WebSocket, enabled: bool) -> None:
        meta = self.active.get(ws)
        if meta is not None:
            meta["compress"] = bool(enabled)

    async def broadcast_snapshot(self, raw: str) -> None:
        gz = gzip.compress(raw.encode("utf-8"), compresslevel=6)
        dead: list[WebSocket] = []
        for ws, meta in list(self.active.items()):
            try:
                if meta.get("compress", True):
                    await ws.send_bytes(b"\x02" + gz)
                else:
                    await ws.send_text(raw)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)
