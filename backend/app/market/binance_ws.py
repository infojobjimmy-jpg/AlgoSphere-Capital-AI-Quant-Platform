"""Binance combined WebSocket trade stream → market_tick rows (async, reconnecting)."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable, Coroutine
from datetime import datetime, timezone
from typing import Any

import websockets
from websockets.exceptions import WebSocketException

from app.market.schema import market_tick

logger = logging.getLogger(__name__)

BINANCE_WS_URL = (
    "wss://stream.binance.com:9443/stream?"
    "streams=btcusdt@trade/ethusdt@trade/solusdt@trade"
)

_PAIR_TO_SYMBOL: dict[str, str] = {
    "BTCUSDT": "BTC",
    "ETHUSDT": "ETH",
    "SOLUSDT": "SOL",
}

_INITIAL_BACKOFF_SEC = 2.0
_MAX_BACKOFF_SEC = 60.0


async def run_binance_ws_stream(
    push_market_crypto: Callable[[list[dict[str, Any]]], Coroutine[Any, Any, None]],
) -> None:
    """
    Consume Binance combined trade stream; each trade becomes one market_tick list passed to push_market_crypto.
    Reconnects with exponential backoff on failure. Intended to be scheduled alongside REST ingestion.
    """
    backoff = _INITIAL_BACKOFF_SEC
    while True:
        try:
            async with websockets.connect(
                BINANCE_WS_URL,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=10,
            ) as ws:
                logger.info("BINANCE WS CONNECTED")
                backoff = _INITIAL_BACKOFF_SEC
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except (json.JSONDecodeError, TypeError, ValueError):
                        continue
                    data = msg.get("data")
                    if not isinstance(data, dict):
                        continue
                    pair = str(data.get("s", "")).upper()
                    if pair not in _PAIR_TO_SYMBOL:
                        continue
                    try:
                        price = float(data.get("p", 0.0))
                    except (TypeError, ValueError):
                        continue
                    if price <= 0.0:
                        continue
                    now = datetime.now(timezone.utc).isoformat()
                    tick = market_tick(
                        venue="binance_ws",
                        symbol=_PAIR_TO_SYMBOL[pair],
                        price=price,
                        asset_class="crypto",
                        ts_iso=now,
                        extra={"binance_pair": pair, "stream": "trade"},
                    )
                    logger.info("BINANCE WS TICK %s @ %s", _PAIR_TO_SYMBOL[pair], price)
                    await push_market_crypto([tick])
        except asyncio.CancelledError:
            raise
        except (WebSocketException, OSError) as e:
            logger.warning("BINANCE WS disconnected (%s); retry in %.1fs", type(e).__name__, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2.0, _MAX_BACKOFF_SEC)
        except Exception:
            logger.exception("BINANCE WS error; retry in %.1fs", backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2.0, _MAX_BACKOFF_SEC)
