"""Binance public ticker (read-only); maps USDT pairs to BTC/ETH/SOL market_tick rows."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from app.market.schema import market_tick
from app.services.resilient_http import get_json

logger = logging.getLogger(__name__)

BINANCE_TICKER_URL = "https://api.binance.com/api/v3/ticker/price"

_PAIR_TO_SYMBOL: dict[str, str] = {
    "BTCUSDT": "BTC",
    "ETHUSDT": "ETH",
    "SOLUSDT": "SOL",
}


async def fetch_crypto_top_binance() -> list[dict[str, Any]]:
    """Fetch BTC/ETH/SOL spot prices from Binance; returns [] on any failure."""
    now = datetime.now(timezone.utc).isoformat()
    try:
        symbols_json = json.dumps(list(_PAIR_TO_SYMBOL.keys()))
        data = await get_json(
            BINANCE_TICKER_URL,
            params={"symbols": symbols_json},
            timeout=20.0,
        )
    except Exception:
        logger.warning("BINANCE DATA fetch failed; returning empty list", exc_info=True)
        return []

    rows_in: list[dict[str, Any]]
    if isinstance(data, list):
        rows_in = [r for r in data if isinstance(r, dict)]
    elif isinstance(data, dict) and "symbol" in data and "price" in data:
        rows_in = [data]
    else:
        logger.warning("BINANCE DATA unexpected response shape; returning empty list")
        return []

    out: list[dict[str, Any]] = []
    for row in rows_in:
        pair = str(row.get("symbol", ""))
        if pair not in _PAIR_TO_SYMBOL:
            continue
        try:
            price = float(row.get("price", 0.0))
        except (TypeError, ValueError):
            continue
        if price <= 0:
            continue
        out.append(
            market_tick(
                venue="binance",
                symbol=_PAIR_TO_SYMBOL[pair],
                price=price,
                asset_class="crypto",
                ts_iso=now,
                extra={"binance_pair": pair},
            )
        )

    logger.info("BINANCE DATA")
    if not out:
        logger.warning("BINANCE DATA parsed 0 ticks from response")
    return out
