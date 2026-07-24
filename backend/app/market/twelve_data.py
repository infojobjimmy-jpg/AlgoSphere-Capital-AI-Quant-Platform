"""Twelve Data forex / equity quotes (requires TWELVE_DATA_API_KEY)."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import httpx

from app.config import settings
from app.market.schema import market_tick

logger = logging.getLogger(__name__)


async def fetch_forex_sample() -> list[dict[str, Any]]:
    key = settings.twelve_data_api_key
    if not key:
        return []
    url = "https://api.twelvedata.com/quote"
    symbols = ["EUR/USD", "GBP/USD"]
    now = datetime.now(timezone.utc).isoformat()
    out: list[dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=20.0) as client:
        for sym in symbols:
            try:
                r = await client.get(url, params={"symbol": sym, "apikey": key})
                r.raise_for_status()
                row = r.json()
                price = row.get("close") or row.get("price")
                if price is None:
                    continue
                out.append(
                    market_tick(
                        venue="twelvedata",
                        symbol=sym.replace("/", ""),
                        price=float(price),
                        asset_class="forex",
                        ts_iso=now,
                        extra={"raw_symbol": sym},
                    )
                )
            except Exception:
                logger.debug("twelve_data skip %s", sym, exc_info=True)
    return out
