"""Finnhub equity quote (requires FINNHUB_API_KEY)."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import httpx

from app.config import settings
from app.market.schema import market_tick

logger = logging.getLogger(__name__)


async def fetch_equity_sample() -> list[dict[str, Any]]:
    key = settings.finnhub_api_key
    if not key:
        return []
    sym = "SPY"
    url = f"https://finnhub.io/api/v1/quote?symbol={sym}&token={key}"
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            r = await client.get(url)
            r.raise_for_status()
            row = r.json()
        price = row.get("c")
        if price is None:
            return []
        now = datetime.now(timezone.utc).isoformat()
        return [
            market_tick(
                venue="finnhub",
                symbol=sym,
                price=float(price),
                asset_class="equity",
                ts_iso=now,
                extra={"t": row.get("t")},
            )
        ]
    except Exception:
        logger.exception("finnhub fetch failed")
        return []
