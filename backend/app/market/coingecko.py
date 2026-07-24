"""CoinGecko public simple price (no API key required for low rate)."""

from __future__ import annotations

import logging
from typing import Any
from datetime import datetime, timezone

from app.market.schema import market_tick
from app.services.resilient_http import get_json

logger = logging.getLogger(__name__)

COINGECKO_SIMPLE = "https://api.coingecko.com/api/v3/simple/price"

_ID_TO_SYMBOL = {"bitcoin": "BTC", "ethereum": "ETH", "solana": "SOL"}


def _fallback_crypto_ticks() -> list[dict[str, Any]]:
    """Last-resort quotes so market_crypto is never empty."""
    now = datetime.now(timezone.utc).isoformat()
    return [
        market_tick(venue="fallback", symbol="BTC", price=65_000.0, asset_class="crypto", ts_iso=now, extra={"reason": "api_unavailable"}),
        market_tick(venue="fallback", symbol="ETH", price=3_200.0, asset_class="crypto", ts_iso=now, extra={"reason": "api_unavailable"}),
        market_tick(venue="fallback", symbol="SOL", price=140.0, asset_class="crypto", ts_iso=now, extra={"reason": "api_unavailable"}),
    ]


async def fetch_crypto_top() -> list[dict[str, Any]]:
    ids = "bitcoin,ethereum,solana"
    params = {"ids": ids, "vs_currencies": "usd"}
    try:
        data = await get_json(COINGECKO_SIMPLE, params=params, timeout=25.0)
    except Exception:
        logger.exception("coingecko fetch failed after retries; using placeholder ticks")
        return _fallback_crypto_ticks()
    now = datetime.now(timezone.utc).isoformat()
    out: list[dict[str, Any]] = []
    for cid, row in (data or {}).items():
        usd = row.get("usd")
        if usd is None:
            continue
        sym = _ID_TO_SYMBOL.get(str(cid).lower(), str(cid)[:12].upper())
        out.append(
            market_tick(
                venue="coingecko",
                symbol=sym,
                price=float(usd),
                asset_class="crypto",
                ts_iso=now,
                extra={"coingecko_id": cid},
            )
        )
    if not out:
        logger.warning("coingecko returned no rows; using placeholder ticks")
        return _fallback_crypto_ticks()
    return out
