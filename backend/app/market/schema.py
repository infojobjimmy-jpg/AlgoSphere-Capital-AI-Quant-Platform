from __future__ import annotations

from typing import Any


def market_tick(
    *,
    venue: str,
    symbol: str,
    price: float,
    asset_class: str,
    ts_iso: str,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "venue": venue[:64],
        "symbol": symbol[:32],
        "price": float(price),
        "asset_class": asset_class[:24],
        "ts": ts_iso,
        "observed_at": ts_iso,
        "ingest_type": "market",
        "meta": extra or {},
    }
