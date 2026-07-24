from __future__ import annotations

from typing import Any

import httpx

from app.config import settings
from app.trading.brokers.base import BrokerError, ExecutionResult, OrderRequest


class MT5Adapter:
    """
    MT5 bridge adapter.
    Expects a secure bridge exposing:
      POST /orders
      GET  /positions
      GET  /account
    """

    def __init__(self) -> None:
        if not settings.mt5_bridge_url:
            raise BrokerError("mt5_bridge_url_not_configured")
        self.base_url = settings.mt5_bridge_url.rstrip("/")
        self.headers = (
            {"Authorization": f"Bearer {settings.mt5_api_token}"} if settings.mt5_api_token else {}
        )

    async def execute_order(self, req: OrderRequest) -> ExecutionResult:
        payload = {
            "symbol": req.symbol,
            "side": req.side,
            "qty": req.qty,
            "stop_price": req.stop_price,
            "notional_usd": req.notional_usd,
            "meta": req.meta,
        }
        async with httpx.AsyncClient(timeout=20.0) as client:
            res = await client.post(f"{self.base_url}/orders", json=payload, headers=self.headers)
            res.raise_for_status()
            row = res.json()
        return ExecutionResult(
            order_id=str(row.get("order_id", "unknown")),
            status=str(row.get("status", "submitted")),
            filled_price=float(row["filled_price"]) if row.get("filled_price") is not None else None,
            raw=row if isinstance(row, dict) else {"raw": row},
        )

    async def fetch_positions(self) -> list[dict[str, Any]]:
        async with httpx.AsyncClient(timeout=20.0) as client:
            res = await client.get(f"{self.base_url}/positions", headers=self.headers)
            res.raise_for_status()
            data = res.json()
        if isinstance(data, list):
            return [d for d in data if isinstance(d, dict)]
        return []

    async def fetch_account(self) -> dict[str, float]:
        async with httpx.AsyncClient(timeout=20.0) as client:
            res = await client.get(f"{self.base_url}/account", headers=self.headers)
            res.raise_for_status()
            data = res.json()
        if not isinstance(data, dict):
            return {}
        out: dict[str, float] = {}
        for k in ("equity", "cash", "balance", "free_margin"):
            if data.get(k) is not None:
                try:
                    out[k] = float(data[k])
                except (TypeError, ValueError):
                    continue
        return out
