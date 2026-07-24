from __future__ import annotations

from typing import Any

import httpx

from app.config import settings
from app.trading.brokers.base import BrokerError, ExecutionResult, OrderRequest


class CTraderAdapter:
    """
    cTrader Open API adapter over HTTP gateway.
    Expects:
      POST /v1/orders
      GET  /v1/positions
      GET  /v1/account
    """

    def __init__(self) -> None:
        if not settings.ctrader_base_url:
            raise BrokerError("ctrader_base_url_not_configured")
        if not settings.ctrader_access_token:
            raise BrokerError("ctrader_access_token_not_configured")
        self.base_url = settings.ctrader_base_url.rstrip("/")
        self.headers = {"Authorization": f"Bearer {settings.ctrader_access_token}"}

    async def execute_order(self, req: OrderRequest) -> ExecutionResult:
        payload = {
            "account_id": settings.ctrader_account_id,
            "symbol": req.symbol,
            "side": req.side,
            "quantity": req.qty,
            "stop_loss": req.stop_price,
            "meta": req.meta,
        }
        async with httpx.AsyncClient(timeout=20.0) as client:
            res = await client.post(f"{self.base_url}/v1/orders", json=payload, headers=self.headers)
            res.raise_for_status()
            row = res.json()
        return ExecutionResult(
            order_id=str(row.get("order_id", "unknown")),
            status=str(row.get("status", "submitted")),
            filled_price=float(row["fill_price"]) if row.get("fill_price") is not None else None,
            raw=row if isinstance(row, dict) else {"raw": row},
        )

    async def fetch_positions(self) -> list[dict[str, Any]]:
        params = {"account_id": settings.ctrader_account_id} if settings.ctrader_account_id else None
        async with httpx.AsyncClient(timeout=20.0) as client:
            res = await client.get(f"{self.base_url}/v1/positions", params=params, headers=self.headers)
            res.raise_for_status()
            data = res.json()
        if isinstance(data, list):
            return [d for d in data if isinstance(d, dict)]
        return []

    async def fetch_account(self) -> dict[str, float]:
        params = {"account_id": settings.ctrader_account_id} if settings.ctrader_account_id else None
        async with httpx.AsyncClient(timeout=20.0) as client:
            res = await client.get(f"{self.base_url}/v1/account", params=params, headers=self.headers)
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
