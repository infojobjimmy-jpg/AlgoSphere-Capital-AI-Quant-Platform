"""Redis-backed paper ledger reads (shared by trading routes and Market Hub WS)."""

from __future__ import annotations

import json
from typing import Any

from app.config import settings
from app.trading.paper_exec import load_paper_state

LEDGER_KEY = f"{settings.app_slug}:trading_agent:paper_ledger"


async def load_ledger_positions(client: Any) -> list[dict[str, Any]]:
    raw = await client.get(LEDGER_KEY)
    if not raw:
        return []
    try:
        d = json.loads(raw)
        pos = d.get("positions")
        return pos if isinstance(pos, list) else []
    except Exception:
        return []


async def ledger_equity_peak(client: Any) -> tuple[float, float, float]:
    raw = await client.get(LEDGER_KEY)
    if not raw:
        st = await load_paper_state(client)
        eq = float(st.get("equity", settings.paper_capital_usd))
        pk = float(st.get("peak", eq))
        return eq, pk, pk
    try:
        d = json.loads(raw)
        eq = float(d.get("equity", settings.paper_capital_usd))
        pk = max(float(d.get("peak", eq)), 1.0)
        return eq, pk, pk
    except Exception:
        st = await load_paper_state(client)
        eq = float(st.get("equity", settings.paper_capital_usd))
        pk = float(st.get("peak", eq))
        return eq, pk, pk
