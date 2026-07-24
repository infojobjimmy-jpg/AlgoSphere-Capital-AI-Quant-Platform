"""Read-only Market Hub metrics: DB snapshots, market_ticks marks, ledger leg PnL.

Does not execute trades or alter signals; safe to call from API routers.
"""

from __future__ import annotations

import math
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.trading.redis_ledger import ledger_equity_peak


def _norm_symbol(sym: str) -> str:
    return str(sym or "").strip().upper()


def _mark_keys_for_symbol(sym: str) -> list[str]:
    u = _norm_symbol(sym)
    if not u:
        return []
    keys = [u]
    for suf in ("USDT", "USD", "-USD"):
        if u.endswith(suf) and len(u) > len(suf):
            base = u[: -len(suf)]
            keys.append(base)
    if "/" in u:
        keys.append(u.replace("/", ""))
    return list(dict.fromkeys(keys))


def latest_price_by_symbol(rows: list[dict[str, Any]]) -> dict[str, float]:
    """Rows: symbol, price (first row per symbol wins if pre-sorted desc by time)."""
    out: dict[str, float] = {}
    for r in rows:
        s = _norm_symbol(str(r.get("symbol", "")))
        if not s or s in out:
            continue
        try:
            out[s] = float(r["price"])
        except (TypeError, ValueError):
            continue
    return out


def resolve_mark(sym: str, marks: dict[str, float]) -> float | None:
    for k in _mark_keys_for_symbol(sym):
        v = marks.get(k)
        if v is not None and v > 0 and not math.isnan(v):
            return float(v)
    return None


def leg_pnl_payload(positions: list[dict[str, Any]], marks: dict[str, float]) -> dict[str, Any]:
    legs: list[dict[str, Any]] = []
    total = 0.0
    worst_frac: float | None = None
    worst_sym: str | None = None
    missing_mark_open = False
    for p in positions:
        if not isinstance(p, dict):
            continue
        sym = str(p.get("symbol", ""))
        try:
            qty = float(p.get("qty", 0) or 0)
        except (TypeError, ValueError):
            qty = 0.0
        try:
            entry = float(p.get("entry", 0) or p.get("entry_price", 0) or 0)
        except (TypeError, ValueError):
            entry = 0.0
        notional = abs(qty) * entry if entry > 0 else 0.0
        mk = resolve_mark(sym, marks)
        pnl_usd: float | None = None
        pnl_frac: float | None = None
        if mk is not None and entry > 0 and qty != 0:
            pnl_usd = qty * (mk - entry)
            total += pnl_usd
            if notional > 1e-9:
                pnl_frac = pnl_usd / notional
                if worst_frac is None or pnl_frac < worst_frac:
                    worst_frac = pnl_frac
                    worst_sym = sym
        elif qty != 0 and entry > 0:
            missing_mark_open = True
        legs.append(
            {
                "symbol": sym,
                "qty": qty,
                "entry": entry,
                "mark": mk,
                "notional_usd": round(notional, 4) if notional else 0.0,
                "pnl_usd": None if pnl_usd is None else round(pnl_usd, 6),
                "pnl_frac": None if pnl_frac is None else round(pnl_frac, 8),
            }
        )
    return {
        "legs": legs,
        "aggregate_pnl_usd": round(total, 6),
        "aggregate_partial": missing_mark_open,
        "worst_leg_symbol": worst_sym,
        "worst_leg_pnl_frac": None if worst_frac is None else round(worst_frac, 8),
        "marks_source": "market_ticks",
    }


async def fetch_latest_marks(session: AsyncSession, *, tick_limit: int = 4000) -> dict[str, float]:
    lim = max(100, min(tick_limit, 20_000))
    res = await session.execute(
        text(
            """
            SELECT symbol, price, time
            FROM market_ticks
            ORDER BY time DESC
            LIMIT :lim
            """
        ),
        {"lim": lim},
    )
    rows = [dict(r) for r in res.mappings().all()]
    return latest_price_by_symbol(rows)


async def fetch_portfolio_curve(session: AsyncSession, limit: int = 800) -> list[dict[str, Any]]:
    lim = max(1, min(limit, 5000))
    res = await session.execute(
        text(
            """
            SELECT time, equity_usd, cash_usd, realized_pnl_usd
            FROM portfolio_snapshots
            ORDER BY time ASC
            LIMIT :lim
            """
        ),
        {"lim": lim},
    )
    out: list[dict[str, Any]] = []
    for r in res.mappings().all():
        out.append(
            {
                "time": r["time"].isoformat(),
                "equity_usd": float(r["equity_usd"]),
                "cash_usd": float(r["cash_usd"]),
                "realized_pnl_usd": float(r["realized_pnl_usd"]),
            }
        )
    return out


async def fetch_market_tick_pulse(session: AsyncSession, *, per_class: int = 12) -> dict[str, Any]:
    """Latest rows per asset_class for dashboard strip (live DB)."""
    n = max(1, min(per_class, 80))
    res = await session.execute(
        text(
            """
            SELECT id, time, venue, symbol, asset_class, price
            FROM (
              SELECT id, time, venue, symbol, asset_class, price,
                     ROW_NUMBER() OVER (PARTITION BY asset_class ORDER BY time DESC) AS rn
              FROM market_ticks
              WHERE time > NOW() - INTERVAL '48 hours'
            ) t
            WHERE rn <= :n
            ORDER BY asset_class, time DESC
            """
        ),
        {"n": n},
    )
    rows = res.mappings().all()
    by_class: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        ac = str(r["asset_class"] or "unknown")
        by_class.setdefault(ac, []).append(
            {
                "time": r["time"].isoformat(),
                "venue": r["venue"],
                "symbol": r["symbol"],
                "price": float(r["price"]),
            }
        )
    first_ts = rows[0]["time"].isoformat() if rows else None
    return {"by_asset_class": by_class, "ts": first_ts}


async def fetch_equity_series_snapshot(
    session: AsyncSession,
    client: Any,
    *,
    limit: int = 400,
) -> dict[str, Any]:
    """Same shape as GET /trading/equity-series (read-only)."""
    lim = max(1, min(limit, 2000))
    mode_raw = await client.get(settings.redis_trading_mode_key())
    mode = (mode_raw or settings.trading_mode).strip().lower()
    table = "live_orders" if mode == "live" else "paper_orders"
    equity, peak, _ = await ledger_equity_peak(client)
    res = await session.execute(
        text(
            f"""
            SELECT time, symbol, side, notional_usd, status
            FROM {table}
            ORDER BY time ASC
            LIMIT :lim
            """
        ),
        {"lim": lim},
    )
    rows = res.mappings().all()
    order_flow: list[dict[str, Any]] = []
    cum = 0.0
    for r in rows:
        n = float(r["notional_usd"])
        cum += n
        order_flow.append(
            {
                "time": r["time"].isoformat(),
                "symbol": r["symbol"],
                "side": r["side"],
                "notional_usd": n,
                "cumulative_notional_usd": round(cum, 4),
                "status": r["status"],
            }
        )
    return {
        "mode": mode,
        "equity_now": equity,
        "peak": peak,
        "order_flow": order_flow,
        "disclaimer": "order_flow reflects stored orders only; equity_now is the latest ledger/paper value.",
    }


async def fetch_symbol_universe(session: AsyncSession, *, limit_symbols: int = 120) -> list[dict[str, Any]]:
    """Latest price per symbol from market_ticks (live DB), no synthetic rows."""
    lim = max(1, min(limit_symbols, 500))
    res = await session.execute(
        text(
            """
            SELECT DISTINCT ON (symbol) symbol, asset_class, venue, price, time
            FROM market_ticks
            WHERE time > NOW() - INTERVAL '48 hours'
            ORDER BY symbol, time DESC
            LIMIT :lim
            """
        ),
        {"lim": lim},
    )
    out: list[dict[str, Any]] = []
    for r in res.mappings().all():
        out.append(
            {
                "symbol": str(r["symbol"]),
                "asset_class": str(r["asset_class"]),
                "venue": str(r["venue"]),
                "price": float(r["price"]),
                "time": r["time"].isoformat(),
            }
        )
    return out
