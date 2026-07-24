from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.trading.risk import RiskManager

logger = logging.getLogger(__name__)


async def load_paper_state(r: Any) -> dict[str, float]:
    raw = await r.get(f"{settings.app_slug}:paper:state")
    if not raw:
        return {
            "equity": float(settings.paper_capital_usd),
            "cash": float(settings.paper_capital_usd),
            "peak": float(settings.paper_capital_usd),
        }
    try:
        d = json.loads(raw)
        return {
            "equity": float(d.get("equity", settings.paper_capital_usd)),
            "cash": float(d.get("cash", settings.paper_capital_usd)),
            "peak": float(d.get("peak", settings.paper_capital_usd)),
        }
    except Exception:
        return {
            "equity": float(settings.paper_capital_usd),
            "cash": float(settings.paper_capital_usd),
            "peak": float(settings.paper_capital_usd),
        }


async def save_paper_state(r: Any, state: dict[str, float]) -> None:
    await r.set(f"{settings.app_slug}:paper:state", json.dumps(state, separators=(",", ":")))


def choose_trade_signal(signals: list[dict[str, Any]]) -> dict[str, Any] | None:
    for s in sorted(signals, key=lambda z: -float(z.get("strength", 0.0))):
        px = (s.get("meta") or {}).get("price")
        if px is None:
            continue
        if float(s.get("strength", 0.0)) < 0.65:
            continue
        sym = str(s.get("symbol", ""))
        if not sym or ":" in sym:
            continue
        return s
    return None


async def persist_signal(session: AsyncSession, sig: dict[str, Any]) -> None:
    await session.execute(
        text(
            """
            INSERT INTO trading_signals (time, symbol, side, strength, rationale, meta)
            VALUES (NOW(), :sym, :side, :st, :rat, CAST(:m AS jsonb))
            """
        ),
        {
            "sym": str(sig.get("symbol", ""))[:64],
            "side": str(sig.get("side", ""))[:16],
            "st": float(sig.get("strength", 0.0)),
            "rat": str(sig.get("rationale", ""))[:2000],
            "m": json.dumps(sig.get("meta") or {}),
        },
    )
    await session.commit()


async def persist_order(session: AsyncSession, row: dict[str, Any]) -> None:
    await session.execute(
        text(
            """
            INSERT INTO paper_orders (time, symbol, side, qty, entry_price, stop_price, notional_usd, status, meta)
            VALUES (NOW(), :sym, :side, :qty, :ent, :stp, :not, :st, CAST(:m AS jsonb))
            """
        ),
        {
            "sym": row["symbol"][:64],
            "side": row["side"][:16],
            "qty": float(row["qty"]),
            "ent": float(row["entry"]),
            "stp": float(row["stop"]),
            "not": float(row["notional"]),
            "st": row.get("status", "open")[:24],
            "m": json.dumps(row.get("meta") or {}),
        },
    )
    await session.commit()


async def maybe_trade_from_signals(
    r: Any,
    session: AsyncSession,
    signals: list[dict[str, Any]],
) -> None:
    """Execute at most one conservative paper order per tick when a high-strength signal exists."""
    if not signals:
        return
    top = choose_trade_signal(signals)
    if top is None:
        return
    sym = str(top.get("symbol", ""))

    kill_raw = await r.get(settings.redis_trading_kill_key())
    kill_switch = kill_raw == "1" or bool(settings.trading_kill_switch)
    st = await load_paper_state(r)
    rm = RiskManager(
        equity_usd=st["equity"],
        cash_usd=st["cash"],
        peak_equity_usd=st["peak"],
        initial_capital_usd=float(settings.paper_capital_usd),
    )
    notional = min(st["equity"] * 0.005, st["cash"] * 0.5)
    ok, reason = rm.allow_order(notional_usd=notional, symbol=sym, kill_switch=kill_switch)
    if not ok:
        logger.info("paper trade skipped: %s", reason)
        return

    entry = float((top.get("meta") or {}).get("price") or 0.0)
    if entry <= 0:
        return
    qty = notional / entry
    side = "buy"
    stop = rm.stop_price(side=side, entry=entry)
    row = {
        "symbol": sym,
        "side": side,
        "qty": qty,
        "entry": entry,
        "stop": stop,
        "notional": notional,
        "status": "open",
        "meta": {"signal": top},
    }
    await persist_order(session, row)
    raw_recent = await r.get(f"{settings.app_slug}:paper:recent")
    try:
        recent = json.loads(raw_recent) if raw_recent else []
    except Exception:
        recent = []
    recent.insert(0, {"time": datetime.now(timezone.utc).isoformat(), **row})
    await r.set(f"{settings.app_slug}:paper:recent", json.dumps(recent[:80], separators=(",", ":")))
    st["cash"] = max(0.0, st["cash"] - notional)
    st["peak"] = max(st["peak"], st["equity"])
    await save_paper_state(r, st)
