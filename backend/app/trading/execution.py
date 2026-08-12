from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.services.news_guard import read_state as read_news_guard_state
from app.trading.brokers.base import BrokerError, OrderRequest
from app.trading.brokers.factory import get_live_broker_adapter
from app.trading.paper_exec import choose_trade_signal, load_paper_state, maybe_trade_from_signals
from app.trading.risk import RiskManager

logger = logging.getLogger("acap.trading.exec")


def _signal_side_to_order_side(side: str) -> str:
    s = (side or "").lower()
    if "short" in s or "sell" in s or "de_risk" in s:
        return "sell"
    return "buy"


async def _load_live_account_state(r: Any) -> dict[str, float]:
    raw = await r.get(settings.redis_live_account_key())
    if not raw:
        return {
            "equity": float(settings.paper_capital_usd),
            "cash": float(settings.paper_capital_usd),
            "peak": float(settings.paper_capital_usd),
        }
    try:
        d = json.loads(raw)
    except Exception:
        return {
            "equity": float(settings.paper_capital_usd),
            "cash": float(settings.paper_capital_usd),
            "peak": float(settings.paper_capital_usd),
        }
    equity = float(d.get("equity", settings.paper_capital_usd))
    cash = float(d.get("cash", d.get("balance", settings.paper_capital_usd)))
    peak = max(float(d.get("peak", settings.paper_capital_usd)), equity)
    return {"equity": equity, "cash": cash, "peak": peak}


async def _save_live_account_state(r: Any, state: dict[str, float]) -> None:
    await r.set(settings.redis_live_account_key(), json.dumps(state, separators=(",", ":")))


async def _persist_live_order(session: AsyncSession, row: dict[str, Any]) -> None:
    await session.execute(
        text(
            """
            INSERT INTO live_orders (time, broker, broker_order_id, symbol, side, qty, entry_price, stop_price, notional_usd, status, meta)
            VALUES (NOW(), :broker, :boid, :sym, :side, :qty, :ent, :stp, :not, :st, CAST(:m AS jsonb))
            """
        ),
        {
            "broker": str(row["broker"])[:24],
            "boid": str(row["broker_order_id"])[:120],
            "sym": str(row["symbol"])[:64],
            "side": str(row["side"])[:16],
            "qty": float(row["qty"]),
            "ent": float(row["entry"]),
            "stp": float(row["stop"]),
            "not": float(row["notional"]),
            "st": str(row.get("status", "submitted"))[:24],
            "m": json.dumps(row.get("meta") or {}),
        },
    )
    await session.commit()


async def _persist_portfolio_snapshot(session: AsyncSession, state: dict[str, float], mode: str) -> None:
    await session.execute(
        text(
            """
            INSERT INTO portfolio_snapshots (time, equity_usd, cash_usd, realized_pnl_usd, meta)
            VALUES (NOW(), :eq, :cash, :pnl, CAST(:m AS jsonb))
            """
        ),
        {
            "eq": float(state.get("equity", settings.paper_capital_usd)),
            "cash": float(state.get("cash", settings.paper_capital_usd)),
            "pnl": float(state.get("realized_pnl", 0.0)),
            "m": json.dumps({"mode": mode}),
        },
    )
    await session.commit()


async def _execute_live_from_signals(r: Any, session: AsyncSession, signals: list[dict[str, Any]]) -> None:
    top = choose_trade_signal(signals)
    if top is None:
        return
    symbol = str(top.get("symbol", ""))
    side = _signal_side_to_order_side(str(top.get("side", "")))
    entry = float((top.get("meta") or {}).get("price") or 0.0)
    if not symbol or entry <= 0:
        return

    guard = await read_news_guard_state(r, symbol=symbol)
    if guard.get("status") != "ON":
        logger.warning(
            "live trade blocked by News Guard: symbol=%s risk=%s reason=%s",
            symbol,
            guard.get("risk"),
            guard.get("reason", "economic_event"),
        )
        return

    kill_raw = await r.get(settings.redis_trading_kill_key())
    kill_switch = kill_raw == "1" or bool(settings.trading_kill_switch)
    account = await _load_live_account_state(r)
    rm = RiskManager(
        equity_usd=account["equity"],
        cash_usd=account["cash"],
        peak_equity_usd=account["peak"],
        initial_capital_usd=float(settings.paper_capital_usd),
    )
    notional = min(account["equity"] * 0.005, account["cash"] * 0.5)
    ok, reason = rm.allow_order(notional_usd=notional, symbol=symbol, kill_switch=kill_switch)
    if not ok:
        logger.info("live trade blocked: %s", reason)
        return

    qty = notional / entry
    stop = rm.stop_price(side=side, entry=entry)
    req = OrderRequest(symbol=symbol, side=side, qty=qty, stop_price=stop, notional_usd=notional, meta={"signal": top, "news_guard": guard})

    adapter = get_live_broker_adapter()
    result = await adapter.execute_order(req)
    row = {
        "broker": settings.trading_live_broker,
        "broker_order_id": result.order_id,
        "symbol": symbol,
        "side": side,
        "qty": qty,
        "entry": float(result.filled_price if result.filled_price is not None else entry),
        "stop": stop,
        "notional": notional,
        "status": result.status,
        "meta": {"signal": top, "news_guard": guard, "broker_result": result.raw},
    }
    await _persist_live_order(session, row)

    positions = await adapter.fetch_positions()
    await r.set(settings.redis_live_positions_key(), json.dumps(positions[:200], separators=(",", ":")))
    broker_acc = await adapter.fetch_account()
    if broker_acc:
        state = {
            "equity": float(broker_acc.get("equity", account["equity"])),
            "cash": float(broker_acc.get("cash", broker_acc.get("balance", account["cash"]))),
            "peak": max(float(account["peak"]), float(broker_acc.get("equity", account["equity"]))),
        }
        await _save_live_account_state(r, state)
        await _persist_portfolio_snapshot(session, state, "live")


async def refresh_live_state(r: Any, session: AsyncSession | None = None) -> None:
    mode_raw = await r.get(settings.redis_trading_mode_key())
    mode = (mode_raw or settings.trading_mode or "paper").strip().lower()
    if mode != "live":
        return
    adapter = get_live_broker_adapter()
    positions = await adapter.fetch_positions()
    await r.set(settings.redis_live_positions_key(), json.dumps(positions[:200], separators=(",", ":")))
    broker_acc = await adapter.fetch_account()
    if broker_acc:
        cur = await _load_live_account_state(r)
        state = {
            "equity": float(broker_acc.get("equity", cur["equity"])),
            "cash": float(broker_acc.get("cash", broker_acc.get("balance", cur["cash"]))),
            "peak": max(float(cur["peak"]), float(broker_acc.get("equity", cur["equity"]))),
        }
        await _save_live_account_state(r, state)
        if session is not None:
            await _persist_portfolio_snapshot(session, state, "live")


async def execute_from_signals(r: Any, session: AsyncSession, signals: list[dict[str, Any]]) -> None:
    mode_raw = await r.get(settings.redis_trading_mode_key())
    mode = (mode_raw or settings.trading_mode or "paper").strip().lower()
    if mode == "paper":
        await maybe_trade_from_signals(r, session, signals)
        st = await load_paper_state(r)
        await _persist_portfolio_snapshot(session, st, "paper")
        return
    if mode != "live":
        logger.warning("unknown TRADING_MODE=%s; defaulting to paper", mode)
        await maybe_trade_from_signals(r, session, signals)
        return
    try:
        await refresh_live_state(r, session)
        await _execute_live_from_signals(r, session, signals)
    except BrokerError:
        logger.exception("live execution misconfigured")
    except Exception:
        logger.exception("live execution failed")
