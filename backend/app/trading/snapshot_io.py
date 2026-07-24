"""Assembles full Market Hub / Trading Lab payload for WebSocket (read-only, same sources as REST)."""

from __future__ import annotations

import json
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.services.market_live import build_market_hub_live_payload
from app.trading.hub_metrics import (
    fetch_equity_series_snapshot,
    fetch_latest_marks,
    fetch_market_tick_pulse,
    fetch_portfolio_curve,
    fetch_symbol_universe,
    leg_pnl_payload,
)
from app.trading.lab_intel import regime_from_signals, risk_intel
from app.trading.paper_exec import load_paper_state
from app.trading.redis_ledger import ledger_equity_peak, load_ledger_positions


async def assemble_trading_lab_ws_payload(redis_client: Any, session: AsyncSession) -> dict[str, Any]:
    """Flat dict matching frontend tradingStore patch fields."""
    lim_sig = 60
    lim_ord = 40
    res_s = await session.execute(
        text(
            """
            SELECT time, symbol, side, strength, rationale
            FROM trading_signals
            ORDER BY time DESC
            LIMIT :lim
            """
        ),
        {"lim": lim_sig},
    )
    signals = [
        {
            "time": r["time"].isoformat(),
            "symbol": r["symbol"],
            "side": r["side"],
            "strength": float(r["strength"]),
            "rationale": r["rationale"],
        }
        for r in res_s.mappings().all()
    ]

    mode_raw = await redis_client.get(settings.redis_trading_mode_key())
    mode = (mode_raw or settings.trading_mode).strip().lower()
    table = "live_orders" if mode == "live" else "paper_orders"
    res_o = await session.execute(
        text(
            f"""
            SELECT time, symbol, side, qty, entry_price, stop_price, notional_usd, status
            FROM {table}
            ORDER BY time DESC
            LIMIT :lim
            """
        ),
        {"lim": lim_ord},
    )
    orders = [
        {
            "time": r["time"].isoformat(),
            "symbol": r["symbol"],
            "side": r["side"],
            "qty": float(r["qty"]),
            "entry_price": float(r["entry_price"]),
            "stop_price": float(r["stop_price"]),
            "notional_usd": float(r["notional_usd"]),
            "status": r["status"],
        }
        for r in res_o.mappings().all()
    ]

    positions = await load_ledger_positions(redis_client)
    raw_live = await redis_client.get(settings.redis_live_positions_key())
    live_lp = json.loads(raw_live) if raw_live else []
    live_positions = live_lp[:80] if mode == "live" else []

    st = await load_paper_state(redis_client)
    raw_recent = await redis_client.get(f"{settings.app_slug}:paper:recent")
    recent = json.loads(raw_recent) if raw_recent else []
    kill = await redis_client.get(settings.redis_trading_kill_key())
    ks = kill == "1" or bool(settings.trading_kill_switch)
    mode_pf = (await redis_client.get(settings.redis_trading_mode_key())) or settings.trading_mode
    raw_live_positions = await redis_client.get(settings.redis_live_positions_key())
    live_positions_pf = json.loads(raw_live_positions) if raw_live_positions else []
    raw_live_account = await redis_client.get(settings.redis_live_account_key())
    live_account = json.loads(raw_live_account) if raw_live_account else {}
    portfolio: dict[str, Any] = {
        "mode": mode_pf,
        "paper": st,
        "live_account": live_account,
        "live_positions": live_positions_pf[:80],
        "recent_orders": recent[:40],
        "kill_switch": ks,
    }

    kill_r = await redis_client.get(settings.redis_trading_kill_key())
    ks_r = kill_r == "1" or bool(settings.trading_kill_switch)
    mode_r = (mode_raw or settings.trading_mode).strip().lower()

    equity, peak, _ = await ledger_equity_peak(redis_client)
    pos_list = [p for p in positions if isinstance(p, dict)]
    dd = 100.0 * (1.0 - equity / max(peak, 1.0))
    ri = risk_intel(
        equity=equity,
        peak=peak,
        kill_switch=ks_r,
        mode=mode_r,
        open_positions=len(pos_list),
    )
    res_r = await session.execute(
        text(
            """
            SELECT time, symbol, side, strength, rationale
            FROM trading_signals
            ORDER BY time DESC
            LIMIT 24
            """
        ),
    )
    sig_rows = res_r.mappings().all()
    sigs_r = [
        {
            "time": r["time"].isoformat(),
            "symbol": r["symbol"],
            "side": r["side"],
            "strength": float(r["strength"]),
            "rationale": r["rationale"],
        }
        for r in sig_rows
    ]
    risk: dict[str, Any] = {
        "mode": mode_r,
        "kill_switch": ks_r,
        "equity": equity,
        "peak": peak,
        "drawdown_pct": round(dd, 4),
        "thresholds": {
            "global_drawdown_kill_pct": float(settings.trading_global_drawdown_kill_pct),
            "emergency_stop_dd_pct": float(settings.trading_emergency_stop_dd_pct),
            "max_risk_per_trade_pct": float(settings.trading_max_risk_per_trade_pct),
            "capital_floor_pct": float(settings.trading_capital_floor_pct),
        },
        "risk_intel": ri,
        "regime": regime_from_signals(sigs_r),
    }

    ticks = [
        {"t": s["time"], "sym": s["symbol"], "side": s["side"], "st": s["strength"]}
        for s in signals[:24]
    ]

    market_hub = await build_market_hub_live_payload(redis_client)
    marks = await fetch_latest_marks(session)
    hub_metrics = {
        "portfolio_curve": await fetch_portfolio_curve(session),
        "leg_pnl": leg_pnl_payload(pos_list, marks),
        "market_tick_pulse": await fetch_market_tick_pulse(session),
        "symbol_universe": await fetch_symbol_universe(session),
    }
    equity_series = await fetch_equity_series_snapshot(session, redis_client, limit=500)

    return {
        "signals": signals,
        "orders": orders,
        "positions": positions,
        "livePositions": live_positions,
        "risk": risk,
        "portfolio": portfolio,
        "ticks": ticks,
        "mode": "live" if mode == "live" else "paper",
        "marketHub": market_hub,
        "equitySeries": equity_series,
        "hubMetrics": hub_metrics,
    }
