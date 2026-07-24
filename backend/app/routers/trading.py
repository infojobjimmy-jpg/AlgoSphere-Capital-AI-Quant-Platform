from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.ai.ai_trader import analyze_trading_advisory
from app.config import settings
from app.db.session import get_session
from app.services.market_live import build_market_hub_live_payload
from app.trading.hub_metrics import (
    fetch_equity_series_snapshot,
    fetch_latest_marks,
    fetch_market_tick_pulse,
    fetch_portfolio_curve,
    fetch_symbol_universe,
    leg_pnl_payload,
)
from app.trading.redis_ledger import ledger_equity_peak, load_ledger_positions
from app.trading.lab_intel import build_ai_payload, regime_from_signals, risk_intel
from app.trading.paper_exec import load_paper_state

router = APIRouter()


async def _require_admin_key(x_admin_key: str | None = Header(default=None)) -> None:
    expected = settings.trading_admin_api_key
    if not expected:
        raise HTTPException(status_code=503, detail="Admin key not configured — set TRADING_ADMIN_API_KEY env var")
    if not x_admin_key or x_admin_key != expected:
        raise HTTPException(status_code=403, detail="Forbidden")


@router.get("/portfolio")
async def portfolio() -> dict[str, object]:
    import redis.asyncio as redis

    client = redis.from_url(settings.redis_url, decode_responses=True)
    try:
        st = await load_paper_state(client)
        raw = await client.get(f"{settings.app_slug}:paper:recent")
        recent = json.loads(raw) if raw else []
        kill = await client.get(settings.redis_trading_kill_key())
        ks = kill == "1" or bool(settings.trading_kill_switch)
        mode = (await client.get(settings.redis_trading_mode_key())) or settings.trading_mode
        raw_live_positions = await client.get(settings.redis_live_positions_key())
        live_positions = json.loads(raw_live_positions) if raw_live_positions else []
        raw_live_account = await client.get(settings.redis_live_account_key())
        live_account = json.loads(raw_live_account) if raw_live_account else {}
        return {
            "mode": mode,
            "paper": st,
            "live_account": live_account,
            "live_positions": live_positions[:80],
            "recent_orders": recent[:40],
            "kill_switch": ks,
        }
    finally:
        await client.aclose()


@router.get("/signals")
async def list_signals(limit: int = 40, session: AsyncSession = Depends(get_session)):
    lim = max(1, min(limit, 200))
    res = await session.execute(
        text(
            """
            SELECT time, symbol, side, strength, rationale
            FROM trading_signals
            ORDER BY time DESC
            LIMIT :lim
            """
        ),
        {"lim": lim},
    )
    rows = res.mappings().all()
    return {
        "signals": [
            {
                "time": r["time"].isoformat(),
                "symbol": r["symbol"],
                "side": r["side"],
                "strength": float(r["strength"]),
                "rationale": r["rationale"],
            }
            for r in rows
        ]
    }


@router.get("/orders")
async def list_orders(limit: int = 40, session: AsyncSession = Depends(get_session)):
    lim = max(1, min(limit, 200))
    import redis.asyncio as redis

    client = redis.from_url(settings.redis_url, decode_responses=True)
    try:
        mode_raw = await client.get(settings.redis_trading_mode_key())
    finally:
        await client.aclose()
    mode = (mode_raw or settings.trading_mode).strip().lower()
    table = "live_orders" if mode == "live" else "paper_orders"
    sql = f"""
            SELECT time, symbol, side, qty, entry_price, stop_price, notional_usd, status
            FROM {table}
            ORDER BY time DESC
            LIMIT :lim
            """
    res = await session.execute(
        text(sql),
        {"lim": lim},
    )
    rows = res.mappings().all()
    return {
        "orders": [
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
            for r in rows
        ]
    }


@router.post("/kill-switch")
async def set_kill_switch(on: bool, _: None = Depends(_require_admin_key)) -> dict[str, bool]:
    import redis.asyncio as redis

    client = redis.from_url(settings.redis_url, decode_responses=True)
    try:
        await client.set(settings.redis_trading_kill_key(), "1" if on else "0")
    finally:
        await client.aclose()
    return {"trading_kill_switch": bool(on)}


@router.post("/mode")
async def set_mode(mode: str, _: None = Depends(_require_admin_key)) -> dict[str, str]:
    v = (mode or "").strip().lower()
    if v not in {"paper", "live"}:
        return {"mode": settings.trading_mode, "status": "invalid_mode"}
    import redis.asyncio as redis

    client = redis.from_url(settings.redis_url, decode_responses=True)
    try:
        await client.set(settings.redis_trading_mode_key(), v)
    finally:
        await client.aclose()
    return {"mode": v, "status": "ok"}


@router.get("/positions")
async def list_positions() -> dict[str, object]:
    import redis.asyncio as redis

    client = redis.from_url(settings.redis_url, decode_responses=True)
    try:
        mode_raw = await client.get(settings.redis_trading_mode_key())
        mode = (mode_raw or settings.trading_mode).strip().lower()
        positions = await load_ledger_positions(client)
        raw_live = await client.get(settings.redis_live_positions_key())
        live_positions = json.loads(raw_live) if raw_live else []
        return {
            "mode": mode,
            "positions": positions,
            "live_positions": live_positions[:80] if mode == "live" else [],
        }
    finally:
        await client.aclose()


@router.get("/hub-metrics")
async def hub_metrics(session: AsyncSession = Depends(get_session)) -> dict[str, object]:
    """Read-only: portfolio_snapshots curve, leg PnL (ledger + latest market_ticks marks), tick pulse."""
    import redis.asyncio as redis

    client = redis.from_url(settings.redis_url, decode_responses=True)
    try:
        positions = await load_ledger_positions(client)
        marks = await fetch_latest_marks(session)
        pos_list = [p for p in positions if isinstance(p, dict)]
        return {
            "portfolio_curve": await fetch_portfolio_curve(session),
            "leg_pnl": leg_pnl_payload(pos_list, marks),
            "market_tick_pulse": await fetch_market_tick_pulse(session),
            "symbol_universe": await fetch_symbol_universe(session),
        }
    finally:
        await client.aclose()


@router.get("/market-hub/live")
async def market_hub_live() -> dict[str, object]:
    """Aggregated live quotes (crypto/forex/equities) + broker bridge snapshot from Redis."""
    import redis.asyncio as redis

    client = redis.from_url(settings.redis_url, decode_responses=True)
    try:
        return await build_market_hub_live_payload(client)
    finally:
        await client.aclose()


@router.get("/equity-series")
async def equity_series(
    limit: int = 400,
    session: AsyncSession = Depends(get_session),
) -> dict[str, object]:
    """Chronological order notionals from the DB (real) plus current ledger equity (real).

    Full mark-to-market equity time series is not persisted server-side; the client may
    chart ``order_flow`` as executed-notional timeline alongside ``equity_now`` / ``peak``.
    """
    import redis.asyncio as redis

    lim = max(1, min(limit, 2000))
    client = redis.from_url(settings.redis_url, decode_responses=True)
    try:
        return await fetch_equity_series_snapshot(session, client, limit=lim)
    finally:
        await client.aclose()


@router.get("/ai-insights")
async def ai_insights(session: AsyncSession = Depends(get_session)) -> dict[str, object]:
    """Advisory-only ranked opportunities (no execution). Uses live positions, risk, signals, hub universe."""
    import redis.asyncio as redis

    client = redis.from_url(settings.redis_url, decode_responses=True)
    try:
        positions = await load_ledger_positions(client)
        pos_list = [p for p in positions if isinstance(p, dict)]
        marks = await fetch_latest_marks(session)
        leg = leg_pnl_payload(pos_list, marks)
        mode_raw = await client.get(settings.redis_trading_mode_key())
        mode = (mode_raw or settings.trading_mode).strip().lower()
        table = "live_orders" if mode == "live" else "paper_orders"
        res_p = await session.execute(
            text(
                f"""
                SELECT COUNT(*) AS c
                FROM {table}
                WHERE lower(status) IN ('open','pending','submitted','new','partial')
                """
            ),
        )
        pending_count = int(res_p.scalar() or 0)

        res_s = await session.execute(
            text(
                """
                SELECT time, symbol, side, strength, rationale
                FROM trading_signals
                ORDER BY time DESC
                LIMIT 60
                """
            ),
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

        equity, peak, _ = await ledger_equity_peak(client)
        dd = 100.0 * (1.0 - equity / max(peak, 1.0))
        sym_u = await fetch_symbol_universe(session)
        regime_row = regime_from_signals(signals)
        regime = str((regime_row or {}).get("regime") or "unknown")

        vol = float(
            min(
                1.0,
                max(0.0, 0.55 * (dd / 25.0) + 0.35 * min(1.0, len(signals) / 45.0) + 0.25 * min(1.0, pending_count / 8.0)),
            )
        )

        pnl_ctx: dict[str, object] = {
            "drawdown_pct": round(dd, 4),
            "aggregate_pnl_usd": leg.get("aggregate_pnl_usd"),
            "pending_orders": pending_count,
        }
        return analyze_trading_advisory(
            pos_list,
            pnl_ctx,
            signals,
            vol,
            regime,
            sym_u,
        )
    finally:
        await client.aclose()


@router.get("/risk")
async def trading_risk(session: AsyncSession = Depends(get_session)) -> dict[str, object]:
    import redis.asyncio as redis

    client = redis.from_url(settings.redis_url, decode_responses=True)
    try:
        kill = await client.get(settings.redis_trading_kill_key())
        ks = kill == "1" or bool(settings.trading_kill_switch)
        mode_raw = await client.get(settings.redis_trading_mode_key())
        mode = (mode_raw or settings.trading_mode).strip().lower()
        equity, peak, _ = await ledger_equity_peak(client)
        positions = await load_ledger_positions(client)
        dd = 100.0 * (1.0 - equity / max(peak, 1.0))
        ri = risk_intel(
            equity=equity,
            peak=peak,
            kill_switch=ks,
            mode=mode,
            open_positions=len([p for p in positions if isinstance(p, dict)]),
        )
        res_s = await session.execute(
            text(
                """
                SELECT time, symbol, side, strength, rationale
                FROM trading_signals
                ORDER BY time DESC
                LIMIT 24
                """
            ),
        )
        sigs = [
            {
                "time": r["time"].isoformat(),
                "symbol": r["symbol"],
                "side": r["side"],
                "strength": float(r["strength"]),
                "rationale": r["rationale"],
            }
            for r in res_s.mappings().all()
        ]
        return {
            "mode": mode,
            "kill_switch": ks,
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
            "regime": regime_from_signals(sigs),
        }
    finally:
        await client.aclose()


class TradingAiRequest(BaseModel):
    message: str = ""
    context: dict[str, Any] | None = Field(default=None)


@router.post("/ai")
async def trading_ai(
    body: TradingAiRequest,
    session: AsyncSession = Depends(get_session),
) -> dict[str, Any]:
    """Advisory-only: aggregates the same sources as GET trading endpoints (no execution)."""
    import redis.asyncio as redis

    client = redis.from_url(settings.redis_url, decode_responses=True)
    try:
        lim = 40
        res = await session.execute(
            text(
                """
                SELECT time, symbol, side, strength, rationale
                FROM trading_signals
                ORDER BY time DESC
                LIMIT :lim
                """
            ),
            {"lim": lim},
        )
        sig_rows = res.mappings().all()
        signals = [
            {
                "time": r["time"].isoformat(),
                "symbol": r["symbol"],
                "side": r["side"],
                "strength": float(r["strength"]),
                "rationale": r["rationale"],
            }
            for r in sig_rows
        ]

        mode_raw = await client.get(settings.redis_trading_mode_key())
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
            {"lim": lim},
        )
        orows = res_o.mappings().all()
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
            for r in orows
        ]

        positions = await load_ledger_positions(client)
        kill = await client.get(settings.redis_trading_kill_key())
        ks = kill == "1" or bool(settings.trading_kill_switch)
        equity, peak, _ = await ledger_equity_peak(client)
        dd = 100.0 * (1.0 - equity / max(peak, 1.0))
        risk_row = {
            "equity": equity,
            "peak": peak,
            "drawdown_pct": dd,
        }
        return build_ai_payload(
            message=body.message,
            signals=signals,
            orders=orders,
            positions=[p for p in positions if isinstance(p, dict)],
            risk_row=risk_row,
            kill_switch=ks,
            mode=mode,
            extra_context=body.context if isinstance(body.context, dict) else None,
        )
    finally:
        await client.aclose()
