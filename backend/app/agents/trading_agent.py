"""
Autonomous paper trading agent: consumes telemetry, reuses cortex signal detection, simulates execution only.

Does not call live brokers. State is isolated in Redis under a trading-agent-specific key.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any

import redis.asyncio as redis

from app.agents import cortex_agent
from app.config import settings
from app.execution import execute_trade
from app.kafka_bus import make_consumer, wait_for_kafka
from app.trading.risk import RiskManager

logger = logging.getLogger("acap.trading_agent")

MODE = "paper"
TOPIC = settings.kafka_telemetry_topic
GROUP_ID = f"{settings.app_slug}-trading-agent"
LEDGER_KEY = f"{settings.app_slug}:trading_agent:paper_ledger"
# Container startup: TCP probe Kafka before subscribing (docker compose ordering).
KAFKA_STARTUP_MAX_ATTEMPTS = 30

MAX_OPEN_TRADES = 5
# Post-fill cooldown per symbol (institutional: avoid churn).
COOLDOWN_SEC = 90.0
AGENT_DRAWDOWN_REJECT_PCT = 10.0
# "soft_scale": allow orders under global DD kill with reduced size (agent + RiskManager).
AGENT_DRAWDOWN_MODE = "soft_scale"
# Floor for paper sizing after all scaling (never below this when cap allows).
MIN_NOTIONAL = 10.0
# Vol target for step-scaled qty only (not entry filters).
TARGET_VOL_FOR_SCALE = 0.01
# Minimum partial TP threshold floor (adaptive threshold = max(this, 2.5×volatility)).
MIN_PARTIAL_TP_FRAC = 0.006

# Rolling crypto prints per symbol (>=3 for momentum & trend windows).
PRICE_HISTORY_MAXLEN = 24
# Momentum-only path: 3-bar range vs first bar must clear this (responsive, not noise).
_MOMENTUM_STRONG_MOVE_PCT = 0.5
# Institutional gates (decision layer only; execution & RiskManager caps unchanged).
_INST_MOMENTUM_TICKS = 6
_INST_MOMENTUM_MIN_PCT = 0.12
# Fallback 3-bar path: skip if net move from oldest→newest of last 3 prints is below this (%).
_FALLBACK_MIN_NET_MOVE_PCT = 0.01
# Minimum last-tick move |p2-p1|/p1 as percent (filters flat 3rd bar).
_FALLBACK_LAST_TICK_MIN_PCT = 0.005
# Min (max-min)/min over last 10 prints; blocks flat markets before any candidate is returned (low-vol testing).
MIN_VOLATILITY = 0.00002
_VOL_WINDOW_TICKS = 8
_MIN_ROLLING_VOL_PCT = 0.035
_SHORT_MA = 3
_LONG_MA = 8
_TRAIL_FRAC = 0.012
_SIZE_CONF_MIN = 0.42
_SIZE_CONF_MAX = 1.0

_symbol_prices: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=PRICE_HISTORY_MAXLEN))
# Set only when returning a fallback candidate; read by _decision_confidence(["fallback"]).
_fallback_micro_confidence_slot: list[float | None] = [None]

_last_trade_mono: dict[str, float] = {}
# Latest Binance/telemetry prices by base symbol (e.g. BTC); survives non-crypto Kafka messages.
_live_crypto_marks: dict[str, float] = {}


def _norm_symbol(sym: str) -> str:
    """Base symbol for anti-hedge / dedupe (BTC vs BTCUSD vs BTC/USDT)."""
    s = str(sym or "").strip().upper().replace("-", "").replace("/", "")
    if s.endswith("USDT"):
        return s[:-4]
    if s.endswith("USD"):
        return s[:-3]
    return s


def _positions_sorted_by_opened_at(
    positions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    def _open_ts(p: dict[str, Any]) -> float:
        raw = p.get("opened_at")
        if not raw:
            return 0.0
        try:
            s = str(raw).replace("Z", "+00:00")
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except Exception:
            return 0.0

    return sorted(positions, key=_open_ts)


def _safe_qty(value: Any) -> float:
    try:
        v = float(value)
        if v != v:
            return 0.0
        return v
    except Exception:
        return 0.0


def _broker_symbol(sym: str) -> str:
    u = sym.upper().strip()
    if u in ("BTC", "BITCOIN"):
        return "BTCUSD"
    if u in ("ETH", "ETHEREUM"):
        return "ETHUSD"
    if u in ("SOL", "SOLANA"):
        return "SOLUSD"
    return f"{u}USD" if u and not u.endswith("USD") else u or "BTCUSD"


def _prices_from_items(items: list[Any]) -> dict[str, float]:
    out: dict[str, float] = {}
    for row in items:
        if not isinstance(row, dict):
            continue
        sym = str(row.get("symbol") or "").strip().upper()
        if not sym:
            continue
        try:
            px = float(row.get("price"))
        except (TypeError, ValueError):
            continue
        if px > 0:
            out[sym] = px
    return out


def _mark_for_symbol(marks: dict[str, float], symbol: str) -> float | None:
    """Resolve live mark for BTC, BTCUSD, etc."""
    s = symbol.strip().upper()
    if not s:
        return None
    if s in marks:
        return float(marks[s])
    key = s.replace("USD", "")
    if key in marks:
        return float(marks[key])
    return None


def _dominant_mover(
    items: list[Any],
    prices_before: dict[str, float],
) -> tuple[str, str, float]:
    """Return (symbol, BUY|SELL, move_pct) for largest absolute move vs prices_before."""
    best_sym = ""
    best_move = 0.0
    for row in items if isinstance(items, list) else []:
        if not isinstance(row, dict):
            continue
        sym_u = str(row.get("symbol") or "").strip().upper()
        if not sym_u:
            continue
        try:
            curr = float(row.get("price"))
        except (TypeError, ValueError):
            continue
        prev = prices_before.get(sym_u) or prices_before.get(sym_u.lower())
        if not prev or prev <= 0:
            continue
        move = (curr - prev) / prev * 100.0
        if abs(move) > abs(best_move):
            best_move = move
            best_sym = sym_u
    if not best_sym and isinstance(items, list):
        for row in items:
            if isinstance(row, dict) and row.get("symbol"):
                best_sym = str(row["symbol"]).strip().upper()
                break
    side = "BUY" if best_move >= 0 else "SELL"
    return (best_sym or "BTC").strip().upper(), side, best_move


def _momentum_3bar(prices: deque[float]) -> tuple[float, float, float] | None:
    if len(prices) < 3:
        return None
    return prices[-3], prices[-2], prices[-1]


def _momentum_strong_move(prices: deque[float], side: str) -> bool:
    """Strict 3-bar momentum plus material range (first→last bar)."""
    tri = _momentum_3bar(prices)
    if tri is None:
        return False
    a, b, c = tri
    if a <= 0:
        return False
    if side == "BUY" and not (a < b < c):
        return False
    if side == "SELL" and not (a > b > c):
        return False
    move_pct = abs((c - a) / a) * 100.0
    return move_pct >= _MOMENTUM_STRONG_MOVE_PCT


def _signal_momentum(prices: deque[float]) -> tuple[str | None, str]:
    tri = _momentum_3bar(prices)
    if tri is None:
        return None, "momentum"
    a, b, c = tri
    if a < b < c:
        return "BUY", "momentum"
    if a > b > c:
        return "SELL", "momentum"
    return None, "momentum"


def _signal_micro_movement(prices: deque[float]) -> tuple[str | None, str]:
    if len(prices) < 2:
        return None, "micro"
    prev, cur = prices[-2], prices[-1]
    if prev <= 0:
        return None, "micro"
    chg_pct = abs(cur - prev) / prev * 100.0
    if chg_pct >= 0.3:
        return ("BUY" if cur > prev else "SELL"), "micro"
    return None, "micro"


def _signal_trend(prices: deque[float]) -> tuple[str | None, str]:
    if len(prices) < 3:
        return None, "trend"
    window = list(prices)[-3:]
    avg = sum(window) / 3.0
    cur = prices[-1]
    if avg <= 0:
        return None, "trend"
    if cur > avg * 1.00005:
        return "BUY", "trend"
    if cur < avg * 0.99995:
        return "SELL", "trend"
    return None, "trend"


def _volatility_confidence(sigs: list[dict[str, Any]]) -> float:
    for s in sigs:
        if isinstance(s, dict) and s.get("type") == "MARKET_VOLATILITY":
            try:
                return float(s.get("confidence", 0.65))
            except (TypeError, ValueError):
                return 0.65
    return 0.65


def _votes_for_symbol(
    sym: str,
    *,
    volatility_side: str | None,
    dom_sym: str,
    has_market_volatility: bool,
) -> tuple[list[str], list[str]]:
    """Return (buy_signal_names, sell_signal_names) for one symbol."""
    dq = _symbol_prices[sym]
    buy_n: list[str] = []
    sell_n: list[str] = []

    def _add(side: str | None, name: str) -> None:
        if side == "BUY":
            buy_n.append(name)
        elif side == "SELL":
            sell_n.append(name)

    ms, mn = _signal_momentum(dq)
    _add(ms, mn)
    ms, mn = _signal_micro_movement(dq)
    _add(ms, mn)
    ms, mn = _signal_trend(dq)
    _add(ms, mn)
    if has_market_volatility and sym == dom_sym and volatility_side:
        _add(volatility_side, "volatility")
    return buy_n, sell_n


def _decision_confidence(
    agreeing_names: list[str],
    *,
    has_market_volatility: bool,
    sigs: list[dict[str, Any]],
) -> float:
    if agreeing_names == ["fallback"]:
        v = _fallback_micro_confidence_slot[0]
        return float(v) if v is not None else 0.55
    n = len(agreeing_names)
    base = 0.5 + 0.15 * min(n, 5)
    if n == 1 and agreeing_names == ["momentum"]:
        base = min(base, 0.72)
    if has_market_volatility and "volatility" in agreeing_names:
        base = max(base, _volatility_confidence(sigs) * 0.92)
    return round(min(0.95, base), 4)


def _append_price_history(marks: dict[str, float]) -> None:
    for sym, px in marks.items():
        if px > 0:
            _symbol_prices[sym].append(float(px))


def _inst_momentum_filter_ok(dq: deque[float], side: str) -> bool:
    """Require |Δ%| over last N ticks vs threshold, directionally aligned with side."""
    if len(dq) < _INST_MOMENTUM_TICKS:
        return False
    old = dq[-_INST_MOMENTUM_TICKS]
    new = dq[-1]
    if old <= 0:
        return False
    pct = (new - old) / old * 100.0
    if side == "BUY":
        return pct >= _INST_MOMENTUM_MIN_PCT
    return pct <= -_INST_MOMENTUM_MIN_PCT


def _inst_volatility_filter_ok(dq: deque[float]) -> bool:
    """Skip when range is too tight vs mean (low vol chop)."""
    if len(dq) < _VOL_WINDOW_TICKS:
        return False
    w = list(dq)[-_VOL_WINDOW_TICKS:]
    mean = sum(w) / len(w)
    if mean <= 0:
        return False
    band_pct = (max(w) - min(w)) / mean * 100.0
    return band_pct >= _MIN_ROLLING_VOL_PCT


def _inst_trend_bias_ok(dq: deque[float], side: str) -> bool:
    """Short MA vs long MA must agree with trade direction."""
    if len(dq) < _LONG_MA:
        return False
    short_m = sum(list(dq)[-_SHORT_MA:]) / float(_SHORT_MA)
    long_m = sum(list(dq)[-_LONG_MA:]) / float(_LONG_MA)
    if long_m <= 0:
        return False
    if side == "BUY":
        return short_m > long_m * 1.00015
    return short_m < long_m * 0.99985


def _confidence_notional_scale(confidence: float) -> float:
    """Map confidence to [SIZE_CONF_MIN, SIZE_CONF_MAX] of the risk cap."""
    c = max(0.0, min(1.0, float(confidence)))
    return _SIZE_CONF_MIN + (_SIZE_CONF_MAX - _SIZE_CONF_MIN) * c


def _canonical_signal_names(names: tuple[str, ...]) -> list[str]:
    order = ("momentum", "micro", "trend", "volatility")
    s = set(names)
    return [k for k in order if k in s]


def _resolve_buy_patterns(buy_names: list[str], dq: deque[float]) -> tuple[bool, list[str]]:
    """momentum+micro, trend+micro, or strong 3-bar momentum alone."""
    b = set(buy_names)
    if "momentum" in b and "micro" in b:
        return True, _canonical_signal_names(("momentum", "micro"))
    if "trend" in b and "micro" in b:
        return True, _canonical_signal_names(("trend", "micro"))
    if "momentum" in b and _momentum_strong_move(dq, "BUY"):
        return True, ["momentum"]
    return False, []


def _symbol_range_volatility_last10(sym: str) -> float | None:
    """Range vs floor over last 10 prices: (max - min) / min. None if insufficient data."""
    dq = _symbol_prices.get(sym) or deque()
    if len(dq) < 10:
        return None
    tail = list(dq)[-10:]
    lo = min(tail)
    hi = max(tail)
    if lo <= 0:
        return None
    return (hi - lo) / lo


def _risk_parity_notional_multiplier(symbol: str, marks: dict[str, float]) -> float:
    """Scale notional inversely to symbol vol vs peer median (paper sizing only)."""
    marks = marks or {}
    v_sym = _symbol_range_volatility_last10(symbol) or 0.0
    peer: list[float] = []
    for k in marks:
        vx = _symbol_range_volatility_last10(str(k).strip()) or 0.0
        if vx > 0:
            peer.append(vx)
    v_ref = sorted(peer)[len(peer) // 2] if peer else TARGET_VOL_FOR_SCALE
    if v_sym <= 0 or v_ref <= 0:
        return 1.0
    return min(max(v_ref / v_sym, 0.85), 1.15)


def _detect_regime(prices: deque[float]) -> tuple[str, float]:
    """Trend / neutral / range; multiplier applies to add-on scaled qty only."""
    if len(prices) < _LONG_MA:
        return "neutral", 1.0
    seq = list(prices)[-_LONG_MA :]
    lo, hi = min(seq), max(seq)
    if lo <= 0:
        return "neutral", 1.0
    mid = (lo + hi) / 2.0
    width_pct = (hi - lo) / mid
    drift = abs(seq[-1] - seq[0]) / mid
    w = max(width_pct, 1e-9)
    if drift > 0.48 * w:
        return "trend", 1.08
    if drift < 0.30 * w:
        return "range", 0.88
    return "neutral", 1.0


def _resolve_sell_patterns(sell_names: list[str], dq: deque[float]) -> tuple[bool, list[str]]:
    b = set(sell_names)
    if "momentum" in b and "micro" in b:
        return True, _canonical_signal_names(("momentum", "micro"))
    if "trend" in b and "micro" in b:
        return True, _canonical_signal_names(("trend", "micro"))
    if "momentum" in b and _momentum_strong_move(dq, "SELL"):
        return True, ["momentum"]
    return False, []


def _pick_trade_candidate(
    marks: dict[str, float],
    *,
    has_market_volatility: bool,
    dom_sym: str,
    volatility_side: str | None,
) -> tuple[str, str, list[str]] | None:
    """Return (raw_symbol, BUY|SELL, agreeing_signal_names) or None."""
    _fallback_micro_confidence_slot[0] = None
    if not marks:
        return None
    scored: list[tuple[int, str, str, list[str]]] = []
    for sym in marks:
        buy_n, sell_n = _votes_for_symbol(
            sym,
            volatility_side=volatility_side,
            dom_sym=dom_sym,
            has_market_volatility=has_market_volatility,
        )
        dq = _symbol_prices[sym]
        ok_b, names_b = _resolve_buy_patterns(buy_n, dq)
        ok_s, names_s = _resolve_sell_patterns(sell_n, dq)
        if ok_b and names_b:
            scored.append((len(names_b), sym, "BUY", names_b))
        if ok_s and names_s:
            scored.append((len(names_s), sym, "SELL", names_s))
    if scored:
        scored.sort(key=lambda t: (-t[0], t[1]))
        _, sym, side, names = scored[0]
        volatility = _symbol_range_volatility_last10(sym)
        if volatility is None or volatility < MIN_VOLATILITY:
            logger.info(f"VOL FILTER: {sym} skipped low volatility ({volatility})")
            return None
        return sym, side, names

    # Fallback only: flexible 3-bar direction + last-tick + MA10 when primary finds nothing.
    for sym in marks:
        dq = _symbol_prices[sym]
        if len(dq) < 10:
            continue
        p0, p1, p2 = dq[-3], dq[-2], dq[-1]
        if p0 <= 0 or p1 <= 0 or p2 <= 0:
            continue
        momentum = abs(p2 - p0) / p0
        if momentum * 100.0 < _FALLBACK_MIN_NET_MOVE_PCT:
            continue
        last_tick_pct = abs(p2 - p1) / p1 * 100.0
        if last_tick_pct < _FALLBACK_LAST_TICK_MIN_PCT:
            continue
        tail = list(dq)[-10:]
        avg10 = sum(tail) / 10.0
        micro = 0.55 + momentum * 10.0
        confidence = round(min(0.7, max(0.55, micro)), 4)
        up_move = p2 > p0
        down_move = p2 < p0
        up_steps = int(p1 > p0) + int(p2 > p1) + int(p2 > p0)
        dn_steps = int(p1 < p0) + int(p2 < p1) + int(p2 < p0)
        if up_move and up_steps >= 2 and p2 >= avg10:
            side = "BUY"
            names = ["fallback"]
            _fallback_micro_confidence_slot[0] = confidence
            logger.info(f"FALLBACK TRADE FLEX: {sym} {side} conf={confidence}")
            return sym, side, names
        if down_move and dn_steps >= 2 and p2 <= avg10:
            side = "SELL"
            names = ["fallback"]
            _fallback_micro_confidence_slot[0] = confidence
            logger.info(f"FALLBACK TRADE FLEX: {sym} {side} conf={confidence}")
            return sym, side, names
    return None


def _default_ledger() -> dict[str, Any]:
    cap = float(settings.paper_capital_usd)
    return {"equity": cap, "cash": cap, "peak": cap, "positions": []}


def _ledger_equity(ledger: dict[str, Any], marks: dict[str, float]) -> float:
    cash = float(ledger.get("cash", 0.0))
    unreal = 0.0
    for p in ledger.get("positions") or []:
        if not isinstance(p, dict):
            continue
        sym = str(p.get("symbol", ""))
        qty = _safe_qty(p.get("qty"))
        entry = float(p.get("entry", 0.0))
        mark = _mark_for_symbol(marks, sym)
        if mark is None or mark <= 0:
            mark = entry
        if entry > 0 and qty != 0:
            unreal += qty * (mark - entry)
    return cash + unreal


def _apply_stops(ledger: dict[str, Any], marks: dict[str, float]) -> None:
    """Trailing ratchet then close positions whose stop is touched (paper)."""
    logger.info("CHECKING FOR STALE POSITIONS")
    for idx, _p in enumerate(ledger.get("positions") or []):
        if not isinstance(_p, dict):
            logger.info(f"POSITION idx={idx} status=invalid_type value={_p!r}")
            continue
        _q = _safe_qty(_p.get("qty"))
        _st = "zero_qty" if _q == 0 else "open"
        logger.info(
            f"POSITION idx={idx} symbol={_p.get('symbol')} qty={_q} status={_st} snapshot={_p}"
        )
    cash = float(ledger.get("cash", 0.0))
    kept: list[dict[str, Any]] = []
    for p in ledger.get("positions") or []:
        if not isinstance(p, dict):
            continue
        sym = str(p.get("symbol", ""))
        qty = _safe_qty(p.get("qty"))
        entry = float(p.get("entry", 0.0))
        stop = float(p.get("stop", 0.0))
        mark = _mark_for_symbol(marks, sym)
        if mark is None or mark <= 0 or entry <= 0 or qty == 0:
            kept.append(p)
            continue
        if qty > 0 and mark > entry:
            trail = float(mark) * (1.0 - _TRAIL_FRAC)
            p["stop"] = max(stop, trail)
        elif qty < 0 and mark < entry:
            trail = float(mark) * (1.0 + _TRAIL_FRAC)
            p["stop"] = min(stop, trail)
        stop = float(p.get("stop", 0.0))
        hit = False
        reason = ""
        if qty > 0 and mark <= stop:
            hit = True
            reason = "STOP LOSS"
            cash += qty * mark
        elif qty < 0 and mark >= stop:
            hit = True
            reason = "STOP LOSS"
            cash -= abs(qty) * mark
        else:
            risk_amt = abs(entry - stop)
            if risk_amt > 1e-12:
                if qty > 0 and mark >= entry + risk_amt:
                    hit = True
                    reason = "TAKE PROFIT"
                    cash += qty * mark
                elif qty < 0 and mark <= entry - risk_amt:
                    hit = True
                    reason = "TAKE PROFIT"
                    cash -= abs(qty) * mark
        if hit:
            side_str = str(p.get("side", "")).strip().upper() or (
                "BUY" if qty > 0 else "SELL"
            )
            logger.info(
                f"CLOSING POSITION {sym} {side_str} reason={reason} price={mark}"
            )
            if reason == "STOP LOSS":
                logger.info("paper stop-out %s qty=%s mark=%s", sym, qty, mark)
            logger.info(f"TRADE CLOSED: {p}")
            continue
        kept.append(p)
    ledger["positions"] = kept
    ledger["cash"] = cash
    logger.info(f"POSITIONS AFTER CLEANUP: {len(ledger['positions'])}")


async def _load_ledger(r: redis.Redis) -> dict[str, Any]:
    raw = await r.get(LEDGER_KEY)
    if not raw:
        return _default_ledger()
    try:
        d = json.loads(raw)
        if not isinstance(d, dict):
            return _default_ledger()
        out = _default_ledger()
        out.update({k: d[k] for k in ("equity", "cash", "peak") if k in d})
        pos = d.get("positions")
        out["positions"] = pos if isinstance(pos, list) else []
        return out
    except Exception:
        return _default_ledger()


async def _save_ledger(r: redis.Redis, ledger: dict[str, Any]) -> None:
    await r.set(LEDGER_KEY, json.dumps(ledger, separators=(",", ":")))


def _open_count(ledger: dict[str, Any]) -> int:
    logger.info(f"_open_count INPUT: {ledger}")
    count = sum(
        1
        for p in (ledger.get("positions") or [])
        if isinstance(p, dict) and _safe_qty(p.get("qty")) != 0
    )
    logger.info(f"_open_count RESULT: {count}")
    return count


def _drawdown_pct(ledger: dict[str, Any]) -> float:
    peak = max(float(ledger.get("peak", 1.0)), 1.0)
    eq = float(ledger.get("equity", peak))
    return 100.0 * (1.0 - eq / peak)


def _equity_mode(drawdown: float) -> float:
    """Fractional drawdown from peak (0–1): scale aggressiveness."""
    if drawdown < 0.05:
        return 1.0
    if drawdown < 0.10:
        return 0.75
    if drawdown < 0.15:
        return 0.5
    return 0.3


def _drawdown_scale(drawdown: float, limit: float) -> float:
    """Scale size vs drawdown (%). limit matches AGENT_DRAWDOWN_REJECT_PCT units (0–100)."""
    try:
        if drawdown != drawdown or limit != limit:
            return 0.5
        if drawdown <= 0:
            return 1.0
        if limit <= 0:
            return 1.0
        ratio = drawdown / limit
        if ratio <= 1:
            return 1.0 - (0.5 * ratio)
        return max(0.25, 0.5 / ratio)
    except Exception:
        return 0.5


def _paper_exit_engine_auto_close(ledger: dict[str, Any], marks: dict[str, Any]) -> None:
    # =========================
    # EXIT ENGINE (AUTO-CLOSE)
    # =========================
    marks = marks or {}
    now_ts = datetime.now(timezone.utc)
    new_positions: list[dict[str, Any]] = []
    cash = float(ledger.get("cash", 0.0))

    for pos in ledger.get("positions") or []:
        try:
            if not isinstance(pos, dict):
                new_positions.append(pos)
                continue
            symbol_p = str(pos.get("symbol") or "")
            qty = _safe_qty(pos.get("qty"))
            entry = float(pos.get("entry", 0) or 0)
            opened_at = pos.get("opened_at")

            if not opened_at:
                new_positions.append(pos)
                continue

            opened_raw = str(opened_at).replace("Z", "+00:00")
            opened_dt = datetime.fromisoformat(opened_raw)
            if opened_dt.tzinfo is None:
                opened_dt = opened_dt.replace(tzinfo=timezone.utc)
            age_sec = (now_ts - opened_dt).total_seconds()

            mark = _mark_for_symbol(marks, symbol_p)
            if mark is None or mark <= 0:
                new_positions.append(pos)
                continue

            stop = float(pos.get("stop", 0) or 0)
            risk = abs(entry - stop)

            pos_notional = abs(qty) * entry
            if pos_notional > 1e-12:
                pnl_usd = (mark - entry) * qty
                pnl_frac = pnl_usd / pos_notional
            else:
                pnl_frac = 0.0

            if pos.get("partial_tp_done") and not pos.get("tp1_done"):
                pos["tp1_done"] = True

            vol_for_tp = _symbol_range_volatility_last10(symbol_p) or 0.0
            thr1 = max(MIN_PARTIAL_TP_FRAC, 2.5 * vol_for_tp)
            thr2 = max(0.015, 4.0 * vol_for_tp)

            if not pos.get("tp1_done") and pnl_frac > thr1 and abs(qty) > 1e-12:
                close_abs = abs(qty) * 0.33
                if close_abs >= 1e-10:
                    if qty > 0:
                        cash += close_abs * mark
                        pos["qty"] = qty - close_abs
                    else:
                        cash -= close_abs * mark
                        pos["qty"] = qty + close_abs
                    pos["tp1_done"] = True
                    pos["partial_tp_done"] = True
                    logger.info(
                        "EXIT: TP1 partial %s pnl_frac=%.6f thr=%.6f qty_left=%s vol=%.6f",
                        symbol_p,
                        pnl_frac,
                        thr1,
                        pos["qty"],
                        vol_for_tp,
                    )
                    new_positions.append(pos)
                    continue
            if (
                pos.get("tp1_done")
                and not pos.get("tp2_done")
                and pnl_frac > thr2
                and abs(qty) > 1e-12
            ):
                close_abs = abs(qty) * 0.40
                if close_abs >= 1e-10:
                    if qty > 0:
                        cash += close_abs * mark
                        pos["qty"] = qty - close_abs
                    else:
                        cash -= close_abs * mark
                        pos["qty"] = qty + close_abs
                    pos["tp2_done"] = True
                    logger.info(
                        "EXIT: TP2 partial %s pnl_frac=%.6f thr=%.6f qty_left=%s vol=%.6f",
                        symbol_p,
                        pnl_frac,
                        thr2,
                        pos["qty"],
                        vol_for_tp,
                    )
                    new_positions.append(pos)
                    continue

            # =========================
            # VOLATILITY TRAILING STOP
            # =========================
            volatility = vol_for_tp

            if risk > 0 and volatility > 0:
                dynamic_factor = min(2.5, max(1.2, volatility * 10000))

                if qty > 0 and mark >= entry + risk:
                    pos["stop"] = max(float(pos.get("stop", 0) or 0), entry)
                    logger.info(f"TRAILING: BE LONG {symbol_p}")

                if qty < 0 and mark <= entry - risk:
                    pos["stop"] = min(float(pos.get("stop", 0) or 0), entry)
                    logger.info(f"TRAILING: BE SHORT {symbol_p}")

                if qty > 0 and mark >= entry + dynamic_factor * risk:
                    pos["stop"] = max(
                        float(pos.get("stop", 0) or 0), entry + 0.7 * risk
                    )
                    logger.info(f"TRAILING: VOL LOCK LONG {symbol_p}")

                if qty < 0 and mark <= entry - dynamic_factor * risk:
                    pos["stop"] = min(
                        float(pos.get("stop", 0) or 0), entry - 0.7 * risk
                    )
                    logger.info(f"TRAILING: VOL LOCK SHORT {symbol_p}")

            stop = float(pos.get("stop", 0) or 0)
            trail_adapt = max(0.002, volatility * 1.5)
            if qty > 0 and mark > entry:
                ratchet = float(mark) * (1.0 - trail_adapt)
                pos["stop"] = max(stop, ratchet)
                logger.info(
                    "ADAPTIVE_TRAIL long %s trail_frac=%.5f stop=%s",
                    symbol_p,
                    trail_adapt,
                    pos["stop"],
                )
            elif qty < 0 and mark < entry:
                ratchet = float(mark) * (1.0 + trail_adapt)
                prior = float(pos.get("stop", 0) or 0)
                pos["stop"] = min(prior, ratchet) if prior > 0 else ratchet
                logger.info(
                    "ADAPTIVE_TRAIL short %s trail_frac=%.5f stop=%s",
                    symbol_p,
                    trail_adapt,
                    pos["stop"],
                )
            stop = float(pos.get("stop", 0) or 0)

            # 1. HARD STOP LOSS
            if qty > 0 and mark <= stop:
                logger.info(f"EXIT: STOP LOSS HIT {symbol_p}")
                cash += qty * mark
                continue

            if qty < 0 and mark >= stop:
                logger.info(f"EXIT: STOP LOSS HIT {symbol_p}")
                cash -= abs(qty) * mark
                continue

            # 2. TAKE PROFIT (1.5R)
            if risk > 0:
                if qty > 0 and mark >= entry + 1.5 * risk:
                    logger.info(f"EXIT: TAKE PROFIT {symbol_p}")
                    cash += qty * mark
                    continue

                if qty < 0 and mark <= entry - 1.5 * risk:
                    logger.info(f"EXIT: TAKE PROFIT {symbol_p}")
                    cash -= abs(qty) * mark
                    continue

            # 3. TIME EXIT (15 min)
            if age_sec > 900:
                logger.info(f"EXIT: TIMEOUT {symbol_p}")
                if qty > 0:
                    cash += qty * mark
                else:
                    cash -= abs(qty) * mark
                continue

            new_positions.append(pos)

        except Exception as e:
            logger.warning(f"EXIT ENGINE ERROR: {e}")
            new_positions.append(pos)

    ledger["positions"] = new_positions
    ledger["cash"] = cash


async def _maybe_execute_paper_trade(
    r: redis.Redis,
    ledger: dict[str, Any],
    *,
    symbol: str,
    side: str,
    confidence: float,
    marks: dict[str, float],
    trade_decision: dict[str, Any],
) -> dict[str, Any]:
    """Place at most one paper trade if risk gates pass. Mutates ledger; returns ledger."""
    marks = marks or {}
    _paper_exit_engine_auto_close(ledger, marks)
    now = time.monotonic()
    last = _last_trade_mono.get(symbol, 0.0)
    _signals_list = trade_decision.get("signals") or []
    _is_fallback = _signals_list == ["fallback"]

    cooldown_active = (now - last) < COOLDOWN_SEC
    if cooldown_active:
        logger.info(
            f"COOLDOWN CHECK: elapsed={now - last:.2f}s required={COOLDOWN_SEC}"
        )
        if _signals_list == ["fallback"]:
            logger.info("cooldown bypass for fallback")
        elif any(
            isinstance(p, dict)
            and _norm_symbol(str(p.get("symbol") or "")) == _norm_symbol(symbol)
            and _safe_qty(p.get("qty")) != 0
            for p in (ledger.get("positions") or [])
        ):
            logger.info("cooldown bypass for scaling")
        else:
            logger.info("BLOCKED BY: cooldown_per_symbol")
            return ledger
    count = _open_count(ledger)
    if count >= MAX_OPEN_TRADES:
        logger.info(f"OPEN TRADES DEBUG: count={count} max={MAX_OPEN_TRADES}")
        logger.info(f"LEDGER DEBUG: {ledger}")
        logger.info("BLOCKED BY: max_open_trades")
        return ledger

    kill_raw = await r.get(settings.redis_trading_kill_key())
    kill_switch = kill_raw == "1" or bool(settings.trading_kill_switch)

    current_price = _mark_for_symbol(marks, symbol)
    if current_price is None or current_price <= 0:
        logger.warning("Invalid price for %s: %s", symbol, current_price)
        logger.info("BLOCKED BY: invalid_price_or_mark")
        return ledger

    entry = float(current_price)

    dq = _symbol_prices.get(symbol) or deque()
    momentum_ok = _inst_momentum_filter_ok(dq, side)
    if not momentum_ok:
        if _signals_list == ["fallback"]:
            logger.info("momentum bypass for fallback")
        else:
            logger.info("skip trade: momentum filter")
            logger.info("BLOCKED BY: momentum_filter")
            return ledger
    volatility_ok = _inst_volatility_filter_ok(dq)
    if not volatility_ok:
        if _signals_list == ["fallback"]:
            logger.info("volatility bypass for fallback")
        else:
            logger.info("skip trade: low volatility %s", symbol)
            logger.info("BLOCKED BY: volatility_filter")
            return ledger
    # Single institutional trend gate (fallback bypasses here only).
    trend_ok = _inst_trend_bias_ok(dq, side)
    if not trend_ok:
        if _signals_list == ["fallback"]:
            logger.info("trend bypass for fallback")
        else:
            logger.info("skip trade: trend mismatch %s", symbol)
            logger.info("BLOCKED BY: trend_filter")
            return ledger

    equity = float(ledger.get("equity", settings.paper_capital_usd))
    peak = max(float(ledger.get("peak", equity)), 1.0)
    cash = float(ledger.get("cash", equity))
    ledger["equity"] = equity
    ledger["peak"] = peak

    pnl_dd_frac = (peak - equity) / peak if peak > 0 else 0.0

    dd_pct_trade = float(_drawdown_pct(ledger))
    if dd_pct_trade != dd_pct_trade:
        dd_pct_trade = 0.0
    logger.info(
        f"DRAWDOWN CHECK: current={dd_pct_trade:.2f}% "
        f"threshold={AGENT_DRAWDOWN_REJECT_PCT} mode={AGENT_DRAWDOWN_MODE}"
    )
    logger.info(
        f"PNL DEBUG: equity={equity} peak={peak} drawdown={dd_pct_trade:.2f}%"
    )
    logger.info(
        f"DRAWDOWN CONTEXT: drawdown_pct={dd_pct_trade:.2f}% limit_pct={AGENT_DRAWDOWN_REJECT_PCT} mode={AGENT_DRAWDOWN_MODE}"
    )

    dd_gate_scale = 1.0
    if dd_pct_trade > float(AGENT_DRAWDOWN_REJECT_PCT):
        if AGENT_DRAWDOWN_MODE == "soft_scale":
            dd_gate_scale = max(
                0.25,
                1.0 - (dd_pct_trade / (AGENT_DRAWDOWN_REJECT_PCT * 2.0)),
            )
            logger.info(
                "DRAWDOWN SOFT SCALE ACTIVE: drawdown=%.2f%% scale=%.3f",
                dd_pct_trade,
                dd_gate_scale,
            )
        else:
            logger.info("paper trade skipped: global_drawdown_gate")
            return ledger
    # 🔒 filtre qualité minimale (TRÈS IMPORTANT)

    if _is_fallback:
        if confidence <= 0.52:
            logger.info("skip trade: low confidence (fallback)")
            logger.info("BLOCKED BY: confidence_threshold_fallback")
            return ledger
    elif confidence < 0.60:
        logger.info("skip trade: low confidence")
        logger.info("BLOCKED BY: confidence_threshold_primary")
        return ledger

    rm = RiskManager(
        equity_usd=equity,
        cash_usd=cash,
        peak_equity_usd=peak,
        initial_capital_usd=float(settings.paper_capital_usd),
    )
    cap_notional = equity * (settings.trading_max_risk_per_trade_pct / 100.0)
    notional = cap_notional * _confidence_notional_scale(confidence)
    notional = max(0.0, min(cap_notional, notional))
    rp_mult = _risk_parity_notional_multiplier(symbol, marks)
    notional *= rp_mult
    notional = max(0.0, min(cap_notional, notional))
    logger.info(
        "RISK_PARITY notional mult=%.4f symbol=%s marks=%s",
        rp_mult,
        symbol,
        list(marks.keys()) if marks else [],
    )
    notional *= dd_gate_scale
    notional = max(0.0, min(cap_notional, notional))
    drawdown_soft = AGENT_DRAWDOWN_MODE == "soft_scale"
    allowed, rm_reason = rm.allow_order(
        notional_usd=notional,
        symbol=symbol,
        kill_switch=kill_switch,
        drawdown_soft=drawdown_soft,
    )
    if not allowed:
        if drawdown_soft and rm_reason == "global_drawdown_gate":
            logger.info("RISK MANAGER SOFT BYPASS (scaled execution)")
        else:
            logger.info("paper trade skipped: %s", rm_reason)
            logger.info("BLOCKED BY: risk_manager_allow_order")
            return ledger

    scale = _drawdown_scale(dd_pct_trade, float(AGENT_DRAWDOWN_REJECT_PCT))
    notional = notional * scale
    notional = max(0.0, min(cap_notional, notional))
    logger.info(f"DRAWDOWN SCALE APPLIED: drawdown={dd_pct_trade:.2f}% scale={scale:.3f}")

    logger.info(f"PNL FILTER SOFT MODE: drawdown={pnl_dd_frac:.4f}")
    mode_scale = _equity_mode(pnl_dd_frac)
    notional *= mode_scale
    notional = max(0.0, min(cap_notional, notional))
    logger.info(f"EQUITY MODE SCALE: {mode_scale}")

    vol = _symbol_range_volatility_last10(symbol) or 0.0
    if vol > 0:
        vol_scale = min(1.5, max(0.5, vol * 5000))
        notional *= vol_scale
        notional = max(0.0, min(cap_notional, notional))
        logger.info(f"VOL SCALE: {vol_scale}")

    notional = min(cap_notional, max(MIN_NOTIONAL, notional))
    logger.info(f"FINAL NOTIONAL CLAMP: {notional}")

    # =========================
    # INTELLIGENT SCALING
    # =========================
    same_symbol_positions = [
        p
        for p in ledger.get("positions", []) or []
        if isinstance(p, dict)
        and _safe_qty(p.get("qty")) != 0
        and _norm_symbol(str(p.get("symbol") or "")) == _norm_symbol(symbol)
    ]

    max_per_symbol = 3

    if len(same_symbol_positions) >= max_per_symbol:
        logger.info("BLOCKED BY: pyramid_limit")
        return ledger

    for p in same_symbol_positions:
        pq = _safe_qty(p.get("qty"))
        if (pq > 0 and side == "SELL") or (pq < 0 and side == "BUY"):
            logger.info(f"BLOCKED BY: hedge_conflict {symbol}")
            return ledger

    loss_threshold = -0.002  # -0.2% vs position notional at entry (pnl USD)
    deep_loss_threshold = -0.015  # -1.5% when already pyramided on symbol
    worst_pnl_frac: float | None = None
    worst_leg_pnl_usd = 0.0
    for p in same_symbol_positions:
        ent = float(p.get("entry") or 0)
        q = _safe_qty(p.get("qty"))
        symp = str(p.get("symbol") or "")
        mk = _mark_for_symbol(marks, symp)
        if mk and ent:
            pnl = (mk - ent) * q
            pos_notional = abs(q) * ent
            pnl_frac = (pnl / pos_notional) if pos_notional > 1e-12 else 0.0
            if worst_pnl_frac is None or pnl_frac < worst_pnl_frac:
                worst_pnl_frac = pnl_frac
                worst_leg_pnl_usd = pnl
            if (
                len(same_symbol_positions) >= 2
                and pnl_frac < deep_loss_threshold
            ):
                logger.info(
                    "BLOCKED BY: deep_loss_scaling pnl_usd=%.6f pnl_frac=%.6f legs=%s",
                    pnl,
                    pnl_frac,
                    len(same_symbol_positions),
                )
                return ledger
            if pnl_frac < loss_threshold:
                logger.info(
                    "BLOCKED BY: no_scale_losing_position pnl_usd=%.6f pnl_frac=%.6f",
                    pnl,
                    pnl_frac,
                )
                return ledger

    n_legs = len(same_symbol_positions)
    if n_legs > 0:
        scaled_legs = _positions_sorted_by_opened_at(same_symbol_positions)
        last_entry_price = float(scaled_legs[-1].get("entry") or 0)
        if last_entry_price <= 1e-12:
            logger.info("BLOCKED BY: invalid_last_entry_micro_scaling")
            return ledger
        sym_mark = _mark_for_symbol(marks, symbol)
        mk_scale = sym_mark
        if mk_scale is None or mk_scale <= 0:
            logger.info("BLOCKED BY: invalid_mark_micro_scaling")
            return ledger
        price_move = abs(float(mk_scale) - last_entry_price) / last_entry_price
        if price_move < 0.001:
            logger.info("BLOCKED BY: micro_scaling_noise")
            return ledger

    base_qty = (notional / entry) if entry > 1e-12 else 0.0
    scale_factor = 1.0 / (n_legs + 1)
    scaled_qty = base_qty * scale_factor
    if n_legs > 0:
        volatility_sz = _symbol_range_volatility_last10(symbol) or 0.0
        if volatility_sz > 0 and TARGET_VOL_FOR_SCALE > 1e-15:
            vol_factor = min(
                max(volatility_sz / TARGET_VOL_FOR_SCALE, 0.7),
                1.3,
            )
            scaled_qty = scaled_qty * vol_factor
        regime_label, regime_mult = _detect_regime(dq)
        scaled_qty *= regime_mult
        logger.info(
            "REGIME scaling regime=%s mult=%.4f n_legs=%s",
            regime_label,
            regime_mult,
            n_legs,
        )

    worst_leg_pnl_frac = worst_pnl_frac if worst_pnl_frac is not None else 0.0
    logger.info(
        f"SCALING DECISION | pnl_usd={worst_leg_pnl_usd:.6f} pnl_frac={worst_leg_pnl_frac:.6f} "
        f"positions={n_legs} scale_factor={scale_factor:.6f} scaled_qty={scaled_qty:.6f}"
    )

    vol_opt = _symbol_range_volatility_last10(symbol) or 0.0
    thr1_opt = max(MIN_PARTIAL_TP_FRAC, 2.5 * vol_opt)
    thr2_opt = max(0.015, 4.0 * vol_opt)
    trail_opt = max(0.002, vol_opt * 1.5)
    regime_opt, scale_opt = _detect_regime(dq)
    logger.info(
        "OPTIMISE PnL | thr1=%.6f thr2=%.6f trail=%.6f regime=%s scale=%.4f multiplier=%.4f",
        thr1_opt,
        thr2_opt,
        trail_opt,
        regime_opt,
        scale_opt,
        rp_mult,
    )

    qty_mag = scaled_qty
    side_lc = "buy" if side == "BUY" else "sell"
    stop = rm.stop_price(side=side_lc, entry=entry)
    qty = qty_mag if side_lc == "buy" else -qty_mag

    if _signals_list == ["fallback"] and confidence < 0.53:
        logger.info("BLOCKED BY: weak_fallback_signal")
        return ledger

    timestamp = int(time.time() * 1000)
    regime_label_exec, _ = _detect_regime(dq)
    vol_meta = _symbol_range_volatility_last10(symbol) or 0.0
    exec_meta: dict[str, Any] = {
        "regime": regime_label_exec,
        "trail_vol": round(vol_meta, 8),
        "exit_profile": "TP1_TP2_runner_adaptive_trail",
    }
    exec_result = await execute_trade(
        {
            "symbol": symbol,
            "side": side,
            "size": round(abs(qty), 8),
            "price": entry,
            "stop": stop,
            "trade_id": f"{symbol}-{timestamp}",
            "equity_usd": equity,
            "meta": exec_meta,
        }
    )
    if exec_result.get("status") in ("error", "rejected", "skipped"):
        logger.info(
            "paper trade not executed: executor status=%s result=%s",
            exec_result.get("status"),
            exec_result,
        )
        logger.info("BLOCKED BY: execution_layer")
        return ledger

    fill_notional = abs(float(qty)) * entry
    if side_lc == "buy":
        cash -= fill_notional

    pos = {
        "symbol": symbol,
        "side": side.upper(),
        "qty": qty,
        "entry": entry,
        "stop": stop,
        "notional": fill_notional,
        "opened_at": datetime.now(timezone.utc).isoformat(),
        "confidence": confidence,
        "signals": trade_decision.get("signals"),
    }
    ledger.setdefault("positions", []).append(pos)
    logger.info(f"TRADE ADDED: {pos}")
    ledger["cash"] = cash
    ledger["equity"] = _ledger_equity(ledger, marks)
    ledger["peak"] = max(peak, float(ledger["equity"]))
    _last_trade_mono[symbol] = now

    bsym = _broker_symbol(symbol)
    trade_decision_out = {
        "symbol": bsym,
        "signals": trade_decision.get("signals", []),
        "decision": side.upper(),
        "confidence": confidence,
    }
    logger.info("TRADE DECISION:\n%s", json.dumps(trade_decision_out, indent=2))

    log_trade = {
        "symbol": bsym,
        "side": side.upper(),
        "size": round(abs(qty), 8),
        "mode": MODE,
        "entry": entry,
        "stop": stop,
        "notional_usd": round(fill_notional, 2),
        "confidence": round(confidence, 4),
        "signals": trade_decision.get("signals", []),
    }
    logger.info("TRADE:\n%s", json.dumps(log_trade, indent=2))
    return ledger


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    try:
        await wait_for_kafka(max_attempts=KAFKA_STARTUP_MAX_ATTEMPTS)
    except RuntimeError as exc:
        logger.error("%s", exc)
        raise SystemExit(1) from exc
    consumer = await make_consumer(TOPIC, group_id=GROUP_ID)
    r = redis.from_url(settings.redis_url, decode_responses=True)
    try:
        while True:
            try:
                msg = await consumer.getmany(timeout_ms=1000)
                for _, batch in msg.items():
                    for record in batch:
                        try:
                            payload = json.loads(record.value.decode("utf-8"))
                        except Exception:
                            continue
                        layer = str(payload.get("layer") or "")
                        items = payload.get("items") or []
                        if not isinstance(items, list):
                            items = []
                        tick_marks = _prices_from_items(items)
                        if layer == "market_crypto" and tick_marks:
                            for k, v in tick_marks.items():
                                if v > 0:
                                    _live_crypto_marks[str(k).strip().upper()] = float(v)
                            logger.info("MARKS LIVE: %s", dict(_live_crypto_marks))

                        marks = dict(_live_crypto_marks)
                        ledger = await _load_ledger(r)
                        _apply_stops(ledger, marks)
                        ledger["equity"] = _ledger_equity(ledger, marks)
                        ledger["peak"] = max(float(ledger.get("peak", 1.0)), float(ledger["equity"]), 1.0)

                        if layer == "market_crypto":
                            prices_before = dict(cortex_agent._last_crypto_price)
                            sigs_raw = cortex_agent._signals_for_layer("market_crypto", items)
                            sigs_list = [s for s in sigs_raw if isinstance(s, dict)]
                            has_mv = any(s.get("type") == "MARKET_VOLATILITY" for s in sigs_list)
                            dom_sym, dom_side, _mv = _dominant_mover(items, prices_before)
                            vol_side = dom_side if has_mv else None

                            _append_price_history(tick_marks)

                            picked = _pick_trade_candidate(
                                marks,
                                has_market_volatility=has_mv,
                                dom_sym=dom_sym,
                                volatility_side=vol_side,
                            )
                            if picked:
                                sym, side, names = picked
                                confidence = _decision_confidence(
                                    names,
                                    has_market_volatility=has_mv,
                                    sigs=sigs_list,
                                )
                                decision = side
                            else:
                                decision = "HOLD"
                                confidence = _decision_confidence(
                                    [],
                                    has_market_volatility=has_mv,
                                    sigs=sigs_list,
                                )
                            logger.info(f"DECISION: {decision} conf={confidence}")
                            if picked:
                                ledger = await _maybe_execute_paper_trade(
                                    r,
                                    ledger,
                                    symbol=sym,
                                    side=side,
                                    confidence=confidence,
                                    marks=marks,
                                    trade_decision={"signals": names},
                                )
                        await _save_ledger(r, ledger)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("trading_agent tick failed")
                await asyncio.sleep(1.0)
    finally:
        await consumer.stop()
        await r.aclose()


if __name__ == "__main__":
    asyncio.run(main())
