"""
Live Data Engine: read-only market data ingestion (public APIs only).
No trading, broker execution, or capital deployment.

Env:
  ALGO_SPHERE_LIVE_SYMBOLS — comma list (default: six FX/index names below; BTC/ETH still supported when listed)
  ALGO_SPHERE_LIVE_INTERVAL_SEC — poll interval seconds (default 60; min 30)
  ALGO_SPHERE_LIVE_TESTING — 1/true/on enables weekend evolution merge with ohlc_live_extension.csv
  ALGO_SPHERE_LIVE_PRIMARY_SYMBOL — symbol whose closes are appended for evolution (default: first in list)
"""

from __future__ import annotations

import csv
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import quote

import requests

from .config import DATA_DIR
from .database import get_alert_engine_state, get_connection, set_alert_engine_state

logger = logging.getLogger(__name__)

STATE_KEY = "live_data_state"
RUNNER_SIM_FEED_KEY = "runner_live_sim_feed_v1"
LIVE_OHLC_EXTENSION_NAME = "ohlc_live_extension.csv"

LIVE_HEALTHY = "LIVE_HEALTHY"
LIVE_DEGRADED = "LIVE_DEGRADED"
LIVE_OFFLINE = "LIVE_OFFLINE"

SUNDAY_DEFAULT_SYMBOLS = (
    "XAUUSD",
    "EURUSD",
    "NAS100",
    "US30",
    "SPX500",
    "USDJPY",
)


def live_poll_interval_sec() -> float:
    raw = (os.environ.get("ALGO_SPHERE_LIVE_INTERVAL_SEC") or "60").strip()
    try:
        return max(30.0, float(raw))
    except ValueError:
        return 60.0


DEFAULT_INTERVAL_SEC = int(live_poll_interval_sec())
MAX_PRICE_BUFFER = 24
STALE_SEC_HEALTHY = 150
STALE_SEC_DEGRADED = 300

_http_get: Callable[..., Any] = requests.get

_state_lock = threading.Lock()
_extension_lock = threading.Lock()
_bg_thread: threading.Thread | None = None
_bg_stop = threading.Event()
_last_loop_error: str | None = None
_minute_ohlc: dict[str, dict[str, Any]] = {}


def get_live_symbols() -> tuple[str, ...]:
    raw = (os.environ.get("ALGO_SPHERE_LIVE_SYMBOLS") or "").strip()
    if not raw:
        return SUNDAY_DEFAULT_SYMBOLS
    parts = tuple(x.strip().upper() for x in raw.split(",") if x.strip())
    return parts if parts else SUNDAY_DEFAULT_SYMBOLS


def live_testing_enabled() -> bool:
    return os.environ.get("ALGO_SPHERE_LIVE_TESTING", "").strip().lower() in ("1", "true", "yes", "on")


def live_primary_symbol() -> str:
    p = os.environ.get("ALGO_SPHERE_LIVE_PRIMARY_SYMBOL", "").strip().upper()
    if p:
        return p
    syms = get_live_symbols()
    return syms[0] if syms else "XAUUSD"


def live_ohlc_extension_path() -> Path:
    return DATA_DIR / LIVE_OHLC_EXTENSION_NAME


def is_live_ingestion_running() -> bool:
    return _bg_thread is not None and _bg_thread.is_alive()


def get_runner_live_sim_feed() -> dict[str, Any]:
    try:
        with get_connection() as conn:
            raw = get_alert_engine_state(conn, RUNNER_SIM_FEED_KEY)
        if not raw:
            return {}
        out = json.loads(raw)
        return out if isinstance(out, dict) else {}
    except Exception:
        return {}


def set_http_get_for_tests(fn: Callable[..., Any] | None) -> None:
    global _http_get
    _http_get = fn or requests.get


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_iso_ts(ts: str | None) -> float | None:
    if not ts:
        return None
    try:
        s = ts.replace("Z", "+00:00")
        return datetime.fromisoformat(s).timestamp()
    except ValueError:
        return None


def _yahoo_chart(
    ticker: str,
    timeout: float = 10.0,
    *,
    range_param: str = "10d",
    interval: str = "1d",
) -> dict[str, Any] | None:
    enc = quote(str(ticker), safe="")
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{enc}?range={range_param}&interval={interval}"
    try:
        r = _http_get(url, timeout=timeout, headers={"User-Agent": "AlgoSphereLiveData/1.0"})
        r.raise_for_status()
        data = r.json()
    except Exception as exc:
        logger.debug("Yahoo chart failed for %s: %s", ticker, exc)
        return None
    try:
        res = (data.get("chart") or {}).get("result") or []
        if not res:
            return None
        meta = res[0].get("meta") or {}
        price = float(meta.get("regularMarketPrice") or meta.get("previousClose") or 0.0)
        if price <= 0:
            return None
        vol = float(meta.get("regularMarketVolume") or 0.0)
        quotes = (res[0].get("indicators") or {}).get("quote") or []
        closes: list[float] = []
        if quotes:
            raw = quotes[0].get("close") or []
            closes = [float(x) for x in raw if x is not None and float(x) > 0]
        return {"price": price, "volume": vol, "closes": closes}
    except (TypeError, ValueError, KeyError):
        return None


def _fetch_binance_pair(pair: str, timeout: float = 8.0) -> dict[str, Any] | None:
    url = f"https://api.binance.com/api/v3/ticker/24hr?symbol={pair}"
    try:
        r = _http_get(url, timeout=timeout)
        r.raise_for_status()
        j = r.json()
        price = float(j.get("lastPrice") or j.get("weightedAvgPrice") or 0.0)
        if price <= 0:
            return None
        vol = float(j.get("quoteVolume") or 0.0)
        return {"price": price, "volume": vol, "closes": []}
    except Exception as exc:
        logger.debug("Binance fetch failed %s: %s", pair, exc)
        return None


def _fetch_coinbase_product(product_id: str, timeout: float = 8.0) -> dict[str, Any] | None:
    url = f"https://api.coinbase.com/v2/prices/{product_id}/spot"
    try:
        r = _http_get(url, timeout=timeout)
        r.raise_for_status()
        j = r.json()
        amt = float(((j.get("data") or {}).get("amount") or 0))
        if amt <= 0:
            return None
        return {"price": amt, "volume": 0.0, "closes": []}
    except Exception as exc:
        logger.debug("Coinbase fetch failed %s: %s", product_id, exc)
        return None


def fetch_symbol_raw(symbol: str) -> tuple[dict[str, Any] | None, str]:
    """
    Try Binance / Coinbase / Yahoo in order. Returns (payload, source_name).
    payload keys: price, volume, closes (optional history from Yahoo).
    """
    sym = symbol.upper().strip()
    if sym == "BTCUSD":
        for src, fn in (
            ("binance", lambda: _fetch_binance_pair("BTCUSDT")),
            ("coinbase", lambda: _fetch_coinbase_product("BTC-USD")),
            ("yahoo", lambda: _yahoo_chart("BTC-USD")),
        ):
            out = fn()
            if out:
                return out, src
        return None, ""
    if sym == "ETHUSD":
        for src, fn in (
            ("binance", lambda: _fetch_binance_pair("ETHUSDT")),
            ("coinbase", lambda: _fetch_coinbase_product("ETH-USD")),
            ("yahoo", lambda: _yahoo_chart("ETH-USD")),
        ):
            out = fn()
            if out:
                return out, src
        return None, ""
    if sym == "XAUUSD":
        out = _yahoo_chart("GC=F")
        if out:
            return out, "yahoo"
        return None, ""
    if sym == "EURUSD":
        out = _yahoo_chart("EURUSD=X")
        if out:
            return out, "yahoo"
        return None, ""
    if sym == "NAS100":
        for ticker in ("^NDX", "QQQ"):
            out = _yahoo_chart(ticker)
            if out:
                return out, "yahoo"
        return None, ""
    if sym == "US30":
        for ticker in ("^DJI", "YM=F"):
            out = _yahoo_chart(ticker)
            if out:
                return out, "yahoo"
        return None, ""
    if sym == "SPX":
        out = _yahoo_chart("^GSPC")
        if out:
            return out, "yahoo"
        return None, ""
    if sym == "SPX500":
        out = _yahoo_chart("^GSPC")
        if out:
            return out, "yahoo"
        return None, ""
    if sym == "USDJPY":
        out = _yahoo_chart("USDJPY=X")
        if out:
            return out, "yahoo"
        return None, ""
    if sym == "DXY":
        for t in ("DX-Y.NYB", "USD=X"):
            out = _yahoo_chart(t)
            if out:
                return out, "yahoo"
        return None, ""
    return None, ""


def compute_volatility_trend(
    history_prices: list[float],
) -> tuple[float, str, str]:
    """
    Simple ATR-like vol (normalized 0..1) and momentum trend.
    Returns (volatility, trend, regime_hint).
    """
    if len(history_prices) < 2:
        return 0.0, "FLAT", "insufficient_history"
    prices = [float(p) for p in history_prices if p and p > 0]
    if len(prices) < 2:
        return 0.0, "FLAT", "insufficient_history"
    rets: list[float] = []
    for i in range(1, len(prices)):
        a, b = prices[i - 1], prices[i]
        if a > 0:
            rets.append(abs(b - a) / a)
    if not rets:
        return 0.0, "FLAT", "insufficient_history"
    mean_abs = sum(rets) / len(rets)
    vol = max(0.0, min(1.0, mean_abs * 80.0))
    window = min(5, len(prices) - 1)
    old_p = prices[-1 - window]
    last_p = prices[-1]
    if old_p <= 0:
        return vol, "FLAT", "flat_momentum"
    mom = (last_p - old_p) / old_p
    if mom > 0.008:
        trend = "UP"
    elif mom < -0.008:
        trend = "DOWN"
    else:
        trend = "FLAT"
    if vol >= 0.45:
        hint = "elevated_volatility"
    elif vol >= 0.25:
        hint = "moderate_volatility"
    else:
        hint = "calm"
    if abs(mom) >= 0.02:
        hint = f"{hint}_strong_momentum"
    return round(vol, 4), trend, hint


def _syms() -> tuple[str, ...]:
    return get_live_symbols()


def _default_state() -> dict[str, Any]:
    return {
        "symbols": {},
        "price_buffers": {s: [] for s in _syms()},
        "last_update": None,
        "data_health": LIVE_OFFLINE,
        "sources_active": [],
        "consecutive_failures": 0,
        "last_error": None,
    }


def load_persisted_state() -> dict[str, Any]:
    with get_connection() as conn:
        raw = get_alert_engine_state(conn, STATE_KEY)
    if not raw:
        return _default_state()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return _default_state()
    if not isinstance(data, dict):
        return _default_state()
    merged = _default_state()
    merged.update({k: v for k, v in data.items() if k in merged})
    if "price_buffers" in data and isinstance(data["price_buffers"], dict):
        for s in _syms():
            buf = data["price_buffers"].get(s)
            if isinstance(buf, list):
                merged["price_buffers"][s] = [
                    float(x["p"])
                    for x in buf[-MAX_PRICE_BUFFER:]
                    if isinstance(x, dict) and "p" in x
                ]
    if "symbols" in data and isinstance(data["symbols"], dict):
        merged["symbols"] = dict(data["symbols"])
    return merged


def _persist_state(state: dict[str, Any]) -> None:
    syms = _syms()
    snap = {
        "symbols": state.get("symbols", {}),
        "price_buffers": {
            s: [{"p": p, "t": state.get("last_update")} for p in (state.get("price_buffers") or {}).get(s, [])[-MAX_PRICE_BUFFER:]]
            for s in syms
        },
        "last_update": state.get("last_update"),
        "data_health": state.get("data_health"),
        "sources_active": state.get("sources_active", []),
        "consecutive_failures": state.get("consecutive_failures", 0),
        "last_error": state.get("last_error"),
    }
    try:
        with get_connection() as conn:
            set_alert_engine_state(conn, STATE_KEY, json.dumps(snap))
    except Exception as exc:
        logger.warning("live_data_state persist failed: %s", exc)


def _flush_ohlc_row(path: Path, symbol: str, st: dict[str, Any]) -> None:
    time_key = str(st.get("bucket", "")) + ":00Z"
    o, h, low, c = float(st["o"]), float(st["h"]), float(st["l"]), float(st["c"])
    v = float(st.get("v", 0))
    src = str(st.get("source", ""))
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    new_file = not path.is_file() or path.stat().st_size == 0
    with path.open("a", newline="", encoding="utf-8") as f:
        if new_file:
            f.write("time,symbol,open,high,low,close,volume,source\n")
        f.write(f"{time_key},{symbol},{o},{h},{low},{c},{v},{src}\n")


def _update_live_extension_minute_bar(
    symbol: str, price: float, volume: float, source: str, now_iso: str
) -> None:
    path = live_ohlc_extension_path()
    minute_bucket = now_iso[:16]
    with _extension_lock:
        prev = _minute_ohlc.get(symbol)
        if prev and prev.get("bucket") != minute_bucket:
            try:
                _flush_ohlc_row(path, symbol, prev)
            except Exception as exc:
                logger.debug("ohlc extension flush failed %s: %s", symbol, exc)
            prev = None
        if prev is None:
            _minute_ohlc[symbol] = {
                "bucket": minute_bucket,
                "o": price,
                "h": price,
                "l": price,
                "c": price,
                "v": volume,
                "source": source,
            }
        else:
            prev["h"] = max(float(prev["h"]), price)
            prev["l"] = min(float(prev["l"]), price)
            prev["c"] = price
            prev["v"] = float(prev.get("v", 0)) + volume
            prev["source"] = source


def _persist_runner_sim_feed(symbols_out: dict[str, Any], now_iso: str) -> None:
    slim_symbols: dict[str, Any] = {}
    for sym, row in symbols_out.items():
        if not isinstance(row, dict):
            continue
        slim_symbols[str(sym)] = {
            "price": row.get("price"),
            "timestamp": row.get("timestamp"),
            "volatility": row.get("volatility"),
            "trend": row.get("trend"),
            "volume": row.get("volume"),
            "source": row.get("source"),
            "stale": row.get("stale", False),
        }
    payload = {
        "updated_at": now_iso,
        "paper_execution_only": True,
        "live_testing_mode": live_testing_enabled(),
        "symbols_tracked": list(_syms()),
        "symbols": slim_symbols,
    }
    try:
        with get_connection() as conn:
            set_alert_engine_state(conn, RUNNER_SIM_FEED_KEY, json.dumps(payload, separators=(",", ":")))
    except Exception as exc:
        logger.debug("runner sim feed persist failed: %s", exc)


def _compute_health(symbols: dict[str, Any], now_ts: float) -> str:
    if not symbols:
        return LIVE_OFFLINE
    fresh = 0
    stale = 0
    for row in symbols.values():
        if not isinstance(row, dict):
            continue
        ts = _parse_iso_ts(str(row.get("timestamp") or ""))
        if ts is None:
            stale += 1
            continue
        age = now_ts - ts
        if age <= STALE_SEC_HEALTHY:
            fresh += 1
        elif age <= STALE_SEC_DEGRADED:
            stale += 1
        else:
            stale += 1
    n = len(symbols)
    if fresh >= max(1, n // 2 + (n % 2)):
        return LIVE_HEALTHY
    if fresh > 0 or stale < n:
        return LIVE_DEGRADED
    return LIVE_OFFLINE


def refresh_once() -> dict[str, Any]:
    """Fetch all symbols, update buffers and snapshots. Safe on failure."""
    global _last_loop_error
    syms = _syms()
    state = load_persisted_state()
    buffers: dict[str, list[float]] = {s: list((state.get("price_buffers") or {}).get(s, [])) for s in syms}
    symbols_out: dict[str, Any] = {}
    sources_used: list[str] = []
    now_iso = _utc_now_iso()
    now_ts = time.time()
    failures = 0

    for sym in syms:
        try:
            raw, src = fetch_symbol_raw(sym)
            if not raw or not src:
                failures += 1
                prev = (state.get("symbols") or {}).get(sym)
                if isinstance(prev, dict):
                    symbols_out[sym] = {**prev, "stale": True}
                continue
            price = float(raw["price"])
            vol_api = float(raw.get("volume") or 0.0)
            closes = raw.get("closes") or []
            buf = list(buffers.get(sym, []))
            buf.append(price)
            if isinstance(closes, list) and len(closes) >= 3:
                merged = [float(c) for c in closes if c is not None and float(c) > 0]
                if merged:
                    buf = (merged + [price])[-MAX_PRICE_BUFFER:]
            else:
                buf = buf[-MAX_PRICE_BUFFER:]
            buffers[sym] = buf

            hist = buf if len(buf) >= 2 else ([float(c) for c in closes if c is not None and float(c) > 0] + [price])[-MAX_PRICE_BUFFER:]
            v, trend, hint = compute_volatility_trend(hist)
            row = {
                "symbol": sym,
                "price": round(price, 6 if price < 1000 else 4),
                "timestamp": now_iso,
                "volatility": v,
                "trend": trend,
                "volume": round(vol_api, 2),
                "source": src,
                "regime_hint": hint,
                "stale": False,
            }
            symbols_out[sym] = row
            if src not in sources_used:
                sources_used.append(src)
            try:
                _update_live_extension_minute_bar(sym, price, vol_api, src, now_iso)
            except Exception as exc:
                logger.debug("live extension bar %s: %s", sym, exc)
        except Exception as exc:
            failures += 1
            logger.debug("refresh symbol %s error: %s", sym, exc)
            prev = (state.get("symbols") or {}).get(sym)
            if isinstance(prev, dict):
                symbols_out[sym] = {**prev, "stale": True}

    if not symbols_out:
        symbols_out = dict(state.get("symbols") or {})

    health = _compute_health(symbols_out, now_ts)
    n_sym = len(syms)
    if failures >= n_sym and not any(
        isinstance(symbols_out.get(s), dict) and not symbols_out[s].get("stale") for s in syms
    ):
        health = LIVE_OFFLINE

    cf = int(state.get("consecutive_failures") or 0)
    if failures == n_sym:
        cf = min(cf + 1, 1000)
    else:
        cf = 0

    new_state = {
        "symbols": symbols_out,
        "price_buffers": buffers,
        "last_update": now_iso if symbols_out else state.get("last_update"),
        "data_health": health,
        "sources_active": sources_used,
        "consecutive_failures": cf,
        "last_error": None,
    }
    if failures == n_sym:
        new_state["last_error"] = "all_symbol_fetches_failed"
        _last_loop_error = new_state["last_error"]
    else:
        _last_loop_error = None

    _persist_state(new_state)
    _persist_runner_sim_feed(symbols_out, now_iso)
    return new_state


def get_cached_state() -> dict[str, Any]:
    """Thread-safe read of latest persisted + memory (reload from DB for multi-runner)."""
    with _state_lock:
        return load_persisted_state()


def get_status_payload() -> dict[str, Any]:
    st = get_cached_state()
    runner = os.environ.get("LIVE_DATA_RUNNER_ID", "")
    ext_path = live_ohlc_extension_path()
    return {
        "symbols_tracked": list(_syms()),
        "last_update": st.get("last_update"),
        "data_health": st.get("data_health", LIVE_OFFLINE),
        "sources": st.get("sources_active", []),
        "distributed_runner_id": runner or None,
        "consecutive_failures": st.get("consecutive_failures", 0),
        "last_loop_error": _last_loop_error or st.get("last_error"),
        "interval_sec_default": int(live_poll_interval_sec()),
        "read_only": True,
        "live_testing_mode": live_testing_enabled(),
        "primary_symbol": live_primary_symbol(),
        "ingestion_thread_running": is_live_ingestion_running(),
        "ohlc_live_extension_path": str(ext_path),
        "ohlc_live_extension_rows": _count_csv_rows(ext_path),
    }


def _count_csv_rows(path: Path) -> int:
    try:
        if not path.is_file():
            return 0
        with path.open(encoding="utf-8") as f:
            return max(0, sum(1 for _ in f) - 1)
    except OSError:
        return 0


def get_market_payload() -> dict[str, Any]:
    st = get_cached_state()
    symbols = st.get("symbols") or {}
    rows = []
    for sym in _syms():
        row = symbols.get(sym)
        if isinstance(row, dict):
            rows.append({k: row[k] for k in ("symbol", "price", "timestamp", "volatility", "trend", "volume", "source") if k in row})
    return {"symbols": rows}


def get_symbol_payload(symbol: str) -> dict[str, Any]:
    st = get_cached_state()
    sym = symbol.upper().strip()
    row = (st.get("symbols") or {}).get(sym)
    if not row:
        return {"symbol_data": None, "symbol": sym, "read_only": True}
    wanted = ("symbol", "price", "timestamp", "volatility", "trend", "volume", "source")
    filtered: dict[str, Any] = {}
    if isinstance(row, dict):
        filtered = {k: row.get(k) for k in wanted if k in row}
    filtered["symbol"] = sym
    return {"symbol_data": filtered, "read_only": True}


def get_aggregate_context() -> dict[str, Any]:
    """Read-only bundle for regime / risk / meta / portfolio."""
    st = get_cached_state()
    syms = st.get("symbols") or {}
    trends = [str((v or {}).get("trend", "")) for v in syms.values() if isinstance(v, dict)]
    vols = [float((v or {}).get("volatility") or 0) for v in syms.values() if isinstance(v, dict)]
    hints = [str((v or {}).get("regime_hint", "")) for v in syms.values() if isinstance(v, dict)]
    avg_vol = sum(vols) / max(1, len(vols))
    up = sum(1 for t in trends if t == "UP")
    down = sum(1 for t in trends if t == "DOWN")
    return {
        "data_health": st.get("data_health", LIVE_OFFLINE),
        "last_update": st.get("last_update"),
        "avg_volatility": round(avg_vol, 4),
        "trend_vote_up": up,
        "trend_vote_down": down,
        "regime_hints_sample": list(dict.fromkeys(h for h in hints if h))[:5],
        "read_only": True,
        "live_testing_mode": live_testing_enabled(),
    }


def format_regime_reason_line(ctx: dict[str, Any]) -> str | None:
    if not ctx or ctx.get("data_health") == LIVE_OFFLINE:
        return None
    return (
        f"Live data: health={ctx.get('data_health')}, avg_vol={ctx.get('avg_volatility')}, "
        f"momentum_votes up/down={ctx.get('trend_vote_up')}/{ctx.get('trend_vote_down')}."
    )


def format_portfolio_brain_hint(ctx: dict[str, Any]) -> str | None:
    if not ctx or ctx.get("data_health") == LIVE_OFFLINE:
        return None
    return (
        f"External market context (read-only): {ctx.get('data_health')}, "
        f"avg vol {ctx.get('avg_volatility')} — advisory only."
    )


def _background_loop(interval_sec: float) -> None:
    while not _bg_stop.wait(timeout=max(1.0, float(interval_sec))):
        try:
            refresh_once()
        except Exception as exc:
            logger.exception("Live data background loop error: %s", exc)


def start_live_data_background_loop(interval_sec: float | None = None) -> None:
    global _bg_thread
    if _bg_thread is not None and _bg_thread.is_alive():
        return
    _bg_stop.clear()
    sec = float(interval_sec) if interval_sec is not None else live_poll_interval_sec()

    def run() -> None:
        try:
            refresh_once()
        except Exception as exc:
            logger.debug("initial live data refresh: %s", exc)
        _background_loop(sec)

    _bg_thread = threading.Thread(target=run, name="live_data_engine", daemon=True)
    _bg_thread.start()


def stop_live_data_background_loop() -> None:
    _bg_stop.set()
    t = _bg_thread
    if t is not None:
        t.join(timeout=5.0)


def load_extension_closes_for_primary(path: Path | None = None, *, symbol: str | None = None) -> list[float]:
    """Ordered closes from ohlc_live_extension for primary symbol (for evolution merge)."""
    p = path or live_ohlc_extension_path()
    if not p.is_file():
        return []
    want = (symbol or live_primary_symbol()).upper().strip()
    rows: list[tuple[str, float]] = []
    try:
        with p.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not row:
                    continue
                sym = str(row.get("symbol") or "").strip().upper()
                if sym != want:
                    continue
                t = str(row.get("time") or "").strip()
                try:
                    c = float(row.get("close") or 0)
                except (TypeError, ValueError):
                    continue
                if c > 0 and t:
                    rows.append((t, c))
    except OSError:
        return []
    rows.sort(key=lambda x: x[0])
    return [c for _, c in rows]
