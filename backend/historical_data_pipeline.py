"""
Real historical OHLC loaders: broker/Dukascopy export, Stooq daily CSV, Yahoo (yfinance).

No synthetic prices. Fails a symbol if no source returns data (caller aggregates errors).

Preference order (``auto``): dukascopy_export → stooq → yahoo.
"""

from __future__ import annotations

import io
import os
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote

import pandas as pd
import requests
import yfinance as yf

from .config import DATA_DIR

# Logical symbol → Stooq daily symbol (lowercase; Stooq free history)
STOOQ_DAILY_SYMBOLS: dict[str, str] = {
    "XAUUSD": "xauusd",
    "EURUSD": "eurusd",
    "USDJPY": "usdjpy",
    "NAS100": "ndx.us",
    "US30": "dji",
    "SPX500": "spx",
}

# Logical → Yahoo ticker (fallback)
YAHOO_TICKERS: dict[str, str] = {
    "XAUUSD": "GC=F",
    "EURUSD": "EURUSD=X",
    "USDJPY": "USDJPY=X",
    "NAS100": "^NDX",
    "US30": "^DJI",
    "SPX500": "^GSPC",
}

DEFAULT_LOGICAL_ORDER: tuple[str, ...] = (
    "XAUUSD",
    "EURUSD",
    "NAS100",
    "US30",
    "SPX500",
    "USDJPY",
)

_http_get = requests.get


def set_http_get_for_tests(fn: Any) -> None:
    global _http_get
    _http_get = fn


def dukascopy_export_dir() -> Path | None:
    raw = (os.environ.get("ALGO_SPHERE_DUKASCOPY_EXPORT_DIR") or "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    p = DATA_DIR / "broker_exports" / "dukascopy"
    return p if p.is_dir() else None


def load_dukascopy_export_csv(logical: str, export_dir: Path) -> pd.Series | None:
    """
    Expect broker-exported daily (or resampled) CSV with a date column and Close (or close).

    Filenames tried: ``{LOGICAL}_D1.csv``, ``{LOGICAL}_d1.csv``, ``{LOGICAL}.csv``.
    """
    logical_u = logical.upper().strip()
    for name in (
        f"{logical_u}_D1.csv",
        f"{logical_u}_d1.csv",
        f"{logical_u}.csv",
    ):
        path = export_dir / name
        if not path.is_file():
            continue
        try:
            df = pd.read_csv(path)
        except OSError:
            continue
        if df.empty:
            continue
        cols = {c.lower(): c for c in df.columns}
        date_col = cols.get("date") or cols.get("time") or cols.get("datetime")
        close_col = cols.get("close") or cols.get("adj close")
        if not date_col or not close_col:
            continue
        dt = pd.to_datetime(df[date_col], utc=True, errors="coerce")
        cl = pd.to_numeric(df[close_col], errors="coerce")
        s = pd.Series(cl.values, index=dt, name=logical_u)
        s = s[s > 0].dropna()
        s = s[~s.index.duplicated(keep="last")].sort_index()
        if len(s) >= 32:
            return s.astype(float)
    return None


def fetch_stooq_daily_close(stooq_sym: str, *, timeout: float = 45.0) -> pd.Series | None:
    """Download Stooq daily OHLCV; return close series indexed by UTC midnight dates."""
    url = f"https://stooq.com/q/d/l/?s={quote(stooq_sym.lower(), safe='')}&i=d"
    try:
        r = _http_get(
            url,
            timeout=timeout,
            headers={"User-Agent": "AlgoSphereHistorical/1.0 (research)"},
        )
        r.raise_for_status()
    except OSError:
        return None
    text = (r.text or "").strip()
    if len(text) < 30 or text.lower().startswith("no data"):
        return None
    try:
        df = pd.read_csv(io.StringIO(text))
    except pd.errors.EmptyDataError:
        return None
    if df.empty or "Close" not in df.columns:
        return None
    if "Date" not in df.columns:
        return None
    dt = pd.to_datetime(df["Date"], utc=True, errors="coerce")
    cl = pd.to_numeric(df["Close"], errors="coerce")
    s = pd.Series(cl.values, index=dt, name=stooq_sym)
    s = s[s > 0].dropna()
    s = s[~s.index.duplicated(keep="last")].sort_index()
    return s if len(s) >= 32 else None


def fetch_yahoo_close(ticker: str, period: str, interval: str) -> pd.Series | None:
    try:
        df = yf.download(
            ticker,
            period=period,
            interval=interval,
            progress=False,
            auto_adjust=True,
        )
    except Exception:
        return None
    if df is None or df.empty:
        return None
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    for col in ("Close", "Adj Close"):
        if col in df.columns:
            s = df[col].astype(float)
            s = s[s > 0].dropna()
            s.index = pd.to_datetime(s.index, utc=True)
            return s if len(s) >= 32 else None
    return None


def fetch_logical_history(
    logical: str,
    *,
    period: str = "10y",
    interval: str = "1d",
    preference: str = "auto",
) -> tuple[pd.Series | None, str]:
    """
    Returns (close_series_or_none, source_label).

    ``preference``: ``auto`` | ``yahoo_only`` | ``stooq_first`` | ``export_only``
    """
    logical = logical.upper().strip()
    pref = preference.strip().lower().replace("-", "_")

    if interval != "1d":
        yh = YAHOO_TICKERS.get(logical)
        if yh:
            s = fetch_yahoo_close(yh, period, interval)
            if s is not None:
                return s, "yahoo"
        return None, ""

    if pref == "export_only":
        ddir = dukascopy_export_dir()
        if ddir:
            s = load_dukascopy_export_csv(logical, ddir)
            if s is not None:
                return s, "dukascopy_export"
        return None, ""

    if pref in ("auto", "stooq_first") or pref == "stooq_first":
        ddir = dukascopy_export_dir()
        if ddir:
            s = load_dukascopy_export_csv(logical, ddir)
            if s is not None:
                return s, "dukascopy_export"

    if pref in ("auto", "stooq_first"):
        st = STOOQ_DAILY_SYMBOLS.get(logical)
        if st:
            s = fetch_stooq_daily_close(st)
            if s is not None:
                return s, "stooq"

    if pref in ("auto", "yahoo_only", "stooq_first"):
        yh = YAHOO_TICKERS.get(logical)
        if yh and interval == "1d":
            s = fetch_yahoo_close(yh, period, interval)
            if s is not None:
                return s, "yahoo"

    return None, ""


def build_aligned_closes_frame(
    logical_symbols: tuple[str, ...] | None = None,
    *,
    period: str = "10y",
    interval: str = "1d",
    preference: str = "auto",
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """
    Inner-join aligned closes (columns = logical names). Metadata includes per-symbol source.
    """
    syms = logical_symbols or DEFAULT_LOGICAL_ORDER
    loaded: dict[str, pd.Series] = {}
    sources: dict[str, str] = {}
    failed: list[str] = []

    for logical in syms:
        s, src = fetch_logical_history(logical, period=period, interval=interval, preference=preference)
        if s is None or not src:
            failed.append(logical)
            continue
        s = s.copy()
        s.name = logical
        loaded[logical] = s
        sources[logical] = src

    if len(loaded) < 2:
        raise RuntimeError(
            f"Need >=2 symbols with data; loaded={list(loaded.keys())}, failed={failed}"
        )

    frame = pd.DataFrame(loaded).sort_index()
    frame = frame.ffill().dropna(how="any")
    return frame, {
        "sources_per_symbol": sources,
        "symbols_failed": failed,
        "symbols_included": list(frame.columns),
    }
