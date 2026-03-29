"""
Merge daily historical panel with minute OHLC live extension (rolled up to daily per symbol).

Output: ``data/ohlc_merged_panel.csv`` plus validation metadata for APIs.
"""

from __future__ import annotations

import csv
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from .config import DATA_DIR
from .live_data_engine import (
    get_live_symbols,
    is_live_ingestion_running,
    live_ohlc_extension_path,
    live_poll_interval_sec,
    live_primary_symbol,
)
from .weekend_evolution_engine import DEFAULT_OHLC_CSV, DEFAULT_PANEL_CSV

MERGED_PANEL_NAME = "ohlc_merged_panel.csv"
MERGED_META_NAME = "ohlc_merged.meta.json"


def merged_panel_path() -> Path:
    return DATA_DIR / MERGED_PANEL_NAME


def merged_meta_path() -> Path:
    return DATA_DIR / MERGED_META_NAME


def _parse_ext_time_iso(ts: str) -> datetime | None:
    ts = (ts or "").strip().replace("Z", "+00:00")
    if not ts:
        return None
    try:
        if len(ts) >= 16 and "T" not in ts and ts[10:11] == " ":
            ts = ts[:10] + "T" + ts[11:]
        return datetime.fromisoformat(ts)
    except ValueError:
        return None


def rollup_live_extension_daily(
    path: Path | None = None,
    *,
    symbols: tuple[str, ...] | None = None,
) -> dict[str, dict[str, float]]:
    """
    From live extension CSV, last close per (UTC calendar day, symbol).
    Returns ``{symbol: { 'YYYY-MM-DD': close }}``.
    """
    p = path or live_ohlc_extension_path()
    want = {s.upper().strip() for s in (symbols or get_live_symbols())}
    by_sym_day: dict[str, dict[str, float]] = {}
    if not p.is_file():
        return {}
    try:
        with p.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except OSError:
        return {}
    for row in rows:
        sym = str(row.get("symbol") or "").strip().upper()
        if sym not in want:
            continue
        t = _parse_ext_time_iso(str(row.get("time") or ""))
        if t is None:
            continue
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
        day = t.astimezone(timezone.utc).date().isoformat()
        try:
            c = float(row.get("close") or 0)
        except (TypeError, ValueError):
            continue
        if c <= 0:
            continue
        # last row wins for that day (file order after sort)
        if sym not in by_sym_day:
            by_sym_day[sym] = {}
        by_sym_day[sym][day] = c
    return by_sym_day


def build_merged_daily_panel(
    *,
    hist_panel_path: Path | None = None,
    extension_path: Path | None = None,
    output_path: Path | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """
    Historical daily panel + daily rollup from live extension for dates after last hist row.
    """
    hp = hist_panel_path or DEFAULT_PANEL_CSV
    if not hp.is_file():
        raise FileNotFoundError(f"Historical panel missing: {hp}")

    hist = pd.read_csv(hp)
    if hist.empty or "time" not in hist.columns:
        raise ValueError("Historical panel must have a time column")

    hist["time"] = pd.to_datetime(hist["time"], utc=True)
    hist = hist.sort_values("time").drop_duplicates(subset=["time"], keep="last")
    last_day = hist["time"].max().date().isoformat()

    sym_cols = [c for c in hist.columns if c != "time"]
    rollup = rollup_live_extension_daily(extension_path, symbols=tuple(sym_cols))

    hist_ts = hist.copy()
    hist_ts["time"] = pd.to_datetime(hist_ts["time"], utc=True)

    all_new_days: set[str] = set()
    for sym in sym_cols:
        su = sym.upper()
        for day_str in rollup.get(su, {}):
            if day_str > last_day:
                all_new_days.add(day_str)

    if not all_new_days:
        merged = hist_ts.copy()
        meta_extra_days = 0
    else:
        last_row = hist_ts.iloc[-1].to_dict()
        extra_frames: list[dict[str, Any]] = []
        for day_str in sorted(all_new_days):
            row = {**last_row, "time": pd.Timestamp(day_str, tz="UTC")}
            for sym in sym_cols:
                su = sym.upper()
                v = rollup.get(su, {}).get(day_str)
                if v is not None:
                    row[sym] = v
            extra_frames.append(row)
            last_row = row
        ext_df = pd.DataFrame(extra_frames)
        merged = pd.concat([hist_ts, ext_df], ignore_index=True)
        merged = merged.sort_values("time").drop_duplicates(subset=["time"], keep="last")
        meta_extra_days = len(ext_df)

    merged["time"] = pd.to_datetime(merged["time"], utc=True)
    merged = merged.sort_values("time")
    out = output_path or merged_panel_path()
    out.parent.mkdir(parents=True, exist_ok=True)
    merged_out = merged.copy()
    merged_out["time"] = merged_out["time"].dt.strftime("%Y-%m-%d")
    merged_out.to_csv(out, index=False)

    t0 = merged["time"].min()
    t1 = merged["time"].max()
    span_days = int((t1 - t0).days) if pd.notna(t0) and pd.notna(t1) else 0
    years_per_symbol: dict[str, float] = {}
    for col in sym_cols:
        sub = merged[["time", col]].dropna(subset=[col])
        if sub.empty:
            continue
        d0, d1 = sub["time"].min(), sub["time"].max()
        years_per_symbol[col] = round((d1 - d0).days / 365.25, 2)

    meta = {
        "written_utc": datetime.now(timezone.utc).isoformat(),
        "historical_panel": str(hp.resolve()),
        "live_extension": str((extension_path or live_ohlc_extension_path()).resolve()),
        "output_csv": str(out.resolve()),
        "rows": int(len(merged)),
        "live_extra_day_rows": meta_extra_days,
        "date_start": str(t0.date()) if pd.notna(t0) else None,
        "date_end": str(t1.date()) if pd.notna(t1) else None,
        "span_days": span_days,
        "years_approx_per_symbol": years_per_symbol,
    }
    merged_meta_path().write_text(json.dumps(meta, indent=2), encoding="utf-8")
    return merged, meta


def validate_data_pipeline() -> dict[str, Any]:
    """
    Dashboard / API: sources, coverage, live thread, merged file readiness.
    """
    hist_meta_path = DEFAULT_OHLC_CSV.with_suffix(".meta.json")
    hist_meta: dict[str, Any] = {}
    if hist_meta_path.is_file():
        try:
            hist_meta = json.loads(hist_meta_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass

    sources_summary = hist_meta.get("sources_per_symbol")
    if not sources_summary and isinstance(hist_meta.get("yahoo_tickers"), list):
        sources_summary = {"_note": "legacy_meta_yahoo_only", "tickers": hist_meta["yahoo_tickers"]}

    years_per: dict[str, Any] = dict(hist_meta.get("years_approx_per_symbol") or {})
    if not years_per and hist_meta.get("years_approx") is not None:
        syms = hist_meta.get("symbols_included") or []
        y = hist_meta["years_approx"]
        years_per = {str(s): y for s in syms}

    panel_ok = DEFAULT_PANEL_CSV.is_file()
    hist_rows = 0
    panel_rows = 0
    if DEFAULT_OHLC_CSV.is_file():
        try:
            hist_rows = max(0, sum(1 for _ in open(DEFAULT_OHLC_CSV, encoding="utf-8")) - 1)
        except OSError:
            pass
    if panel_ok:
        try:
            panel_rows = max(0, sum(1 for _ in open(DEFAULT_PANEL_CSV, encoding="utf-8")) - 1)
        except OSError:
            pass

    ext_path = live_ohlc_extension_path()
    ext_rows = 0
    if ext_path.is_file():
        try:
            ext_rows = max(0, sum(1 for _ in open(ext_path, encoding="utf-8")) - 1)
        except OSError:
            pass

    merged_ok = merged_panel_path().is_file()
    merged_rows = 0
    if merged_ok:
        try:
            merged_rows = max(0, sum(1 for _ in open(merged_panel_path(), encoding="utf-8")) - 1)
        except OSError:
            pass

    live_on = is_live_ingestion_running()
    merged_ready = (
        merged_ok
        and panel_ok
        and panel_rows > 0
        and merged_rows >= panel_rows
        and hist_rows > 0
    )

    no_synth = os.environ.get("ALGO_SPHERE_NO_SYNTHETIC_HISTORY", "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )

    return {
        "historical_csv_exists": DEFAULT_OHLC_CSV.is_file(),
        "historical_panel_exists": panel_ok,
        "historical_meta": hist_meta if hist_meta else None,
        "data_sources_used": sources_summary,
        "years_available_per_symbol": years_per,
        "historical_panel_rows": panel_rows,
        "total_bars_historical_basket": hist_rows,
        "total_bars_live_extension": ext_rows,
        "total_bars_merged_panel": merged_rows,
        "live_feed_running": live_on,
        "live_poll_interval_sec_effective": int(live_poll_interval_sec()),
        "merged_dataset_ready": bool(merged_ready),
        "primary_symbol": live_primary_symbol(),
        "no_synthetic_history_enforced": no_synth,
        "dukascopy_export_dir_configured": bool(
            (os.environ.get("ALGO_SPHERE_DUKASCOPY_EXPORT_DIR") or "").strip()
        )
        or (DATA_DIR / "broker_exports" / "dukascopy").is_dir(),
    }
