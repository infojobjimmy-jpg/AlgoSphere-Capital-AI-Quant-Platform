#!/usr/bin/env python3
"""
Download multi-symbol **real** OHLC history and write research-ready datasets under ``data/``.

Pipeline (``--preference auto``):

1. Optional **Dukascopy / broker CSV** from ``ALGO_SPHERE_DUKASCOPY_EXPORT_DIR`` or
   ``data/broker_exports/dukascopy/{SYMBOL}_D1.csv`` (see ``backend/historical_data_pipeline.py``).
2. **Stooq** daily CSV when the response is non-empty (often blocked on some networks; then Yahoo is used).
3. **Yahoo Finance** via yfinance (fallback).

Outputs:

* ``ohlc_history.csv`` — equal-weight basket of daily simple returns (rebased to 100).
* ``ohlc_history_panel.csv`` — aligned per-symbol closes.
* ``ohlc_history.meta.json`` — sources per symbol, span, limitations.

Use ``--interval 1d`` for multi-year history (Yahoo limits sub-daily length).
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

from backend.config import DATA_DIR
from backend.historical_data_pipeline import DEFAULT_LOGICAL_ORDER, build_aligned_closes_frame

DEFAULT_PANEL_NAME = "ohlc_history_panel.csv"


def closes_to_csv(closes: pd.DataFrame, out_csv: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Equal-weight mean daily return → single basket close (start 100)."""
    rets = closes.pct_change().dropna()
    if rets.empty:
        raise RuntimeError("No overlapping return history after alignment.")
    basket_ret = rets.mean(axis=1)
    equity = (1.0 + basket_ret).cumprod()
    equity = equity * (100.0 / float(equity.iloc[0]))
    aligned_symbols = closes.loc[rets.index]
    out = pd.DataFrame(
        {
            "time": equity.index.strftime("%Y-%m-%d"),
            "open": equity.values,
            "high": equity.values,
            "low": equity.values,
            "close": equity.values,
        }
    )
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_csv, index=False)
    return out, aligned_symbols


def panel_to_csv(closes: pd.DataFrame, out_csv: Path) -> pd.DataFrame:
    """Write time + one column per logical symbol (close prices)."""
    out = pd.DataFrame({"time": closes.index.strftime("%Y-%m-%d")})
    for col in closes.columns:
        out[str(col)] = closes[col].values
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_csv, index=False)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch real multi-symbol OHLC history for AlgoSphere (no synthetic prices)."
    )
    parser.add_argument(
        "--interval",
        default="1d",
        choices=("1d", "15m", "5m"),
        help="Bar interval (default 1d for long history).",
    )
    parser.add_argument(
        "--period",
        default="10y",
        help="yfinance period when Yahoo is used (default 10y).",
    )
    parser.add_argument(
        "--preference",
        default="auto",
        choices=("auto", "yahoo_only", "stooq_first", "export_only"),
        help="Source order: auto tries export→Stooq→Yahoo; yahoo_only skips Stooq.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DATA_DIR / "ohlc_history.csv",
        help="Basket OHLC CSV path",
    )
    parser.add_argument(
        "--panel-output",
        type=Path,
        default=None,
        help=f"Per-symbol panel CSV (default: same dir / {DEFAULT_PANEL_NAME}).",
    )
    parser.add_argument(
        "--no-panel",
        action="store_true",
        help="Do not write per-symbol panel CSV.",
    )
    args = parser.parse_args()

    if args.interval != "1d":
        if args.period in ("10y", "5y", "max"):
            args.period = "60d"

    panel_path = args.panel_output
    if panel_path is None:
        panel_path = args.output.parent / DEFAULT_PANEL_NAME

    closes, pipe_meta = build_aligned_closes_frame(
        DEFAULT_LOGICAL_ORDER,
        period=args.period,
        interval=args.interval,
        preference=args.preference,
    )

    out_df, aligned_closes = closes_to_csv(closes, args.output)
    panel_rows = 0
    if not args.no_panel:
        panel_df = panel_to_csv(aligned_closes, panel_path)
        panel_rows = len(panel_df)

    idx = aligned_closes.index
    t0 = idx.min()
    t1 = idx.max()
    span_days = int((t1 - t0).days) if len(idx) else 0
    years_flat = round(span_days / 365.25, 2) if span_days else None
    years_per_symbol: dict[str, float] = {}
    for col in aligned_closes.columns:
        s = aligned_closes[col].dropna()
        if len(s.index) >= 2:
            years_per_symbol[str(col)] = round((s.index.max() - s.index.min()).days / 365.25, 2)

    from backend.historical_data_pipeline import YAHOO_TICKERS

    meta = {
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "output_csv": str(args.output.resolve()),
        "panel_csv": None if args.no_panel else str(panel_path.resolve()),
        "interval_requested": args.interval,
        "period_requested": args.period,
        "fetch_preference": args.preference,
        "basket_bars": int(len(out_df)),
        "panel_rows": panel_rows,
        "symbols_included": list(aligned_closes.columns),
        "symbols_failed": pipe_meta.get("symbols_failed", []),
        "sources_per_symbol": pipe_meta.get("sources_per_symbol", {}),
        "yahoo_reference_tickers": {k: YAHOO_TICKERS.get(k) for k in aligned_closes.columns},
        "date_start": str(t0.date()) if len(idx) else None,
        "date_end": str(t1.date()) if len(idx) else None,
        "span_days": span_days,
        "years_approx": years_flat,
        "years_approx_per_symbol": years_per_symbol,
        "data_source": "Mixed: broker_export | Stooq | Yahoo (see sources_per_symbol)",
        "limitations": [
            "Venue symbols differ from broker symbols; validate against your LP.",
            "Basket close is an equal-weight blend of daily returns, not one listed instrument.",
            "Daily bars: no true intraday microstructure.",
            "Stooq may return empty responses on some networks; Yahoo is the typical fallback.",
            "Dukascopy integration is via manual CSV export unless you add a custom downloader.",
        ],
        "note": "Set ALGO_SPHERE_DUKASCOPY_EXPORT_DIR or drop files under data/broker_exports/dukascopy/ for vendor history.",
    }
    meta_path = args.output.with_suffix(".meta.json")
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    print(f"Wrote basket {args.output} ({len(out_df)} rows)")
    if not args.no_panel:
        print(f"Wrote panel {panel_path} ({panel_rows} rows)")
    print(f"Meta: {meta_path}")
    print("Sources:", pipe_meta.get("sources_per_symbol"))
    failed = pipe_meta.get("symbols_failed") or []
    if failed:
        print(f"Warning: missing symbols: {', '.join(failed)}", file=sys.stderr)


if __name__ == "__main__":
    main()
