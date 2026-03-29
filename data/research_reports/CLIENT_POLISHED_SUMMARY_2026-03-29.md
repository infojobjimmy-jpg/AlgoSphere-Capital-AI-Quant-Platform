# AlgoSphere — Research Summary for Review

**Confidential — research & simulation only**  
*Not investment advice. Past replay results do not guarantee future performance. No live capital deployment.*

---

## Executive overview

AlgoSphere evaluated a broad set of systematic strategy candidates on approximately **ten years of daily market history** using **public, multi-asset data** (Yahoo Finance proxies). Strategies span multiple **behavior families** (mean reversion, trend, breakout, momentum, volatility-aware and cross-asset-style rules). Output is intended for **demo, paper, and due-diligence** workflows—not as a performance guarantee.

---

## Dataset

| Item | Detail |
|------|--------|
| **Calendar span** | **2016-03-29** to **2026-03-27** (~**9.99 years**, **3,650** calendar days) |
| **Bars in replay** | **2,605** daily observations |
| **Symbols (inputs)** | **XAUUSD**, **EURUSD**, **NAS100**, **US30**, **SPX500**, **USDJPY** |
| **Source** | Yahoo Finance via yfinance (public feeds; **not** a live broker feed) |
| **Replay instrument** | Equal-weight **basket** of daily simple returns across the symbols above, rebased (research construct—not a single listed product) |
| **Cross-asset panel** | Aligned per-symbol closes (`ohlc_history_panel.csv`) when row count matches the basket |

---

## Evaluation scope

| Metric | Value |
|--------|--------|
| **Strategies evaluated** | **900** |
| **Ranking mode** | Research composite (emphasis on drawdown, stability, segment robustness; return influence **damped**) |
| **Family mix (generation)** | **90** candidates per family × **10** families: Mean reversion, EMA cross, trend following (Donchian), session breakout, opening-range-style breakout, momentum, volatility regime, liquidity-sweep proxy, regime switching, cross-asset confirmation |

---

## Primary diversified book (quota-balanced, 20 strategies)

Used for internal diversification scoring and correlation context.

| Measure | Value |
|---------|--------|
| **Diversification score** | **83.96** |
| **Average \|pairwise correlation\|** (segment PnL proxy) | **0.401** |
| **Mean “expected drawdown” (model scale)** | **50.59** |
| **Family mix (count)** | Mean reversion **4**, Cross-asset **4**, EMA cross **3**, Session breakout **3**, Momentum **3**, Volatility regime **3** |
| **Quota targets** | Met |

---

## Presentation book — growth orientation (8 strategies, risk parity)

| Measure | Value |
|---------|--------|
| **Weighted replay return proxy** | **+23.79%** (sum of weights × per-strategy replay total return) |
| **Average \|pairwise correlation\|** | **0.299** |
| **Mean “expected drawdown” (model scale)** | **51.5** |

**Holdings (short IDs — full IDs in JSON report):**

| # | Family | Strategy ID (prefix) | Weight | Replay total return | Max DD (replay) |
|---|--------|---------------------|--------|---------------------|-----------------|
| 1 | Mean reversion | `fe089ed9…` | 16.3% | 18.21% | 5.48% |
| 2 | Volatility regime | `8f2acdcd…` | 13.9% | 27.48% | 7.57% |
| 3 | Mean reversion | `f97475a6…` | 19.8% | 19.40% | 5.49% |
| 4 | Regime switching | `c86dbd3d…` | 9.3% | 46.29% | 11.70% |
| 5 | Cross-asset confirm | `fff37582…` | 8.5% | 17.25% | 9.43% |
| 6 | Session breakout | `fe6b7c84…` | 15.6% | 17.44% | 15.41% |
| 7 | Momentum | `b6ceb4fc…` | 9.2% | 19.14% | 13.67% |
| 8 | Regime switching | `c749e674…` | 7.4% | 39.62% | 22.22% |

---

## Presentation book — demo-safe orientation (8 strategies)

| Measure | Value |
|---------|--------|
| **Weighted replay return proxy** | **0.0%** (dominated by sleeves with **no material replay PnL** on this path—see note below) |
| **Average \|pairwise correlation\|** | **0.177** |
| **Presentation diversification score (heuristic)** | **91.14** |
| **Mean “expected drawdown” (model scale)** | **41.84** |

**Holdings:**

| # | Family | Strategy ID (prefix) | Weight | Replay total return | Max DD (replay) |
|---|--------|---------------------|--------|---------------------|-----------------|
| 1 | Cross-asset confirm | `addb2cb6…` | **50.0%** | 0.0% | 0.0% |
| 2 | Cross-asset confirm | `2725066c…` | **50.0%** | 0.0% | 0.0% |
| 3 | Mean reversion | `f97475a6…` | <0.1% | 19.40% | 5.49% |
| 4 | Volatility regime | `a317ddee…` | <0.1% | 20.44% | 7.48% |
| 5 | Mean reversion | `fe089ed9…` | <0.1% | 18.21% | 5.48% |
| 6 | Volatility regime | `74eb92fe…` | <0.1% | 19.90% | 9.46% |
| 7 | Momentum | `b6ceb4fc…` | <0.1% | 19.14% | 13.67% |
| 8 | Liquidity sweep | `ed871f9a…` | <0.1% | 2.37% | 6.62% |

**Transparency note:** Risk-parity weighting assigned ~**half** the book each to two cross-asset configurations with **flat replay equity** (zero return / zero drawdown on this series). That improves **correlation optics** but is **not** economically meaningful concentration. For client conversations, treat the **top 5 safest candidates** and the **non-flat sleeves** in this table as the substantive demo-safe **shortlist**, or re-weight excluding flat paths.

---

## Top 5 safest candidates (by model drawdown proxy, then score)

| Rank | Family | Strategy ID (prefix) | Research score | Replay max DD | Expected DD (model scale) |
|------|--------|---------------------|----------------|---------------|---------------------------|
| 1 | Mean reversion | `fe089ed9…` | 81.47 | 5.48% | 40.0 |
| 2 | Mean reversion | `ee873fa8…` | 81.35 | **1.46%** | 40.0 |
| 3 | Mean reversion | `f97475a6…` | 80.76 | 5.49% | 40.0 |
| 4 | Volatility regime | `a317ddee…` | 80.73 | 7.48% | 40.0 |
| 5 | Volatility regime | `8f2acdcd…` | 80.71 | 7.57% | 40.0 |

---

## Top 5 strongest growth candidates (by replay total return)

| Rank | Family | Strategy ID (prefix) | Replay total return | Research score | Replay max DD |
|------|--------|---------------------|---------------------|----------------|---------------|
| 1 | Regime switching | `516cf7d4…` | **92.20%** | 76.88 | 16.34% |
| 2 | Regime switching | `3aaecfbd…` | 81.04% | 74.87 | 18.80% |
| 3 | Regime switching | `e6f18358…` | 77.28% | 75.97 | 18.44% |
| 4 | Volatility regime | `e00d49e9…` | 75.47% | 74.73 | 17.71% |
| 5 | Regime switching | `a4c1d76d…` | 74.38% | 76.67 | 16.40% |

*Higher replay return here comes with **material** replay drawdowns—suitable for growth-oriented discussion, not “low risk.”*

---

## Client demo verdict (heuristic)

| Field | Value |
|-------|--------|
| **Demo / paper GO / NO-GO** | **GO** |
| **Blocking reasons** | None flagged by automated rules |

This verdict is a **internal readiness screen** (e.g. minimum book size, correlation and diversification heuristics). It does **not** certify profitability or suitability for any investor.

---

## Limitations (material)

1. **Data:** Public Yahoo proxies may **diverge** from FP Markets or other venue symbols and rolls.  
2. **Instrument:** The replay series is a **synthetic basket**, not a tradable single asset.  
3. **Frequency:** **Daily** bars only—no true intraday session ORB, liquidity sweeps on wicks, or microstructure.  
4. **Method:** Historical **replay** with simplified PnL attribution; **no** fills, slippage, fees, or funding.  
5. **Weights:** Demo-safe book can overweight **near-flat** replay paths under risk parity—interpret weights with care.  
6. **Scores:** “Research composite” and “expected drawdown” are **model constructs**, not live-account metrics.

---

## Artifact references

- Machine-readable run: `data/research_reports/client_research_20260329_023453.json`  
- Auto-generated narrative: `data/research_reports/client_research_20260329_023453.md`  
- Full cycle payload: `data/weekend_reports/weekend_report_2026-03-29T023453.049179_0000.json`

---

*AlgoSphere — research & demo tooling. No live trading.*
