"""Read-only live market snapshots from public vendor APIs + optional configured keys.

No synthetic prices: each venue returns either verified ticks or an explicit empty/error state.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any

import httpx

from app.config import settings
from app.market.binance import fetch_crypto_top_binance
from app.market.finnhub import fetch_equity_sample
from app.market.twelve_data import fetch_forex_sample
from app.services.resilient_http import get_json

logger = logging.getLogger(__name__)

KRAKEN_TICKER = "https://api.kraken.com/0/public/Ticker"
COINBASE_SPOT = "https://api.coinbase.com/v2/prices/{pair}/spot"
YAHOO_CHART = "https://query1.finance.yahoo.com/v8/finance/chart/{sym}"


async def _fetch_kraken_spot() -> dict[str, Any]:
    """Kraken public ticker (XBT/USD, ETH/USD canonical pair names)."""
    now = datetime.now(timezone.utc).isoformat()
    try:
        data = await get_json(KRAKEN_TICKER, params={"pair": "XXBTZUSD,XETHZUSD"}, timeout=25.0)
    except Exception:
        logger.warning("kraken ticker failed", exc_info=True)
        return {"venue": "kraken", "ticks": [], "error": "fetch_failed", "ts": now}
    if isinstance(data, dict) and data.get("error"):
        return {"venue": "kraken", "ticks": [], "error": data.get("error"), "ts": now}
    result = data.get("result") if isinstance(data, dict) else None
    if not isinstance(result, dict):
        return {"venue": "kraken", "ticks": [], "error": "invalid_payload", "ts": now}
    ticks: list[dict[str, Any]] = []
    for pair_key, row in result.items():
        if not isinstance(row, dict):
            continue
        c = row.get("c")
        if isinstance(c, list) and c and c[0] is not None:
            sym = "BTC" if "XBT" in str(pair_key) or "BTC" in str(pair_key) else "ETH"
            try:
                price = float(c[0])
            except (TypeError, ValueError):
                continue
            ticks.append({"symbol": sym, "pair": str(pair_key), "price": price, "venue": "kraken", "ts": now})
    return {"venue": "kraken", "ticks": ticks, "ts": now}


async def _fetch_coinbase_one(pair: str, sym: str) -> dict[str, Any] | None:
    now = datetime.now(timezone.utc).isoformat()
    url = COINBASE_SPOT.format(pair=pair)
    try:
        async with httpx.AsyncClient(timeout=20.0, headers={"User-Agent": "Mozilla/5.0"}, follow_redirects=True) as ac:
            r = await ac.get(url)
            r.raise_for_status()
            j = r.json()
    except Exception:
        logger.debug("coinbase %s failed", pair, exc_info=True)
        return None
    try:
        amt = float(j["data"]["amount"])  # type: ignore[index]
    except Exception:
        return None
    return {"symbol": sym, "pair": pair, "price": amt, "venue": "coinbase", "ts": now}


async def _fetch_coinbase_spot() -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    out = await asyncio.gather(
        _fetch_coinbase_one("BTC-USD", "BTC"),
        _fetch_coinbase_one("ETH-USD", "ETH"),
        _fetch_coinbase_one("SOL-USD", "SOL"),
    )
    ticks = [x for x in out if x is not None]
    return {"venue": "coinbase", "ticks": ticks, "ts": now}


async def _fetch_oanda_spot() -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    token = settings.oanda_api_token
    account = settings.oanda_account_id
    if not token or not account:
        return {
            "venue": "oanda",
            "ticks": [],
            "configured": False,
            "ts": now,
            "hint": "Set OANDA_API_TOKEN and OANDA_ACCOUNT_ID for live FX pricing.",
        }
    url = f"{settings.oanda_base_url.rstrip('/')}/v3/accounts/{account}/pricing"
    params = {"instruments": "EUR_USD,GBP_USD,USD_JPY"}
    try:
        async with httpx.AsyncClient(timeout=25.0, headers={"Authorization": f"Bearer {token}"}) as ac:
            r = await ac.get(url, params=params)
            r.raise_for_status()
            data = r.json()
    except Exception:
        logger.warning("oanda pricing failed", exc_info=True)
        return {"venue": "oanda", "ticks": [], "error": "fetch_failed", "configured": True, "ts": now}
    ticks: list[dict[str, Any]] = []
    prices = data.get("prices") if isinstance(data, dict) else None
    if isinstance(prices, list):
        for p in prices:
            if not isinstance(p, dict):
                continue
            ins = str(p.get("instrument", ""))
            bids = p.get("bids") or []
            asks = p.get("asks") or []
            try:
                bid = float(bids[0]["price"]) if bids and isinstance(bids[0], dict) else None
                ask = float(asks[0]["price"]) if asks and isinstance(asks[0], dict) else None
            except (TypeError, ValueError, KeyError, IndexError):
                continue
            if bid is not None and ask is not None:
                mid = (bid + ask) / 2.0
                ticks.append(
                    {
                        "symbol": ins.replace("_", ""),
                        "instrument": ins,
                        "price": mid,
                        "bid": bid,
                        "ask": ask,
                        "venue": "oanda",
                        "ts": now,
                    }
                )
    return {"venue": "oanda", "ticks": ticks, "configured": True, "ts": now}


async def _fetch_alpha_global_quote(symbol: str = "SPY") -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    key = settings.alpha_vantage_api_key
    if not key:
        return {
            "venue": "alpha_vantage",
            "ticks": [],
            "configured": False,
            "ts": now,
            "hint": "Set ALPHA_VANTAGE_API_KEY for GLOBAL_QUOTE equity snapshots.",
        }
    url = "https://www.alphavantage.co/query"
    params = {"function": "GLOBAL_QUOTE", "symbol": symbol, "apikey": key}
    try:
        data = await get_json(url, params=params, timeout=30.0)
    except Exception:
        logger.warning("alpha_vantage failed", exc_info=True)
        return {"venue": "alpha_vantage", "ticks": [], "error": "fetch_failed", "configured": True, "ts": now}
    gq = data.get("Global Quote") if isinstance(data, dict) else None
    if not isinstance(gq, dict):
        return {"venue": "alpha_vantage", "ticks": [], "error": "rate_limit_or_invalid", "configured": True, "ts": now}
    try:
        price = float(gq.get("05. price", 0) or 0)
    except (TypeError, ValueError):
        price = 0.0
    if price <= 0:
        return {"venue": "alpha_vantage", "ticks": [], "error": "no_price_field", "configured": True, "ts": now}
    return {
        "venue": "alpha_vantage",
        "ticks": [{"symbol": symbol, "price": price, "venue": "alpha_vantage", "ts": now, "raw": gq}],
        "configured": True,
        "ts": now,
    }


async def _fetch_yahoo_spot(symbols: tuple[str, ...] = ("SPY", "QQQ")) -> dict[str, Any]:
    """Yahoo chart endpoint (live, unofficial); may fail under rate limits."""
    now = datetime.now(timezone.utc).isoformat()
    ticks: list[dict[str, Any]] = []
    for sym in symbols:
        url = YAHOO_CHART.format(sym=sym)
        try:
            data = await get_json(url, params={"interval": "1m", "range": "1d"}, timeout=25.0)
        except Exception:
            logger.debug("yahoo %s failed", sym, exc_info=True)
            continue
        try:
            chart = data["chart"]["result"][0]  # type: ignore[index]
            meta = chart.get("meta") or {}
            price = float(meta.get("regularMarketPrice") or meta.get("previousClose") or 0)
            if price > 0:
                ticks.append({"symbol": sym, "price": price, "venue": "yahoo_finance", "ts": now})
        except Exception:
            continue
    return {
        "venue": "yahoo_finance",
        "ticks": ticks,
        "ts": now,
        "note": "Unofficial chart endpoint; use FINNHUB for supported vendor quotes when available.",
    }


async def build_market_hub_snapshot() -> dict[str, Any]:
    """Aggregate parallel live reads; failures are isolated per venue."""
    ts = datetime.now(timezone.utc).isoformat()
    binance_task = fetch_crypto_top_binance()
    kraken_task = _fetch_kraken_spot()
    coin_task = _fetch_coinbase_spot()
    twelve_task = fetch_forex_sample()
    finnhub_task = fetch_equity_sample()
    oanda_task = _fetch_oanda_spot()
    alpha_task = _fetch_alpha_global_quote("SPY")
    yahoo_task = _fetch_yahoo_spot(("SPY", "QQQ"))

    (
        binance_ticks,
        kraken_blob,
        coin_blob,
        twelve_ticks,
        finnhub_ticks,
        oanda_blob,
        alpha_blob,
        yahoo_blob,
    ) = await asyncio.gather(
        binance_task,
        kraken_task,
        coin_task,
        twelve_task,
        finnhub_task,
        oanda_task,
        alpha_task,
        yahoo_task,
        return_exceptions=True,
    )

    def _norm_list(x: Any, venue: str) -> list[dict[str, Any]]:
        if isinstance(x, Exception):
            logger.warning("%s aggregate error: %s", venue, x)
            return []
        if isinstance(x, list):
            return [i for i in x if isinstance(i, dict)]
        return []

    def _norm_dict(x: Any) -> dict[str, Any]:
        if isinstance(x, Exception):
            return {"error": str(x), "ticks": []}
        if isinstance(x, dict):
            return x
        return {"ticks": []}

    crypto_binance = _norm_list(binance_ticks, "binance")
    crypto = {
        "binance": crypto_binance,
        "kraken": _norm_dict(kraken_blob),
        "coinbase": _norm_dict(coin_blob),
    }
    forex_twelve = _norm_list(twelve_ticks, "twelve_data")
    forex = {
        "twelve_data": forex_twelve,
        "oanda": _norm_dict(oanda_blob),
    }
    eq_finnhub = _norm_list(finnhub_ticks, "finnhub")
    equities = {
        "finnhub": eq_finnhub,
        "alpha_vantage": _norm_dict(alpha_blob),
        "yahoo_finance": _norm_dict(yahoo_blob),
    }
    return {"ts": ts, "crypto": crypto, "forex": forex, "equities": equities}


async def redis_broker_snapshot(client: Any) -> dict[str, Any]:
    """Fusion / MetaTrader / cTrader bridge fields already written by execution adapters."""
    now = datetime.now(timezone.utc).isoformat()
    try:
        raw_lp = await client.get(settings.redis_live_positions_key())
        raw_ac = await client.get(settings.redis_live_account_key())
        live_positions = json.loads(raw_lp) if raw_lp else []
        live_account = json.loads(raw_ac) if raw_ac else {}
    except Exception:
        logger.debug("redis broker snapshot failed", exc_info=True)
        live_positions, live_account = [], {}
    bridge_url = settings.mt5_bridge_url or settings.ctrader_base_url
    return {
        "venue": "fusion_mt5_ctrader",
        "ts": now,
        "mt5_bridge_configured": bool(settings.mt5_bridge_url and settings.mt5_api_token),
        "ctrader_configured": bool(settings.ctrader_access_token and settings.ctrader_account_id),
        "any_bridge_url": bool(bridge_url),
        "live_broker_setting": settings.trading_live_broker,
        "live_positions": live_positions[:80] if isinstance(live_positions, list) else [],
        "live_account": live_account if isinstance(live_account, dict) else {},
    }


def merge_broker_into_market_hub(hub: dict[str, Any], broker: dict[str, Any]) -> None:
    hub["broker"] = broker
    lp = broker.get("live_positions") if isinstance(broker.get("live_positions"), list) else []
    acct = broker.get("live_account") if isinstance(broker.get("live_account"), dict) else {}
    hub.setdefault("forex", {})["fusion_markets"] = {
        "venue": broker.get("venue"),
        "ts": broker.get("ts"),
        "mt5_bridge_configured": broker.get("mt5_bridge_configured"),
        "ctrader_configured": broker.get("ctrader_configured"),
        "any_bridge_url": broker.get("any_bridge_url"),
        "live_broker_setting": broker.get("live_broker_setting"),
        "live_positions_count": len(lp),
        "live_account_fields": sorted(acct.keys())[:24],
    }


async def build_market_hub_live_payload(client: Any) -> dict[str, Any]:
    hub = await build_market_hub_snapshot()
    broker = await redis_broker_snapshot(client)
    merge_broker_into_market_hub(hub, broker)
    return hub
