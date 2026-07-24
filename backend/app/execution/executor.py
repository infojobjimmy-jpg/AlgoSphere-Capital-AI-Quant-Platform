"""
Safe execution adapter: Redis-backed dedupe and trade lifecycle, stop-distance risk,
paper vs live routing. Callers supply orders; signal logic lives elsewhere.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import redis.asyncio as redis

from app.config import settings
from app.trading.brokers.base import BrokerError, OrderRequest
from app.trading.brokers.factory import get_live_broker_adapter

logger = logging.getLogger("acap.execution")

EXECUTED_TRADES_REDIS_KEY = "acap:executed_trades"


def _trade_status_key(trade_id: str) -> str:
    return f"acap:trade_status:{trade_id}"


def _pending_marker_key(trade_id: str) -> str:
    return f"acap:pending:{trade_id}"


# Local hint only; Redis keys above are the source of truth across restarts.
pending_trades: set[str] = set()
_flow_lock = asyncio.Lock()

_redis: redis.Redis | None = None

_MAX_RETRIES = 3
_RETRY_DELAYS_SEC = (0.5, 1.0, 2.0)
# Pending reservation TTL (seconds); refreshed on each live submit attempt.
PENDING_TTL_SEC = 300


async def _redis_client() -> redis.Redis:
    global _redis
    if _redis is None:
        _redis = redis.from_url(settings.redis_url, decode_responses=True)
    return _redis


def _mode() -> str:
    return (settings.trading_mode or "paper").strip().lower()


def _normalize_base_symbol(symbol: str) -> str:
    s = symbol.strip().upper().replace("-", "")
    if s.endswith("USDT"):
        s = s[:-4]
    if s.endswith("USD"):
        s = s[:-3]
    return s


def _symbol_allowed(symbol: str) -> bool:
    return _normalize_base_symbol(symbol) in frozenset({"BTC", "ETH", "SOL"})


def _validate_order(order: dict[str, Any]) -> tuple[bool, str]:
    if not isinstance(order, dict):
        return False, "invalid_order_type"
    tid = str(order.get("trade_id") or "").strip()
    if not tid:
        return False, "missing_trade_id"
    sym = str(order.get("symbol") or "").strip()
    if not sym or not _symbol_allowed(sym):
        return False, "invalid_symbol"
    try:
        size = float(order.get("size", 0.0))
    except (TypeError, ValueError):
        return False, "invalid_size"
    if size <= 0.0:
        return False, "size_not_positive"
    if order.get("price") is None:
        return False, "missing_price"
    try:
        price = float(order["price"])
    except (TypeError, ValueError):
        return False, "invalid_price"
    if price <= 0.0:
        return False, "price_not_positive"
    if order.get("stop") is None:
        return False, "missing_stop"
    try:
        stop = float(order["stop"])
    except (TypeError, ValueError):
        return False, "invalid_stop"
    if stop <= 0.0:
        return False, "stop_not_positive"
    return True, "ok"


def _order_risk_usd(order: dict[str, Any]) -> float:
    price = float(order["price"])
    stop = float(order["stop"])
    size = float(order["size"])
    return abs(price - stop) * size


def _max_risk_usd(order: dict[str, Any]) -> float:
    equity = float(order.get("equity_usd") or settings.paper_capital_usd)
    return max(0.0, equity) * (settings.trading_max_risk_per_trade_pct / 100.0)


def _log_execution_safe_final(
    trade_id: str,
    symbol: str,
    side: str,
    size: float,
    price: float,
    risk: float,
    redis_status: str,
    mode: str,
    meta: dict[str, Any] | None = None,
) -> None:
    tail = ""
    if isinstance(meta, dict) and meta:
        tail = (
            f"\nmeta_regime={meta.get('regime')!r} "
            f"meta_trail_vol={meta.get('trail_vol')!r} "
            f"meta_exit_profile={meta.get('exit_profile')!r}"
        )
    logger.info(
        f"""
EXECUTION SAFE FINAL:
trade_id={trade_id}
symbol={symbol}
side={side}
size={size}
price={price}
risk={risk}
status={redis_status}
mode={mode}{tail}
"""
    )


async def _paper_fill(order: dict[str, Any], *, risk: float) -> dict[str, Any]:
    symbol = str(order["symbol"])
    side = str(order["side"])
    size = float(order["size"])
    price = float(order["price"])
    tid = str(order["trade_id"])
    return {
        "status": "filled",
        "mode": "paper",
        "trade_id": tid,
        "filled_price": price,
        "risk_usd": risk,
    }


async def _live_submit(
    order: dict[str, Any],
    *,
    risk: float,
    r: redis.Redis,
    status_key: str,
) -> dict[str, Any]:
    symbol = str(order["symbol"])
    side_raw = str(order.get("side", "buy")).strip().lower()
    side = "sell" if side_raw.startswith("s") else "buy"
    size = float(order["size"])
    price = float(order["price"])
    stop = float(order["stop"])
    notional = size * price
    tid = str(order["trade_id"])
    meta: dict[str, Any] = {"trade_id": tid, "risk_usd": risk}
    _m = order.get("meta")
    if isinstance(_m, dict):
        meta.update(_m)

    last_err: BaseException | None = None
    for attempt in range(_MAX_RETRIES):
        try:
            await r.expire(status_key, PENDING_TTL_SEC)
            st = await r.get(status_key)
            if st == "done":
                logger.info("EXECUTION live skip retry trade_id=%s redis=done", tid)
                return {
                    "status": "skipped",
                    "mode": "live",
                    "trade_id": tid,
                    "reason": "already_done",
                    "risk_usd": risk,
                }
            if st != "pending":
                logger.warning(
                    "EXECUTION live unexpected status trade_id=%s redis=%r",
                    tid,
                    st,
                )

            adapter = get_live_broker_adapter()
            req = OrderRequest(
                symbol=symbol,
                side=side,
                qty=size,
                stop_price=stop,
                notional_usd=notional,
                meta=meta,
            )
            result = await adapter.execute_order(req)
            return {
                "status": result.status,
                "mode": "live",
                "trade_id": tid,
                "broker_order_id": result.order_id,
                "filled_price": result.filled_price or price,
                "risk_usd": risk,
                "raw": result.raw,
            }
        except (BrokerError, OSError, RuntimeError) as e:
            last_err = e
            st = await r.get(status_key)
            if st == "done":
                logger.info("EXECUTION live abort retries trade_id=%s redis=done", tid)
                return {
                    "status": "skipped",
                    "mode": "live",
                    "trade_id": tid,
                    "reason": "already_done",
                    "risk_usd": risk,
                }
            delay = _RETRY_DELAYS_SEC[min(attempt, len(_RETRY_DELAYS_SEC) - 1)]
            logger.warning(
                "live execution attempt %d/%d failed: %s; backoff %.1fs",
                attempt + 1,
                _MAX_RETRIES,
                e,
                delay,
                exc_info=attempt + 1 == _MAX_RETRIES,
            )
            if attempt + 1 < _MAX_RETRIES:
                await asyncio.sleep(delay)
    assert last_err is not None
    logger.error("live execution exhausted retries: %s", last_err)
    return {
        "status": "error",
        "mode": "live",
        "trade_id": tid,
        "error": str(last_err),
        "risk_usd": risk,
    }


async def execute_trade(order: dict[str, Any]) -> dict[str, Any]:
    """
    validate → check Redis (done / pending / executed set) → risk →
    SET pending (NX) + pending marker → execute → finalize

    Redis is authoritative for idempotency; pending_trades is a same-process hint only.
    """
    tid = str(order.get("trade_id") or "").strip()
    if not tid:
        return {"status": "rejected", "reason": "missing_trade_id", "trade_id": ""}

    ok, reason = _validate_order(order)
    if not ok:
        logger.info("EXECUTION rejected trade_id=%s reason=%s", tid, reason)
        return {"status": "rejected", "reason": reason, "trade_id": tid}

    risk = _order_risk_usd(order)
    symbol = str(order["symbol"])
    side = str(order.get("side", "buy"))
    size = float(order["size"])
    price = float(order["price"])
    order_meta: dict[str, Any] | None = None
    _m0 = order.get("meta")
    if isinstance(_m0, dict):
        order_meta = _m0

    try:
        r = await _redis_client()
    except Exception as e:
        logger.exception("EXECUTION redis unavailable trade_id=%s", tid)
        return {"status": "error", "trade_id": tid, "reason": "redis_unavailable", "error": str(e)}

    status_key = _trade_status_key(tid)
    pending_key = _pending_marker_key(tid)
    mode = _mode()

    async with _flow_lock:
        if tid in pending_trades:
            try:
                st = await r.get(status_key)
            except Exception as e:
                logger.exception("EXECUTION redis read failed trade_id=%s", tid)
                return {"status": "error", "trade_id": tid, "reason": "redis_read_failed", "error": str(e)}
            if st == "pending":
                logger.info("EXECUTION skip in-flight trade_id=%s", tid)
                _log_execution_safe_final(
                    tid, symbol, side, size, price, risk, "pending", mode, order_meta
                )
                return {"status": "skipped", "reason": "pending", "trade_id": tid}
            pending_trades.discard(tid)

        try:
            if await r.sismember(EXECUTED_TRADES_REDIS_KEY, tid):
                logger.info("EXECUTION skip executed set trade_id=%s", tid)
                _log_execution_safe_final(
                    tid, symbol, side, size, price, risk, "done", mode, order_meta
                )
                return {"status": "skipped", "reason": "duplicate", "trade_id": tid}
            st = await r.get(status_key)
            if st == "done":
                logger.info("EXECUTION skip redis done trade_id=%s", tid)
                _log_execution_safe_final(
                    tid, symbol, side, size, price, risk, "done", mode, order_meta
                )
                return {"status": "skipped", "reason": "duplicate", "trade_id": tid}
            if st == "pending":
                logger.info("EXECUTION skip redis pending trade_id=%s", tid)
                _log_execution_safe_final(
                    tid, symbol, side, size, price, risk, "pending", mode, order_meta
                )
                return {"status": "skipped", "reason": "pending", "trade_id": tid}
        except Exception as e:
            logger.exception("EXECUTION redis read failed trade_id=%s", tid)
            return {"status": "error", "trade_id": tid, "reason": "redis_read_failed", "error": str(e)}

        cap = _max_risk_usd(order)
        if risk > cap + 1e-9:
            logger.info("EXECUTION rejected trade_id=%s reason=exceeds_max_risk_pct risk=%s", tid, risk)
            return {"status": "rejected", "reason": "exceeds_max_risk_pct", "trade_id": tid, "risk_usd": risk}

        try:
            reserved = await r.set(status_key, "pending", ex=PENDING_TTL_SEC, nx=True)
        except Exception as e:
            logger.exception("EXECUTION redis set pending failed trade_id=%s", tid)
            return {"status": "error", "trade_id": tid, "reason": "redis_write_failed", "error": str(e)}

        if not reserved:
            try:
                st = await r.get(status_key)
            except Exception as e:
                logger.exception("EXECUTION redis read failed trade_id=%s", tid)
                return {"status": "error", "trade_id": tid, "reason": "redis_read_failed", "error": str(e)}
            if st == "done":
                logger.info("EXECUTION skip after NX lost trade_id=%s redis=done", tid)
                _log_execution_safe_final(
                    tid, symbol, side, size, price, risk, "done", mode, order_meta
                )
                return {"status": "skipped", "reason": "duplicate", "trade_id": tid}
            if st == "pending":
                logger.info("EXECUTION skip after NX lost trade_id=%s redis=pending", tid)
                _log_execution_safe_final(
                    tid, symbol, side, size, price, risk, "pending", mode, order_meta
                )
                return {"status": "skipped", "reason": "pending", "trade_id": tid}
            logger.info("EXECUTION skip after NX lost trade_id=%s redis=%r", tid, st)
            _log_execution_safe_final(
                tid, symbol, side, size, price, risk, str(st or ""), mode, order_meta
            )
            return {"status": "skipped", "reason": "pending", "trade_id": tid}

        try:
            await r.set(pending_key, "1", ex=PENDING_TTL_SEC)
        except Exception as e:
            logger.exception("EXECUTION redis pending marker failed trade_id=%s", tid)
            try:
                await r.delete(status_key)
            except Exception:
                logger.exception("EXECUTION redis rollback status failed trade_id=%s", tid)
            return {"status": "error", "trade_id": tid, "reason": "redis_write_failed", "error": str(e)}

        pending_trades.add(tid)

    out: dict[str, Any]
    try:
        if mode == "live":
            out = await _live_submit(order, risk=risk, r=r, status_key=status_key)
        else:
            out = await _paper_fill(order, risk=risk)
    except Exception as e:
        logger.exception("EXECUTION failed trade_id=%s", tid)
        try:
            await r.delete(status_key)
            await r.delete(pending_key)
        except Exception:
            logger.exception("EXECUTION redis delete after exception trade_id=%s", tid)
        async with _flow_lock:
            pending_trades.discard(tid)
        _log_execution_safe_final(
            tid, symbol, side, size, price, risk, "cleared", mode, order_meta
        )
        return {"status": "error", "trade_id": tid, "error": str(e), "risk_usd": risk}

    try:
        if out.get("status") == "error":
            await r.delete(status_key)
            await r.delete(pending_key)
            async with _flow_lock:
                pending_trades.discard(tid)
            _log_execution_safe_final(
                tid, symbol, side, size, price, risk, "cleared", mode, order_meta
            )
            return out

        if out.get("status") == "skipped" and out.get("reason") == "already_done":
            try:
                await r.delete(pending_key)
            except Exception:
                logger.exception("EXECUTION redis delete pending marker trade_id=%s", tid)
            async with _flow_lock:
                pending_trades.discard(tid)
            _log_execution_safe_final(
                tid, symbol, side, size, price, risk, "done", mode, order_meta
            )
            return out

        if out.get("status") == "skipped":
            try:
                await r.delete(status_key)
                await r.delete(pending_key)
            except Exception:
                logger.exception("EXECUTION redis rollback unknown skip trade_id=%s", tid)
            async with _flow_lock:
                pending_trades.discard(tid)
            _log_execution_safe_final(
                tid, symbol, side, size, price, risk, "cleared", mode, order_meta
            )
            return out

        pipe = r.pipeline(transaction=True)
        pipe.set(status_key, "done", ex=86400)
        pipe.sadd(EXECUTED_TRADES_REDIS_KEY, tid)
        pipe.delete(pending_key)
        await pipe.execute()
    except Exception as e:
        logger.exception("EXECUTION finalize redis failed trade_id=%s", tid)
        async with _flow_lock:
            pending_trades.discard(tid)
        return {"status": "error", "trade_id": tid, "error": str(e), "partial": out, "risk_usd": risk}

    async with _flow_lock:
        pending_trades.discard(tid)

    _log_execution_safe_final(tid, symbol, side, size, price, risk, "done", mode, order_meta)
    return out
