"""Post–Cortex tick: derive trading signals and optional paper execution (does not alter fusion)."""

from __future__ import annotations

import logging
from typing import Any

from app.config import settings
from app.db.session import SessionLocal
from app.trading.execution import execute_from_signals, refresh_live_state
from app.trading.paper_exec import persist_signal
from app.trading.signals import derive_signals

logger = logging.getLogger("acap.trading")


async def on_cortex_tick(r: Any, snap: dict[str, Any]) -> None:
    sigs = derive_signals(snap)
    mode_raw = await r.get(settings.redis_trading_mode_key())
    mode = mode_raw or settings.trading_mode
    tmeta = snap.setdefault("meta", {}).setdefault("trading", {})
    tmeta["signals_preview"] = sigs[:16]
    tmeta["mode"] = mode
    tmeta["live_broker"] = settings.trading_live_broker
    if not sigs:
        if str(mode).lower() == "live":
            try:
                async with SessionLocal() as session:
                    await refresh_live_state(r, session)
            except Exception:
                logger.exception("live state refresh failed")
        return
    try:
        async with SessionLocal() as session:
            for s in sigs[:4]:
                await persist_signal(session, s)
            await execute_from_signals(r, session, sigs)
    except Exception:
        logger.exception("trading hook failed")
