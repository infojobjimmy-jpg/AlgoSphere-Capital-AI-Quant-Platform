from __future__ import annotations

from app.config import settings
from app.trading.brokers.base import BrokerAdapter, BrokerError
from app.trading.brokers.ctrader import CTraderAdapter
from app.trading.brokers.mt5 import MT5Adapter


def get_live_broker_adapter() -> BrokerAdapter:
    broker = (settings.trading_live_broker or "mt5").strip().lower()
    if broker == "mt5":
        return MT5Adapter()
    if broker == "ctrader":
        return CTraderAdapter()
    raise BrokerError(f"unsupported_live_broker:{broker}")
