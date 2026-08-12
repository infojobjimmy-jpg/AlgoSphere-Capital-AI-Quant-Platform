import pytest

from app.trading import execution


class FakeRedis:
    async def get(self, key):
        return None


@pytest.mark.asyncio
async def test_live_execution_never_reaches_broker_when_news_guard_off(monkeypatch):
    signals = [
        {
            "symbol": "XAUUSD",
            "side": "buy",
            "confidence": 0.99,
            "meta": {"price": 2400.0},
        }
    ]

    monkeypatch.setattr(execution, "choose_trade_signal", lambda rows: rows[0])

    async def guard_off(_redis, *, symbol=None):
        return {
            "status": "OFF",
            "risk": "CRITICAL",
            "reason": "economic_event",
            "symbol": symbol,
        }

    monkeypatch.setattr(execution, "read_news_guard_state", guard_off)

    def broker_must_not_be_created():
        raise AssertionError("broker adapter must not be created while News Guard is OFF")

    monkeypatch.setattr(execution, "get_live_broker_adapter", broker_must_not_be_created)

    await execution._execute_live_from_signals(FakeRedis(), object(), signals)


@pytest.mark.asyncio
async def test_live_execution_fail_safe_off_never_reaches_broker(monkeypatch):
    signals = [
        {
            "symbol": "XAGUSD",
            "side": "sell",
            "confidence": 0.95,
            "meta": {"price": 30.0},
        }
    ]

    monkeypatch.setattr(execution, "choose_trade_signal", lambda rows: rows[0])

    async def stale_guard(_redis, *, symbol=None):
        return {
            "status": "OFF",
            "risk": "CRITICAL",
            "reason": "news_guard_calendar_stale",
            "symbol": symbol,
        }

    monkeypatch.setattr(execution, "read_news_guard_state", stale_guard)

    def broker_must_not_be_created():
        raise AssertionError("broker adapter must not be created during News Guard fail-safe")

    monkeypatch.setattr(execution, "get_live_broker_adapter", broker_must_not_be_created)

    await execution._execute_live_from_signals(FakeRedis(), object(), signals)
