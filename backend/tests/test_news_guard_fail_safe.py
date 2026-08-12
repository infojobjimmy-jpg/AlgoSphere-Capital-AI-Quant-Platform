import asyncio

from app.services.news_guard import read_state, save_events


class FakeRedis:
    def __init__(self):
        self.data: dict[str, str] = {}

    async def get(self, key: str):
        return self.data.get(key)

    async def set(self, key: str, value: str, **kwargs):
        if kwargs.get("nx") and key in self.data:
            return False
        self.data[key] = value
        return True


def test_calendar_not_initialized_blocks_in_fail_safe():
    state = asyncio.run(read_state(FakeRedis(), symbol="XAUUSD"))
    assert state["status"] == "OFF"
    assert state["risk"] == "CRITICAL"
    assert state["reason"] == "news_guard_calendar_not_initialized"


def test_fresh_empty_calendar_is_allowed():
    redis = FakeRedis()
    asyncio.run(save_events(redis, []))
    state = asyncio.run(read_state(redis, symbol="XAUUSD"))
    assert state["status"] == "ON"
    assert state["risk"] == "LOW"
    assert "calendar_last_refresh" in state
