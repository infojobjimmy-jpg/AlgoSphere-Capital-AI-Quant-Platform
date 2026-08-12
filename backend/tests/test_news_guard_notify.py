import pytest

from app.services import news_guard_notify


@pytest.mark.asyncio
async def test_dispatch_keeps_successful_discord_when_sms_fails(monkeypatch):
    async def discord_ok(message):
        return {"configured": True, "sent": True}

    async def sms_fail(message):
        raise RuntimeError("sms unavailable")

    monkeypatch.setattr(news_guard_notify, "send_discord", discord_ok)
    monkeypatch.setattr(news_guard_notify, "send_sms", sms_fail)

    result = await news_guard_notify.dispatch("test", sms=True)

    assert result["discord"]["sent"] is True
    assert result["sms"]["sent"] is False
    assert result["sms"]["error"] == "RuntimeError"


@pytest.mark.asyncio
async def test_dispatch_reports_discord_failure_without_raising(monkeypatch):
    async def discord_fail(message):
        raise TimeoutError("discord timeout")

    monkeypatch.setattr(news_guard_notify, "send_discord", discord_fail)

    result = await news_guard_notify.dispatch("test", sms=False)

    assert result["discord"]["sent"] is False
    assert result["discord"]["error"] == "TimeoutError"
