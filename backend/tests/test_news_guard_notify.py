import pytest

from app.config import settings
from app.services import news_guard_notify


def test_format_brief_lists_multiple_events_in_quebec_time(monkeypatch):
    monkeypatch.setattr(settings, "news_guard_timezone", "America/Montreal")
    state = {
        "status": "ON",
        "risk": "LOW",
        "active_events": [],
        "upcoming_events": [
            {
                "title": "US CPI",
                "at": "2026-08-12T12:30:00+00:00",
                "risk": "CRITICAL",
                "currencies": ["USD"],
                "off_before_min": 30,
                "off_after_min": 75,
                "meta": {"estimate": 2.7, "previous": 2.9},
            },
            {
                "title": "US EIA Inventories",
                "at": "2026-08-12T14:30:00+00:00",
                "risk": "HIGH",
                "currencies": ["USD"],
                "off_before_min": 15,
                "off_after_min": 45,
                "meta": {},
            },
        ],
    }

    message = news_guard_notify.format_brief(state)

    assert "US CPI" in message
    assert "US EIA Inventories" in message
    assert "2026-08-12 08:30 Québec" in message
    assert "est. 2.7" in message
    assert "préc. 2.9" in message
    assert "fenêtre OFF -30 / +75 min" in message


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
