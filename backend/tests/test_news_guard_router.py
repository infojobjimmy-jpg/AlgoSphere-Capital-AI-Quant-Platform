import json

import pytest

from app.routers import news_guard as router
from app.config import settings
from app.services import news_guard_notify


class FakeRedis:
    def __init__(self, values=None):
        self.values = values or {}
        self.set_calls = []
        self.deleted = []

    async def get(self, key):
        return self.values.get(key)

    async def set(self, key, value, **kwargs):
        self.set_calls.append((key, value, kwargs))
        return True

    async def delete(self, key):
        self.deleted.append(key)
        return 1

    async def aclose(self):
        return None


@pytest.mark.asyncio
async def test_health_reports_ready_configuration(monkeypatch):
    fake = FakeRedis(
        {
            settings.redis_news_guard_events_key(): json.dumps([{"id": "cpi"}]),
            f"{settings.app_slug}:news_guard:last_refresh": "2026-08-12T17:00:00+00:00",
        }
    )
    monkeypatch.setattr(router.redis, "from_url", lambda *args, **kwargs: fake)

    async def fake_read_state(_client, **kwargs):
        return {"status": "ON", "risk": "LOW", "reason": None}

    monkeypatch.setattr(router, "read_state", fake_read_state)
    monkeypatch.setattr(settings, "news_guard_enabled", True)
    monkeypatch.setattr(settings, "news_guard_calendar_provider", "finnhub")
    monkeypatch.setattr(settings, "news_guard_max_stale_sec", 900)
    monkeypatch.setattr(settings, "finnhub_api_key", "test-key")
    monkeypatch.setattr(settings, "mt5_api_token", "token")
    monkeypatch.setattr(settings, "mt5_bridge_url", "http://mt5-bridge")
    monkeypatch.setattr(settings, "news_guard_discord_webhook_url", "https://discord.invalid/webhook")
    monkeypatch.setattr(settings, "twilio_account_sid", None)
    monkeypatch.setattr(settings, "twilio_auth_token", None)
    monkeypatch.setattr(settings, "twilio_from_number", None)
    monkeypatch.setattr(settings, "news_guard_sms_to", None)

    result = await router.health()

    assert result["ready"] is True
    assert result["calendar"]["source_configured"] is True
    assert result["calendar"]["event_count"] == 1
    assert result["calendar"]["max_stale_sec"] == 900
    assert result["notifications"]["discord_configured"] is True
    assert result["notifications"]["sms_configured"] is False
    assert result["mt5"]["token_configured"] is True
    assert result["mt5"]["bridge_url_configured"] is True


@pytest.mark.asyncio
async def test_unconfigured_notification_channels_return_without_network(monkeypatch):
    monkeypatch.setattr(settings, "news_guard_discord_webhook_url", None)
    monkeypatch.setattr(settings, "twilio_account_sid", None)
    monkeypatch.setattr(settings, "twilio_auth_token", None)
    monkeypatch.setattr(settings, "twilio_from_number", None)
    monkeypatch.setattr(settings, "news_guard_sms_to", None)

    discord = await news_guard_notify.send_discord("test")
    sms = await news_guard_notify.send_sms("test")

    assert discord == {"configured": False, "sent": False}
    assert sms == {"configured": False, "sent": False}


@pytest.mark.asyncio
async def test_tminus_dry_run_never_claims_dedupe(monkeypatch):
    fake = FakeRedis()
    monkeypatch.setattr(router.redis, "from_url", lambda *args, **kwargs: fake)

    async def fake_read_state(_client, **kwargs):
        return {
            "status": "ON",
            "risk": "LOW",
            "next_event": {
                "id": "nfp",
                "title": "US NFP",
                "at": "2026-08-12T18:30:00+00:00",
                "risk": "CRITICAL",
                "currencies": ["USD"],
            },
        }

    monkeypatch.setattr(router, "read_state", fake_read_state)
    monkeypatch.setattr(router, "minutes_until", lambda value: 10.0)

    result = await router.notify(kind="tminus", sms=False, dry_run=True)

    assert result["dry_run"] is True
    assert fake.set_calls == []
    assert fake.deleted == []


@pytest.mark.asyncio
async def test_tminus_failed_delivery_releases_dedupe(monkeypatch):
    fake = FakeRedis()
    monkeypatch.setattr(router.redis, "from_url", lambda *args, **kwargs: fake)

    async def fake_read_state(_client, **kwargs):
        return {
            "status": "ON",
            "risk": "LOW",
            "next_event": {
                "id": "cpi",
                "title": "US CPI",
                "at": "2026-08-12T18:30:00+00:00",
                "risk": "HIGH",
                "currencies": ["USD"],
            },
        }

    async def fake_dispatch(message, *, sms=False):
        return {"discord": {"configured": True, "sent": False}}

    monkeypatch.setattr(router, "read_state", fake_read_state)
    monkeypatch.setattr(router, "minutes_until", lambda value: 10.0)
    monkeypatch.setattr(router, "dispatch", fake_dispatch)

    result = await router.notify(kind="tminus", sms=False, dry_run=False)

    assert result["sent"] is False
    assert result["reason"] == "no_notification_channel_delivered"
    assert len(fake.set_calls) == 1
    dedupe_key = fake.set_calls[0][0]
    assert dedupe_key in fake.deleted


@pytest.mark.asyncio
async def test_tminus_success_keeps_dedupe(monkeypatch):
    fake = FakeRedis()
    monkeypatch.setattr(router.redis, "from_url", lambda *args, **kwargs: fake)

    async def fake_read_state(_client, **kwargs):
        return {
            "status": "ON",
            "risk": "LOW",
            "next_event": {
                "id": "ppi",
                "title": "US PPI",
                "at": "2026-08-12T18:30:00+00:00",
                "risk": "HIGH",
                "currencies": ["USD"],
            },
        }

    async def fake_dispatch(message, *, sms=False):
        return {"discord": {"configured": True, "sent": True}}

    monkeypatch.setattr(router, "read_state", fake_read_state)
    monkeypatch.setattr(router, "minutes_until", lambda value: 10.0)
    monkeypatch.setattr(router, "dispatch", fake_dispatch)

    result = await router.notify(kind="tminus", sms=False, dry_run=False)

    assert result["sent"] is True
    assert len(fake.set_calls) == 1
    assert fake.deleted == []
