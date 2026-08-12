from datetime import datetime, timezone

from app.services.news_guard import evaluate, normalize_event


def test_high_risk_event_blocks_during_window():
    now = datetime(2026, 8, 12, 12, 25, tzinfo=timezone.utc)
    state = evaluate(
        [
            {
                "id": "cpi",
                "title": "US CPI",
                "at": "2026-08-12T12:30:00+00:00",
                "risk": "CRITICAL",
                "currencies": ["USD"],
            }
        ],
        symbol="XAUUSD",
        now=now,
    )
    assert state["status"] == "OFF"
    assert state["risk"] == "CRITICAL"


def test_unrelated_currency_does_not_block_symbol():
    now = datetime(2026, 8, 12, 12, 25, tzinfo=timezone.utc)
    state = evaluate(
        [
            {
                "title": "UK event",
                "at": "2026-08-12T12:30:00+00:00",
                "risk": "HIGH",
                "currencies": ["GBP"],
            }
        ],
        symbol="XAUUSD",
        now=now,
    )
    assert state["status"] == "ON"


def test_symbol_override_targets_exact_instrument():
    now = datetime(2026, 8, 12, 12, 25, tzinfo=timezone.utc)
    state = evaluate(
        [
            {
                "title": "Metal liquidity event",
                "at": "2026-08-12T12:30:00+00:00",
                "risk": "HIGH",
                "symbols": ["XAGUSD"],
            }
        ],
        symbol="XAUUSD",
        now=now,
    )
    assert state["status"] == "ON"


def test_french_risk_aliases_are_normalized():
    event = normalize_event(
        {
            "title": "Inflation",
            "at": "2026-08-12T12:30:00Z",
            "risk": "CRITIQUE",
            "currencies": ["usd"],
        }
    )
    assert event["risk"] == "CRITICAL"
    assert event["currencies"] == ["USD"]
