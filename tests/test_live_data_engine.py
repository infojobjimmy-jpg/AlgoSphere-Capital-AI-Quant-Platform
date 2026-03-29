"""Tests for Live Data Engine (read-only ingestion; no trading)."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


from backend import live_data_engine as lde  # noqa: E402
from backend.live_data_engine import (  # noqa: E402
    LIVE_HEALTHY,
    LIVE_OFFLINE,
    compute_volatility_trend,
    fetch_symbol_raw,
    refresh_once,
)


class LiveDataAggregationTests(unittest.TestCase):
    def test_compute_volatility_trend_uptrend(self) -> None:
        prices = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0]
        vol, trend, hint = compute_volatility_trend(prices)
        self.assertEqual(trend, "UP")
        self.assertGreater(vol, 0.0)
        self.assertTrue(hint)

    def test_compute_volatility_trend_flat_short(self) -> None:
        vol, trend, hint = compute_volatility_trend([100.0])
        self.assertEqual(trend, "FLAT")
        self.assertEqual(vol, 0.0)


class LiveDataFetchTests(unittest.TestCase):
    def test_fetch_binance_btcusd(self) -> None:
        class Resp:
            def raise_for_status(self) -> None:
                pass

            def json(self) -> dict:
                return {"lastPrice": "42000.5", "quoteVolume": "12345.6"}

        def fake_get(url: str, timeout: float = 8.0, headers=None) -> Resp:
            self.assertIn("binance.com", url)
            self.assertIn("BTCUSDT", url)
            return Resp()

        with patch.object(lde, "_http_get", fake_get):
            raw, src = fetch_symbol_raw("BTCUSD")
        self.assertIsNotNone(raw)
        assert raw is not None
        self.assertEqual(src, "binance")
        self.assertAlmostEqual(raw["price"], 42000.5)
        self.assertGreater(raw["volume"], 0.0)

    def test_fetch_fallback_coinbase_when_binance_fails(self) -> None:
        def fake_get(url: str, timeout: float = 8.0, headers=None) -> object:
            if "binance.com" in url:
                raise ConnectionError("down")

            class Resp:
                def raise_for_status(self) -> None:
                    pass

                def json(self) -> dict:
                    return {"data": {"amount": "2500.25"}}

            self.assertIn("coinbase.com", url)
            return Resp()

        with patch.object(lde, "_http_get", fake_get):
            raw, src = fetch_symbol_raw("ETHUSD")
        self.assertIsNotNone(raw)
        assert raw is not None
        self.assertEqual(src, "coinbase")
        self.assertAlmostEqual(raw["price"], 2500.25)


class LiveDataRefreshTests(unittest.TestCase):
    def test_refresh_once_safe_when_all_fetch_fail(self) -> None:
        holder = [lde._default_state()]

        def load() -> dict:
            return holder[0]

        def persist(s: dict) -> None:
            holder[0] = s

        def boom(*_a: object, **_k: object) -> None:
            raise OSError("network")

        with patch.object(lde, "load_persisted_state", load):
            with patch.object(lde, "_persist_state", persist):
                with patch.object(lde, "fetch_symbol_raw", boom):
                    st = refresh_once()
        self.assertEqual(st.get("data_health"), LIVE_OFFLINE)

    def test_refresh_once_populates_when_fetch_ok(self) -> None:
        holder = [lde._default_state()]

        def load() -> dict:
            return holder[0]

        def persist(s: dict) -> None:
            holder[0] = s

        base = {"BTCUSD": 50000.0, "ETHUSD": 3000.0, "XAUUSD": 2000.0, "SPX": 5000.0, "DXY": 104.0}
        legacy_syms = ("BTCUSD", "ETHUSD", "XAUUSD", "SPX", "DXY")

        def fake_fetch(sym: str):
            p = base.get(sym)
            if p is None:
                return None, ""
            return {"price": p, "volume": 100.0, "closes": [p * 0.99, p * 0.995, p]}, "mock"

        with patch.object(lde, "load_persisted_state", load):
            with patch.object(lde, "_persist_state", persist):
                with patch.object(lde, "get_live_symbols", return_value=legacy_syms):
                    with patch.object(lde, "fetch_symbol_raw", side_effect=fake_fetch):
                        st = refresh_once()
        self.assertEqual(st.get("data_health"), LIVE_HEALTHY)
        self.assertIn("BTCUSD", st.get("symbols") or {})


class LiveDataEndpointTests(unittest.TestCase):
    def test_live_api_handlers_shape(self) -> None:
        """Same payloads as GET /live/* (no httpx TestClient dependency)."""
        from backend.main import get_market_payload, get_status_payload, get_symbol_payload

        body = get_status_payload()
        self.assertIn("symbols_tracked", body)
        self.assertIn("data_health", body)
        self.assertIn("sources", body)

        m = get_market_payload()
        self.assertIn("symbols", m)
        self.assertIsInstance(m["symbols"], list)

        sym = get_symbol_payload("BTCUSD")
        self.assertIn("symbol_data", sym)


class LiveDataBackgroundLoopTests(unittest.TestCase):
    def test_background_loop_swallows_refresh_errors(self) -> None:
        class StopEv:
            def __init__(self) -> None:
                self._n = 0

            def wait(self, timeout: float | None = None) -> bool:
                self._n += 1
                return self._n > 1

        fake_stop = StopEv()
        with patch.object(lde, "_bg_stop", fake_stop):
            with patch.object(lde, "refresh_once", side_effect=RuntimeError("simulated")):
                with patch.object(lde.logger, "exception"):
                    lde._background_loop(1.0)


if __name__ == "__main__":
    unittest.main()
