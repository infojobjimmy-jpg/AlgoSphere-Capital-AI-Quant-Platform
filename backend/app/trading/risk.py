from __future__ import annotations

from app.config import settings


class RiskManager:
    """Mandatory risk gates before any paper (or future live) execution."""

    def __init__(
        self,
        *,
        equity_usd: float,
        cash_usd: float,
        peak_equity_usd: float,
        initial_capital_usd: float | None = None,
    ) -> None:
        self.equity_usd = float(equity_usd)
        self.cash_usd = float(cash_usd)
        self.peak_equity_usd = max(float(peak_equity_usd), 1.0)
        self.initial_capital_usd = max(
            float(initial_capital_usd if initial_capital_usd is not None else self.peak_equity_usd),
            1.0,
        )

    def allow_order(
        self,
        *,
        notional_usd: float,
        symbol: str,
        kill_switch: bool = False,
        drawdown_soft: bool = False,
    ) -> tuple[bool, str]:
        if kill_switch:
            return False, "kill_switch_engaged"
        if notional_usd <= 0:
            return False, "invalid_notional"
        max_trade = self.equity_usd * (settings.trading_max_risk_per_trade_pct / 100.0)
        if notional_usd > max_trade + 1e-6:
            return False, f"notional_exceeds_{settings.trading_max_risk_per_trade_pct}%_equity"
        if notional_usd > self.cash_usd + 1e-6:
            return False, "insufficient_cash"
        dd = 100.0 * (1.0 - self.equity_usd / self.peak_equity_usd)
        if dd >= float(settings.trading_emergency_stop_dd_pct):
            return False, "emergency_stop_drawdown"
        if dd >= float(settings.trading_global_drawdown_kill_pct):
            if drawdown_soft:
                return True, "ok_soft_global_dd"
            return False, "global_drawdown_gate"
        floor_ratio = max(0.0, min(1.0, float(settings.trading_capital_floor_pct) / 100.0))
        if self.equity_usd < self.initial_capital_usd * floor_ratio:
            return False, "capital_protection_floor"
        _ = symbol
        return True, "ok"

    def stop_price(self, *, side: str, entry: float) -> float:
        """Mandatory protective stop (percent distance from entry)."""
        pct = settings.trading_default_stop_pct / 100.0
        if side == "buy":
            return float(entry) * (1.0 - pct)
        return float(entry) * (1.0 + pct)
