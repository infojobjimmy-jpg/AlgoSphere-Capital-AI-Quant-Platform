from typing import Sequence

from .ml_model import BaselineModel
from .smc_ai import detect_market_bias
from .strategy_gen import generate_strategy_tag


def build_signal_snapshot(values: Sequence[float]) -> dict[str, float | str]:
    model = BaselineModel()
    model.fit(values)
    last_value = values[-1] if values else 0.0
    score = model.predict_score(last_value)
    strategy = generate_strategy_tag(score, values)
    bias = detect_market_bias(values)
    return {"score": score, "strategy": strategy, "bias": bias}
