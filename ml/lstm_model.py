from dataclasses import dataclass
from typing import Sequence


@dataclass
class LSTMPlaceholder:
    """
    Placeholder interface for future sequence model.
    """

    lookback: int = 5

    def predict_next(self, series: Sequence[float]) -> float:
        if not series:
            return 0.0
        window = list(series)[-self.lookback :]
        return round(sum(window) / len(window), 4)
