from dataclasses import dataclass
from typing import Sequence


@dataclass
class BaselineModel:
    """
    Minimal baseline model.
    Easy to replace with any future ML model implementation.
    """

    threshold: float = 0.0

    def fit(self, values: Sequence[float]) -> None:
        if values:
            self.threshold = sum(values) / len(values)

    def predict_score(self, value: float) -> float:
        return round(value - self.threshold, 4)
