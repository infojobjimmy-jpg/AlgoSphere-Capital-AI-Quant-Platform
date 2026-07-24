from __future__ import annotations

from collections.abc import Sequence


def linear_next(values: Sequence[float]) -> tuple[float, float]:
    """
    Minimal online predictor: next-step estimate and slope from a short window.
    Replace with calibrated probabilistic models (GLM, GBDT, temporal CNN) as data volume grows.
    """
    ys = [float(x) for x in values if x == x]
    if len(ys) < 3:
        y = ys[-1] if ys else 0.0
        return y, 0.0
    xs = list(range(len(ys)))
    n = float(len(xs))
    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys, strict=False))
    den = sum((x - mx) ** 2 for x in xs) or 1.0
    slope = num / den
    next_x = len(xs)
    next_y = my + slope * (next_x - mx)
    return max(0.0, float(next_y)), float(slope)
