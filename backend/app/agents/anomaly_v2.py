from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np

from app.fusion.h3_index import cell, counts_by_cell


@dataclass
class AnomalySignalV2:
    severity: str
    title: str
    body: str
    payload: dict[str, Any]


def _ewma_residual(series: list[float], alpha: float = 0.12) -> float:
    if not series:
        return 0.0
    ema = series[0]
    for x in series[1:]:
        ema = alpha * x + (1 - alpha) * ema
    x = series[-1]
    return float(x - ema)


def _isolation_score_row(
    row: np.ndarray, history: list[np.ndarray], contamination: float = 0.06
) -> float | None:
    if len(history) < 12:
        return None
    try:
        from sklearn.ensemble import IsolationForest

        X = np.vstack(history[-200:])
        if X.shape[0] < 12 or X.shape[1] != row.shape[0]:
            return None
        clf = IsolationForest(
            n_estimators=128,
            contamination=float(np.clip(contamination, 0.03, 0.12)),
            random_state=42,
            n_jobs=1,
        )
        clf.fit(X)
        s = float(clf.decision_function(row.reshape(1, -1))[0])
        return -s
    except Exception:
        return None


def build_feature_vector(
    aircraft: list[dict[str, Any]],
    ships: list[dict[str, Any]],
    weather: list[dict[str, Any]],
    resolution: int,
) -> np.ndarray:
    ac = len(aircraft)
    sh = len(ships)
    mx_wind = max((float(w.get("wind_mps") or 0.0) for w in weather), default=0.0)
    cells = counts_by_cell(aircraft, resolution)
    ent = 0.0
    if cells:
        tot = float(sum(cells.values()))
        for c in cells.values():
            p = c / tot
            ent -= p * float(np.log(p + 1e-9))
    return np.array(
        [np.log1p(ac), np.log1p(sh), mx_wind, ent],
        dtype=float,
    )


def detect_multivariate_anomalies(
    aircraft: list[dict[str, Any]],
    ships: list[dict[str, Any]],
    weather: list[dict[str, Any]],
    feat_history: list[np.ndarray],
    resolution: int,
    *,
    contamination: float = 0.06,
    drift_scale: float = 0.35,
    iso_warn: float = 0.25,
    iso_critical: float = 0.45,
) -> tuple[list[AnomalySignalV2], list[np.ndarray]]:
    vec = build_feature_vector(aircraft, ships, weather, resolution)
    hist = feat_history + [vec]
    hist = hist[-240:]

    signals: list[AnomalySignalV2] = []

    counts_hist = [float(h[0]) for h in hist if h.size]
    resid = abs(_ewma_residual(counts_hist[-60:])) if len(counts_hist) >= 8 else 0.0
    if resid >= drift_scale * (np.mean(counts_hist[-60:]) + 1e-6):
        signals.append(
            AnomalySignalV2(
                severity="warning",
                title="Temporal traffic drift",
                body="Aircraft volume diverged from EWMA baseline across the fused window.",
                payload={"residual": resid, "aircraft": len(aircraft)},
            )
        )

    iso = _isolation_score_row(vec, hist[:-1], contamination=contamination)
    if iso is not None and iso > iso_warn:
        signals.append(
            AnomalySignalV2(
                severity="critical" if iso > iso_critical else "warning",
                title="Multi-signal anomaly",
                body="Isolation model flagged unusual coupling of air, maritime, wind, and dispersion entropy.",
                payload={"isolation": iso, "vector": vec.tolist()},
            )
        )

    return signals, hist


def density_importance(aircraft: list[dict[str, Any]], resolution: int) -> dict[str, float]:
    cells = counts_by_cell(aircraft, resolution)
    if not cells:
        return {}
    mx = max(cells.values()) or 1
    out: dict[str, float] = {}
    for p in aircraft:
        icao = str(p.get("id", ""))
        if not icao:
            continue
        lat = float(p.get("lat", 0.0))
        lon = float(p.get("lon", 0.0))
        try:
            h = cell(lat, lon, resolution)
            out[icao] = min(1.0, float(cells.get(h, 0)) / float(mx))
        except Exception:
            out[icao] = 0.0
    return out
