from __future__ import annotations

import logging
import time
from typing import Any
from urllib.parse import urlparse

import httpx

from app.config import settings

logger = logging.getLogger(__name__)


def heuristic_category(url: str, content_type: str | None) -> tuple[str, float]:
    u = url.lower()
    ct = (content_type or "").lower()
    score = 0.55
    cat = "public"
    if any(k in u for k in ("traffic", "dot", "511", "cctv", "jam", "highway", "autoroute")):
        cat, score = "traffic", 0.78
    if any(k in u for k in ("weather", "radar", "satellite", "noaa", "meteo", "rain", "wind")):
        cat, score = "weather", 0.76
    if "m3u8" in u or "hls" in u or ct.startswith("video") or "application/vnd.apple.mpegurl" in ct:
        cat, score = cat if cat != "public" else "public", min(0.95, score + 0.12)
    if u.endswith((".jpg", ".png", ".mjpg", ".cgi")):
        cat, score = "traffic", max(score, 0.66)
    return cat, float(score)


async def llm_classify_stream(url: str, headers_summary: str) -> tuple[str, float] | None:
    if not settings.openai_api_key:
        return None
    try:
        from openai import AsyncOpenAI

        client = AsyncOpenAI(api_key=settings.openai_api_key)
        prompt = (
            "Classify this public sensor/stream URL into exactly one label: "
            "traffic | weather | public | unknown. "
            "Return JSON {\"category\":\"...\",\"confidence\":0-1} only.\n"
            f"URL: {url}\nHEADERS: {headers_summary[:1200]}\n"
        )
        resp = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
        )
        txt = (resp.choices[0].message.content or "{}").strip()
        import json

        obj = json.loads(txt)
        cat = str(obj.get("category", "unknown")).lower()
        conf = float(obj.get("confidence", 0.0))
        if cat not in ("traffic", "weather", "public", "unknown"):
            cat = "unknown"
        return cat, max(0.0, min(1.0, conf))
    except Exception:
        logger.exception("LLM stream classification failed")
        return None


async def probe_url(url: str) -> dict[str, Any]:
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        return {"ok": False, "reason": "unsupported_scheme", "status": None, "latency_ms": None, "ctype": None}

    t0 = time.perf_counter()
    try:
        async with httpx.AsyncClient(timeout=12.0, follow_redirects=True) as client:
            r = await client.head(url)
            if r.status_code >= 400 or r.status_code == 405:
                r = await client.get(url, headers={"Range": "bytes=0-0"})
            latency_ms = (time.perf_counter() - t0) * 1000.0
            ctype = r.headers.get("content-type")
            ok = r.status_code < 500
            return {
                "ok": ok and r.status_code < 400,
                "status": r.status_code,
                "latency_ms": latency_ms,
                "ctype": ctype,
            }
    except Exception as exc:
        latency_ms = (time.perf_counter() - t0) * 1000.0
        return {"ok": False, "reason": str(exc), "status": None, "latency_ms": latency_ms, "ctype": None}


async def validate_camera_row(row: dict[str, Any]) -> dict[str, Any]:
    url = row.get("stream_url") or row.get("info_url")
    if not url:
        return {"health_state": "down", "http_status": None, "latency_ms": None, "category": "unknown", "conf": 0.0}
    pr = await probe_url(str(url))
    ok = bool(pr.get("ok"))
    health = "ok" if ok else "degraded" if pr.get("status") in (401, 403, 429) else "down"
    cat, conf = heuristic_category(str(url), pr.get("ctype"))
    llm = await llm_classify_stream(str(url), str(pr.get("ctype", "")))
    if llm:
        cat, conf = llm
    return {
        "health_state": health,
        "http_status": pr.get("status"),
        "latency_ms": pr.get("latency_ms"),
        "category": cat,
        "conf": conf,
    }
