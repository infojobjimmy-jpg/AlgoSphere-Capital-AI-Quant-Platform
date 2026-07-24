from __future__ import annotations

import hashlib
import json
import logging
import re
from typing import Any
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.memory.chroma_memory import get_chroma_client

logger = logging.getLogger(__name__)

DATA_KEYWORDS = re.compile(
    r"(dataset|api|download|csv|json|geojson|kml|wms|wfs|atom|rss|feed|stac|sparql)",
    re.I,
)


async def tavily_search(query: str, max_results: int = 8) -> list[dict[str, Any]]:
    if not settings.tavily_api_key:
        return []
    try:
        async with httpx.AsyncClient(timeout=40.0) as client:
            r = await client.post(
                "https://api.tavily.com/search",
                json={
                    "api_key": settings.tavily_api_key,
                    "query": query,
                    "search_depth": "advanced",
                    "max_results": max_results,
                },
            )
            r.raise_for_status()
            data = r.json()
        return list(data.get("results") or [])
    except Exception:
        logger.exception("tavily search failed")
        return []


async def serp_search(query: str) -> list[dict[str, Any]]:
    if not settings.serpapi_key:
        return []
    try:
        async with httpx.AsyncClient(timeout=40.0) as client:
            r = await client.get(
                "https://serpapi.com/search.json",
                params={"engine": "google", "q": query, "api_key": settings.serpapi_key, "num": 8},
            )
            r.raise_for_status()
            data = r.json()
        out: list[dict[str, Any]] = []
        for it in data.get("organic_results") or []:
            out.append({"url": it.get("link"), "title": it.get("title"), "content": it.get("snippet", "")})
        return [x for x in out if x.get("url")]
    except Exception:
        logger.exception("serpapi search failed")
        return []


def composite_score(url: str, title: str, snippet: str) -> tuple[float, float, float]:
    u = (url or "").lower()
    t = (title or "").lower()
    s = (snippet or "").lower()
    richness = 0.0
    if DATA_KEYWORDS.search(u) or DATA_KEYWORDS.search(t) or DATA_KEYWORDS.search(s):
        richness += 0.45
    if u.endswith((".json", ".csv", ".geojson", ".zip", ".parquet")):
        richness += 0.25
    if any(h in u for h in ("data.gov", "opendata", "europa.eu", "ckan", "stac", "aws")):
        richness += 0.2

    reliability = 0.35
    if "gov" in urlparse(u).netloc or "europa.eu" in u:
        reliability += 0.35
    if "github.com" in u or "gitlab" in u:
        reliability += 0.1

    update_frequency = 0.25
    if any(k in t + s for k in ("hourly", "real-time", "realtime", "live", "streaming", "updates")):
        update_frequency += 0.35
    if "api" in u:
        update_frequency += 0.25

    return min(1.0, richness), min(1.0, reliability), min(1.0, update_frequency)


async def llm_final_score(url: str, title: str, snippet: str) -> float | None:
    if not settings.openai_api_key:
        return None
    try:
        from openai import AsyncOpenAI

        client = AsyncOpenAI(api_key=settings.openai_api_key)
        prompt = (
            "Score 0.0-1.0 how valuable this public data source is for automated global fusion. "
            "Return a number only.\n"
            f"Title: {title}\nURL: {url}\nSnippet: {snippet[:1200]}\n"
        )
        resp = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
        )
        txt = (resp.choices[0].message.content or "0").strip()
        return max(0.0, min(1.0, float(txt.split()[0])))
    except Exception:
        logger.exception("llm final score failed")
        return None


def upsert_chroma(records: list[dict[str, Any]]) -> None:
    if not records:
        return
    try:
        client = get_chroma_client()
        coll = client.get_or_create_collection(name="public_sources", metadata={"hnsw:space": "cosine"})
        ids = []
        docs = []
        metas = []
        for item in records:
            digest = hashlib.sha256(item["url"].encode("utf-8")).hexdigest()
            ids.append(digest)
            docs.append(f"{item.get('title','')}\n{item['url']}\n{item.get('snippet','')}")
            metas.append(
                {
                    "url": item["url"],
                    "richness": float(item.get("richness", 0.0)),
                    "reliability": float(item.get("reliability", 0.0)),
                    "update_frequency": float(item.get("update_frequency", 0.0)),
                    "score": float(item.get("score", 0.0)),
                }
            )
        coll.upsert(ids=ids, documents=docs, metadatas=metas)
    except Exception:
        logger.exception("chroma upsert failed")


async def crawl_seed(url: str, limit: int = 30) -> list[dict[str, Any]]:
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        r = await client.get(url)
        r.raise_for_status()
        ctype = r.headers.get("content-type", "")
        if "html" not in ctype and "text" not in ctype:
            return []
        soup = BeautifulSoup(r.text, "lxml")
    found: list[dict[str, Any]] = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        title = (a.get_text() or "").strip()[:240]
        if not href.startswith("http"):
            continue
        if urlparse(href).netloc != urlparse(url).netloc:
            if not any(
                d in href for d in ("data.gov", "amazonaws.com", "opendata", "ckan", "stac", "europa")
            ):
                continue
        rich, rel, upd = composite_score(href, title, "")
        score = 0.45 * rich + 0.35 * rel + 0.20 * upd
        found.append({"url": href, "title": title, "snippet": "", "richness": rich, "reliability": rel, "update_frequency": upd, "score": score})
        if len(found) >= limit:
            break
    found.sort(key=lambda x: x["score"], reverse=True)
    return found[:limit]


async def run_discovery_v2(session: AsyncSession, query: str | None) -> None:
    seeds = [s.strip() for s in settings.discovery_seed_urls.split(",") if s.strip()]
    aggregated: list[dict[str, Any]] = []

    q = query or "open public datasets APIs geospatial JSON feeds"
    aggregated.extend(await tavily_search(q))
    aggregated.extend(await serp_search(q))

    for s in seeds[:4]:
        try:
            aggregated.extend(await crawl_seed(s, limit=25))
        except Exception:
            logger.exception("seed crawl failed: %s", s)

    normalized: list[dict[str, Any]] = []
    for it in aggregated:
        url = it.get("url")
        if not url:
            continue
        title = str(it.get("title", ""))
        snippet = str(it.get("content", it.get("snippet", "")))
        rich, rel, upd = composite_score(url, title, snippet)
        base = 0.45 * rich + 0.35 * rel + 0.20 * upd
        llm = await llm_final_score(url, title, snippet)
        score = float(llm) if llm is not None else float(base)
        normalized.append(
            {
                "url": url,
                "title": title,
                "snippet": snippet[:2000],
                "richness": rich,
                "reliability": rel,
                "update_frequency": upd,
                "score": score,
            }
        )

    normalized.sort(key=lambda x: float(x.get("score", 0.0)), reverse=True)
    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for it in normalized:
        u = str(it["url"])
        if u in seen:
            continue
        seen.add(u)
        deduped.append(it)

    upsert_chroma(deduped[:200])

    await session.execute(
        text("INSERT INTO discovery_runs (query, results) VALUES (:q, CAST(:r AS jsonb))"),
        {"q": q, "r": json.dumps(deduped[:200])},
    )

    for item in deduped[:120]:
        score = float(item.get("score", 0.0))
        if score < 0.42:
            continue
        digest = hashlib.sha256(item["url"].encode("utf-8")).hexdigest()[:24]
        await session.execute(
            text(
                """
                INSERT INTO sources (name, source_type, endpoint, metadata, relevance_score,
                    richness_score, reliability_score, update_frequency_score, last_seen_at)
                SELECT :name, 'discovered_v2', :endpoint,
                    CAST(:meta AS jsonb), :score, :rich, :rel, :upd, NOW()
                WHERE NOT EXISTS (SELECT 1 FROM sources WHERE endpoint = :endpoint)
                """
            ),
            {
                "name": (item.get("title") or digest)[:500],
                "endpoint": item["url"][:2000],
                "meta": json.dumps({"digest": digest, "engine": "v2"}),
                "score": score,
                "rich": float(item.get("richness", 0.0)),
                "rel": float(item.get("reliability", 0.0)),
                "upd": float(item.get("update_frequency", 0.0)),
            },
        )

    await session.commit()
