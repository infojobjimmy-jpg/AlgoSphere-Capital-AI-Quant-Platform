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


def heuristic_relevance(url: str, title: str) -> float:
    u = url.lower()
    t = title.lower()
    score = 0.0
    if DATA_KEYWORDS.search(u) or DATA_KEYWORDS.search(t):
        score += 0.45
    if any(h in u for h in ("opendata", "data.gov", "europa.eu", "ckan", "stac")):
        score += 0.35
    if u.endswith((".json", ".csv", ".geojson", ".zip", ".parquet")):
        score += 0.25
    return min(1.0, score)


async def llm_classify(url: str, title: str) -> float | None:
    if not settings.openai_api_key:
        return None
    try:
        from openai import AsyncOpenAI

        client = AsyncOpenAI(api_key=settings.openai_api_key)
        prompt = (
            "Rate 0.0-1.0 how useful this URL is as a machine-consumable public data source "
            "for a global situational awareness platform. Reply with a number only.\n"
            f"Title: {title}\nURL: {url}\n"
        )
        resp = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
        )
        txt = (resp.choices[0].message.content or "0").strip()
        return max(0.0, min(1.0, float(txt.split()[0])))
    except Exception:
        logger.exception("llm classify failed")
        return None


async def crawl_seed(url: str, limit: int = 40) -> list[dict[str, Any]]:
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
                d in href
                for d in (
                    "data.gov",
                    "amazonaws.com",
                    "opendata",
                    "ckan",
                    "stac",
                    "europa",
                )
            ):
                continue
        h = heuristic_relevance(href, title)
        if h < 0.2 and not DATA_KEYWORDS.search(href):
            continue
        llm = await llm_classify(href, title)
        score = float(llm) if llm is not None else h
        found.append({"url": href, "title": title, "score": score})
        if len(found) >= limit:
            break
    found.sort(key=lambda x: x["score"], reverse=True)
    return found[:limit]


def upsert_chroma(records: list[dict[str, Any]]) -> None:
    if not records:
        return
    try:
        client = get_chroma_client()
        coll = client.get_or_create_collection(
            name="public_sources",
            metadata={"hnsw:space": "cosine"},
        )
        ids = []
        docs = []
        metas = []
        for item in records:
            digest = hashlib.sha256(item["url"].encode("utf-8")).hexdigest()
            ids.append(digest)
            docs.append(f"{item.get('title','')}\n{item['url']}")
            metas.append({"url": item["url"], "score": float(item.get("score", 0.0))})
        coll.upsert(ids=ids, documents=docs, metadatas=metas)
    except Exception:
        logger.exception("chroma upsert failed")


async def run_discovery_cycle(session: AsyncSession, query: str | None) -> None:
    seeds = [s.strip() for s in settings.discovery_seed_urls.split(",") if s.strip()]
    aggregated: list[dict[str, Any]] = []
    for seed in seeds[:5]:
        try:
            items = await crawl_seed(seed)
            aggregated.extend(items)
        except Exception:
            logger.exception("seed crawl failed: %s", seed)
    if query:
        qurl = query if query.startswith("http") else f"https://{query}"
        try:
            aggregated.extend(await crawl_seed(qurl, limit=20))
        except Exception:
            logger.exception("query crawl failed")

    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for item in sorted(aggregated, key=lambda x: x.get("score", 0.0), reverse=True):
        u = item["url"]
        if u in seen:
            continue
        seen.add(u)
        deduped.append(item)

    upsert_chroma(deduped[:200])

    await session.execute(
        text("INSERT INTO discovery_runs (query, results) VALUES (:q, CAST(:r AS jsonb))"),
        {"q": query or "", "r": json.dumps(deduped[:200])},
    )

    for item in deduped[:80]:
        score = float(item.get("score", 0.0))
        if score < 0.35:
            continue
        digest = hashlib.sha256(item["url"].encode("utf-8")).hexdigest()[:24]
        await session.execute(
            text(
                """
                INSERT INTO sources (name, source_type, endpoint, metadata, relevance_score)
                SELECT :name, 'discovered', :endpoint, CAST(:meta AS jsonb), :score
                WHERE NOT EXISTS (SELECT 1 FROM sources WHERE endpoint = :endpoint)
                """
            ),
            {
                "name": (item.get("title") or digest)[:500],
                "endpoint": item["url"][:2000],
                "meta": json.dumps({"digest": digest}),
                "score": score,
            },
        )
    await session.commit()
