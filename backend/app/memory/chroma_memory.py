from __future__ import annotations

import hashlib
import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

COLLECTION = "acap_episodes"

_CHROMA_RETRY_SEC = 3.0
_chroma_client: Any | None = None
_chroma_client_lock = threading.Lock()


def get_chroma_client() -> Any:
    """
    Return a shared Chroma HttpClient, blocking until the server accepts connections.
    Retries on refusal / transient errors (startup ordering with Docker Compose).
    """
    global _chroma_client
    if _chroma_client is not None:
        return _chroma_client
    with _chroma_client_lock:
        if _chroma_client is not None:
            return _chroma_client
        import chromadb

        host = os.getenv("CHROMA_HOST", "chromadb")
        port = int(os.getenv("CHROMA_PORT", "8000"))

        attempt = 0
        while True:
            attempt += 1
            try:
                client = chromadb.HttpClient(
                    host=host,
                    port=port,
                )
                # Force a round-trip so "connection refused" surfaces here, not on first upsert.
                client.list_collections()
            except Exception as exc:
                logger.warning(
                    "Chroma not ready at %s:%s (attempt %d); retrying in %.1fs: %s",
                    host,
                    port,
                    attempt,
                    _CHROMA_RETRY_SEC,
                    exc,
                )
                time.sleep(_CHROMA_RETRY_SEC)
                continue
            logger.info(
                "Chroma client connected at %s:%s after %d attempt(s)",
                host,
                port,
                attempt,
            )
            _chroma_client = client
            return client


def _episode_doc(snap: dict[str, Any]) -> tuple[str, str, dict[str, Any]]:
    ts = str((snap.get("meta") or {}).get("updated_at") or datetime.now(timezone.utc).isoformat())
    parts: list[str] = []
    for e in (snap.get("events") or [])[:12]:
        parts.append(str(e.get("summary", "")))
    for d in (snap.get("decisions") or [])[:8]:
        parts.append(f"{d.get('severity')}:{d.get('action')}:{d.get('rationale','')[:240]}")
    for ins in (snap.get("insights") or [])[:6]:
        parts.append(str(ins.get("text", "")))
    strat = (snap.get("meta") or {}).get("strategy") or {}
    ap = strat.get("active_plan") if isinstance(strat, dict) else None
    if isinstance(ap, dict):
        parts.append(f"plan:{ap.get('title')} step={ap.get('step_index')}")
    ex = strat.get("execution") if isinstance(strat, dict) else None
    if isinstance(ex, dict):
        parts.append(f"exec:{ex.get('status')}")
    text = "\n".join([p for p in parts if p]).strip() or "empty_cycle"
    eid = hashlib.sha256(f"{ts}|{text[:200]}".encode()).hexdigest()
    meta: dict[str, Any] = {
        "ts": ts,
        "episode_id": eid,
        "num_events": len(snap.get("events") or []),
        "num_decisions": len(snap.get("decisions") or []),
        "num_alerts": len(snap.get("alerts") or []),
    }
    pid = (ap or {}).get("plan_id")
    if pid is not None:
        meta["plan_id"] = int(pid)
    est = (ex or {}).get("status")
    if est is not None:
        meta["exec_status"] = str(est)
    return eid, text[:16000], meta


def _upsert_episode_sync(snap: dict[str, Any]) -> None:
    eid, doc, meta = _episode_doc(snap)
    client = get_chroma_client()
    coll = client.get_or_create_collection(name=COLLECTION, metadata={"hnsw:space": "cosine"})
    coll.upsert(ids=[eid], documents=[doc], metadatas=[meta])


async def upsert_episode(snap: dict[str, Any]) -> None:
    try:
        import asyncio

        await asyncio.to_thread(_upsert_episode_sync, snap)
    except Exception:
        logger.exception("chroma episodic upsert failed")


def query_similar(text: str, k: int = 8) -> list[dict[str, Any]]:
    try:
        client = get_chroma_client()
        coll = client.get_or_create_collection(name=COLLECTION, metadata={"hnsw:space": "cosine"})
        res = coll.query(query_texts=[text], n_results=k)
        out: list[dict[str, Any]] = []
        ids = (res.get("ids") or [[]])[0]
        docs = (res.get("documents") or [[]])[0]
        metas = (res.get("metadatas") or [[]])[0]
        dists = (res.get("distances") or [[]])[0] if res.get("distances") else [0.0] * len(ids)
        for i, eid in enumerate(ids):
            out.append(
                {
                    "id": eid,
                    "summary": docs[i] if i < len(docs) else "",
                    "metadata": metas[i] if i < len(metas) else {},
                    "distance": float(dists[i]) if i < len(dists) else 0.0,
                }
            )
        return out
    except Exception:
        logger.exception("chroma query failed")
        return []


def trend_from_performance(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Lightweight evolution summary for UI/API (pattern recall helper).
    """
    if not rows:
        return {"trend": "insufficient_data"}
    vals = [float(r.get("metric_value", 0.0)) for r in rows]
    return {
        "trend": "up" if vals[-1] > vals[0] else "down" if vals[-1] < vals[0] else "flat",
        "first": vals[0],
        "last": vals[-1],
        "n": len(vals),
    }
