from __future__ import annotations

from typing import Any

from app.config import settings


async def run_narrator(ctx: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Human-readable synthesis. Uses OpenAI when configured; otherwise template narration.
    """
    bullets: list[str] = []
    layers = ctx.get("layers") or {}
    bullets.append(
        f"Air picture: {len(layers.get('aircraft') or [])} tracks; maritime demo: {len(layers.get('ships') or [])}."
    )
    events = ctx.get("events") or []
    if events:
        bullets.append(f"Fusion engine emitted {len(events)} discrete events this cycle.")
    inv = ctx.get("investigations") or []
    if inv:
        top = inv[0]
        bullets.append(f"Investigator focus: {top.get('title','')} — {top.get('narrative','')}")

    if settings.openai_api_key:
        try:
            from openai import AsyncOpenAI

            client = AsyncOpenAI(api_key=settings.openai_api_key)
            prompt = (
                "You are a senior intelligence analyst. Write 3 short, precise bullet insights "
                "for an operator console. No speculation beyond provided facts.\n"
                f"FACTS_JSON: {__import__('json').dumps({k: ctx.get(k) for k in ('meta','events')}, default=str)[:8000]}"
            )
            resp = await client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
            )
            txt = (resp.choices[0].message.content or "").strip()
            parts = [p.strip(" -*\t") for p in txt.splitlines() if p.strip()]
            bullets = parts[:6] or bullets
        except Exception:
            pass

    return [{"text": b, "ts": ctx.get("meta", {}).get("updated_at")} for b in bullets[:6]]
