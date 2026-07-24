from fastapi import APIRouter, BackgroundTasks
from pydantic import BaseModel

from app.agents.discovery_v2 import run_discovery_v2
from app.db.session import SessionLocal, ensure_database

router = APIRouter()


class DiscoveryRequest(BaseModel):
    query: str | None = None


@router.post("/run")
async def run_discovery(req: DiscoveryRequest, background: BackgroundTasks):
    async def job() -> None:
        await ensure_database()
        async with SessionLocal() as session:
            await run_discovery_v2(session, req.query)

    background.add_task(job)
    return {"status": "scheduled", "query": req.query}
