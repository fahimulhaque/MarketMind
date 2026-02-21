from fastapi import APIRouter, Depends

from api.auth import require_write_access
from workers.tasks_ingest import run_ingest
from workers.tasks_agent import run_all_sources, run_priority_ingestion

router = APIRouter(prefix="/agents", tags=["agents"])


@router.post("/ingest/{source_id}", dependencies=[Depends(require_write_access)])
def trigger_ingest(source_id: int) -> dict:
    task = run_ingest.delay(source_id)
    return {"task_id": task.id, "source_id": source_id}


@router.post("/run-all", dependencies=[Depends(require_write_access)])
def trigger_run_all(limit: int = 100) -> dict:
    task = run_all_sources.delay(limit=limit)
    return {"task_id": task.id, "limit": limit}


@router.post("/run-priority", dependencies=[Depends(require_write_access)])
def trigger_run_priority(query_text: str, limit: int = 100) -> dict:
    task = run_priority_ingestion.delay(query_text=query_text, limit=limit)
    return {"task_id": task.id, "query_text": query_text, "limit": limit}
