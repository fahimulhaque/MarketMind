from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field

from api.auth import require_write_access
from core import db
from workers.tasks_compliance import execute_deletion_request, run_retention

router = APIRouter(prefix="/compliance", tags=["compliance"])


class DeletionRequestPayload(BaseModel):
    source_id: int = Field(ge=1)
    reason: str = Field(min_length=5, max_length=500)
    requested_by: str | None = Field(default=None, max_length=120)
    auto_execute: bool = Field(default=True)


@router.post("/deletion-requests", dependencies=[Depends(require_write_access)])
def create_deletion_request(payload: DeletionRequestPayload) -> dict:
    if not db.source_exists(payload.source_id):
        return {
            "request": None,
            "task_id": None,
            "detail": "source_not_found_or_already_deleted",
        }

    request = db.create_deletion_request(
        source_id=payload.source_id,
        reason=payload.reason,
        requested_by=payload.requested_by,
    )

    task_id = None
    if payload.auto_execute:
        task = execute_deletion_request.delay(request["id"])
        task_id = task.id

    return {
        "request": request,
        "task_id": task_id,
    }


@router.get("/deletion-requests")
def list_deletion_requests(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    status: str | None = Query(default=None),
) -> dict:
    offset = (page - 1) * page_size
    items = db.list_deletion_requests(limit=page_size, offset=offset, status=status)
    return {
        "page": page,
        "page_size": page_size,
        "items": items,
    }


@router.post("/deletion-requests/{request_id}/execute", dependencies=[Depends(require_write_access)])
def execute_request(request_id: int) -> dict:
    request = db.get_deletion_request(request_id)
    if not request:
        return {
            "request": None,
            "task_id": None,
        }

    task = execute_deletion_request.delay(request_id)
    return {
        "request": request,
        "task_id": task.id,
    }


@router.post("/retention/run", dependencies=[Depends(require_write_access)])
def trigger_retention_run() -> dict:
    task = run_retention.delay()
    return {
        "task_id": task.id,
    }


@router.get("/retention/runs")
def retention_runs(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
) -> dict:
    offset = (page - 1) * page_size
    items = db.list_retention_runs(limit=page_size, offset=offset)
    return {
        "page": page,
        "page_size": page_size,
        "items": items,
    }
