from fastapi import APIRouter, Depends, Query
from fastapi.responses import PlainTextResponse

from api.auth import require_write_access
from core import db
from workers.tasks_report import generate_report

router = APIRouter(prefix="/reports", tags=["reports"])


@router.post("/generate/{source_id}", dependencies=[Depends(require_write_access)])
def generate(source_id: int) -> dict:
    task = generate_report.delay(source_id)
    return {"task_id": task.id, "source_id": source_id}


@router.get("")
def list_latest_reports(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    source_id: int | None = Query(default=None),
) -> dict:
    offset = (page - 1) * page_size
    reports = db.list_reports(limit=page_size, offset=offset, source_id=source_id)
    return {
        "page": page,
        "page_size": page_size,
        "reports": reports,
    }


@router.get("/{report_id}")
def get_report(report_id: int) -> dict:
    report = db.get_report(report_id)
    if not report:
        return {"report": None}
    return {"report": report}


@router.get("/{report_id}/export.md", response_class=PlainTextResponse)
def export_report_markdown(report_id: int) -> str:
    report = db.get_report(report_id)
    if not report:
        return "Report not found"
    return report["content_markdown"]
