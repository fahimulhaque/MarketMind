from fastapi import APIRouter, Query

from core import db

router = APIRouter(prefix="/insights", tags=["insights"])


@router.get("/latest")
def latest_insights(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    source_id: int | None = Query(default=None),
) -> dict:
    offset = (page - 1) * page_size
    items = db.get_latest_insights(limit=page_size, source_id=source_id, offset=offset)
    total = db.count_insights(source_id=source_id)
    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "insights": items,
    }
