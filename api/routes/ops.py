from fastapi import APIRouter

from core import db

router = APIRouter(prefix="/ops", tags=["ops"])


@router.get("/metrics")
def metrics() -> dict:
    return {"metrics": db.get_observability_metrics()}
