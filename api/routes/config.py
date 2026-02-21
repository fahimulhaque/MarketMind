from fastapi import APIRouter, Depends

from api.auth import require_write_access
from core.contracts import SourceCreate
from core import db

router = APIRouter(prefix="/sources", tags=["config"])


@router.post("", dependencies=[Depends(require_write_access)])
def upsert_source(payload: SourceCreate) -> dict:
    source = db.add_source(
        name=payload.name,
        url=str(payload.url),
        connector_type=payload.connector_type,
    )
    return {"source": source}
