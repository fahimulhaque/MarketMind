import json
import logging
import time

from fastapi import APIRouter, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from core import db
from core.config import get_settings
from core.pipeline import run_market_intelligence_query, run_market_intelligence_query_stream
from core.entities import autocomplete_tickers

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/search", tags=["search"])


class SearchQueryPayload(BaseModel):
    query: str = Field(min_length=3)
    limit: int = Field(default=10, ge=1, le=50)


@router.get("/autocomplete")
def search_autocomplete(q: str = Query(min_length=1, max_length=100)):
    """Ticker/company autocomplete — fast lookup for the search input."""
    return autocomplete_tickers(q)


@router.post("/query")
def search_query(payload: SearchQueryPayload) -> dict:
    return run_market_intelligence_query(query_text=payload.query, limit=payload.limit)


@router.post("/stream")
async def search_stream(payload: SearchQueryPayload, request: Request):
    """SSE endpoint that streams progressive intelligence events.

    Returns text/event-stream with events in the format:
        data: {"stage": "...", "progress": 0.0-1.0, "data": {...}}\n\n

    Features:
    - Pipeline timeout guard (default 300s, configurable via INTELLIGENCE_PIPELINE_TIMEOUT)
    - Client disconnect detection between SSE events
    """
    settings = get_settings()
    pipeline_timeout = settings.intelligence_pipeline_timeout

    async def generate():
        start_time = time.monotonic()
        try:
            for event in run_market_intelligence_query_stream(
                query_text=payload.query, limit=payload.limit,
            ):
                # Check client disconnect between events
                if await request.is_disconnected():
                    logger.info("Client disconnected mid-pipeline for query %r", payload.query)
                    return

                # Check pipeline timeout
                elapsed = time.monotonic() - start_time
                if elapsed > pipeline_timeout:
                    logger.warning(
                        "Pipeline timed out after %.0fs for query %r",
                        elapsed, payload.query,
                    )
                    yield f"data: {json.dumps({'stage': 'error', 'progress': 1.0, 'message': f'Pipeline timed out after {int(elapsed)}s — partial results shown above.'})}\n\n"
                    return

                yield event
        except Exception as exc:
            logger.error("Pipeline error for query %r: %s", payload.query, exc)
            yield f"data: {json.dumps({'stage': 'error', 'progress': 1.0, 'message': str(exc)})}\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/history")
def search_history(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
) -> dict:
    offset = (page - 1) * page_size
    items = db.get_search_history(limit=page_size, offset=offset)
    return {
        "page": page,
        "page_size": page_size,
        "items": items,
    }
