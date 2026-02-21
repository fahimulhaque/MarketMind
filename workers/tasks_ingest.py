"""Ingestion pipeline — fetch, track, analyse, critique, and persist source content."""
from datetime import datetime, timezone

import httpx

from connectors.registry import get_connector
from core import db
from core.config import get_settings
from core.processing import chunk_text, normalize_content
from rules.tracker import evaluate_change
from rules.analyst import build_analysis
from rules.critic import review_analysis
from core.memory import upsert_document_memory, upsert_graph_relationship
from security.pii import redact_pii
from security.policy_engine import validate_source_policy
from workers.celery_app import celery_app


import asyncio

async def execute_ingest(source_id: int, force_refresh: bool = False) -> dict:
    db.init_db()
    settings = get_settings()
    
    # Run synchronous DB fetches in thread to not block async loop
    source = await asyncio.to_thread(db.get_source, source_id)
    if not source:
        await asyncio.to_thread(
            db.log_failed_ingestion, source_id, None, "SourceNotFound", f"Source {source_id} not found", False
        )
        raise ValueError(f"Source {source_id} not found")

    last_ingest = await asyncio.to_thread(db.get_last_ingest_time, source_id)
    if last_ingest is not None and not force_refresh:
        delta_seconds = (datetime.now(timezone.utc) - last_ingest).total_seconds()
        if delta_seconds < settings.ingest_min_interval_seconds:
            detail = f"throttled_min_interval={settings.ingest_min_interval_seconds}s"
            await asyncio.to_thread(db.log_ingest_run, source_id, "skipped", detail)
            return {
                "source_id": source_id,
                "skipped": True,
                "reason": "min_interval_not_elapsed",
            }

    async with httpx.AsyncClient() as client:
        policy = await validate_source_policy(source["url"], client)
    
    if not policy.allowed:
        await asyncio.to_thread(db.log_ingest_run, source_id, "blocked", policy.reason)
        return {
            "source_id": source_id,
            "skipped": True,
            "reason": policy.reason,
        }

    try:
        connector = get_connector(source.get("connector_type", "web"))
        # Execute fetch in thread pool to avoid blocking the event loop on sync HTTP calls
        content = await asyncio.to_thread(connector.fetch, source["url"])
    except Exception as error:
        retryable = isinstance(error, (httpx.HTTPError, httpx.ConnectError))
        await asyncio.to_thread(
            db.log_failed_ingestion, source_id, source["url"], type(error).__name__, str(error), retryable
        )
        raise

    # Sync processing tasks running in main worker thread (CPU bound)
    normalized_content = normalize_content(content, source["url"])
    chunks = chunk_text(normalized_content, chunk_size=500, overlap=100)
    previous_hash = await asyncio.to_thread(db.get_latest_snapshot_hash, source_id)
    evaluation = evaluate_change(normalized_content, previous_hash)
    redacted_excerpt = redact_pii(evaluation["excerpt"])
    redacted_chunks = [redact_pii(chunk) for chunk in chunks]

    analysis = build_analysis(
        source_name=source["name"],
        source_url=source["url"],
        has_changed=evaluation["has_changed"],
        excerpt=redacted_excerpt,
    )
    critique = review_analysis(analysis)

    await asyncio.to_thread(
        db.insert_snapshot,
        source_id,
        evaluation["content_hash"],
        redacted_excerpt,
    )

    if evaluation["has_changed"]:
        await asyncio.to_thread(
            db.insert_insight,
            source_id,
            source["name"],
            source["url"],
            analysis["insight"],
            analysis["threat_level"],
            analysis["recommendation"],
            analysis["evidence_ref"],
            evaluation["content_hash"],
            analysis["confidence"],
            critique["critic_status"],
        )
        memory_status = "ok"
        try:
            await asyncio.to_thread(
                upsert_document_memory,
                source_id,
                source["name"],
                source["url"],
                evaluation["content_hash"],
                redacted_chunks,
                analysis["evidence_ref"],
            )
            await asyncio.to_thread(
                upsert_graph_relationship,
                source_id,
                source["name"],
                source["url"],
                analysis["threat_level"],
                analysis["evidence_ref"],
            )
        except Exception as error:
            memory_status = "degraded"
            await asyncio.to_thread(
                db.log_failed_ingestion,
                source_id,
                source["url"],
                type(error).__name__,
                f"memory_write_failed:{error}",
                True,
            )

    await asyncio.to_thread(
        db.log_ingest_run,
        source_id,
        "succeeded",
        f"changed={evaluation['has_changed']};critic_status={critique['critic_status']};memory={locals().get('memory_status', 'n/a')}",
    )

    return {
        "source_id": source_id,
        "changed": evaluation["has_changed"],
        "content_hash": evaluation["content_hash"],
        "critic_status": critique["critic_status"],
        "confidence": analysis["confidence"],
        "chunks": len(redacted_chunks),
    }


@celery_app.task(name="workers.tasks_ingest.run_ingest", bind=True, rate_limit="60/m")
def run_ingest(self, source_id: int) -> dict:
    try:
        return asyncio.run(execute_ingest(source_id=source_id, force_refresh=False))
    except (httpx.HTTPError, httpx.ConnectError) as error:
        raise self.retry(exc=error, countdown=min(2 ** self.request.retries, 30), max_retries=3)

