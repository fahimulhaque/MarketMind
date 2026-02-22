"""Hybrid retrieval, enrichment, and fallback evidence gathering."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from core import db
from core.source_discovery import discover_query_sources, run_full_enrichment
from core.memory import semantic_search, graph_search_related_sources
from workers.tasks_ingest import execute_ingest

logger = logging.getLogger(__name__)


def _enrich_for_query(query_text: str, query_context: dict, max_sources: int = 5) -> dict:
    """Run full multi-provider enrichment + RSS ingest for the query."""
    ticker = query_context.get("ticker")

    # 1. Run structured provider enrichment (SEC, FMP, FRED, etc.)
    enrichment_summary = {}
    try:
        enrichment_summary = run_full_enrichment(query_text, pre_resolved_ticker=ticker)
    except Exception as exc:
        logger.warning("Full enrichment failed for %r: %s", query_text, exc)

    # 2. Also run RSS source discovery + sync ingest (web/news content)
    discovered = discover_query_sources(query_text, pre_resolved_ticker=ticker)
    refreshed: list[dict] = []
    source_ids: list[int] = []

    for candidate in discovered[:max_sources]:
        source = db.add_source(
            name=candidate["name"],
            url=candidate["url"],
            connector_type=candidate["connector_type"],
        )
        source_ids.append(source["id"])
        try:
            result = execute_ingest(source_id=source["id"], force_refresh=True)
            refreshed.append(
                {
                    "source_id": source["id"],
                    "source_name": source["name"],
                    "status": "ok",
                    "changed": result.get("changed", False),
                }
            )
        except Exception as error:
            refreshed.append(
                {
                    "source_id": source["id"],
                    "source_name": source["name"],
                    "status": "failed",
                    "error": str(error),
                }
            )

    return {
        "discovered_sources": len(discovered),
        "attempted_refresh": len(refreshed),
        "source_ids": source_ids,
        "results": refreshed,
        "provider_enrichment": enrichment_summary,
    }


def _fallback_evidence_from_sources(source_ids: list[int], limit: int) -> list[dict]:
    merged: list[dict] = []
    for source_id in source_ids:
        merged.extend(db.get_latest_insights(limit=4, source_id=source_id, offset=0))
    merged.sort(key=lambda item: item.get("created_at") or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    return merged[:limit]


def _hybrid_retrieve(query_text: str, query_context: dict, limit: int = 20) -> tuple[list[dict], list[dict], list[dict]]:
    """Run hybrid retrieval from Postgres, Qdrant, and Neo4j.

    Returns (merged_evidence, semantic_chunks, graph_related).
    """
    # 1. Postgres full-text search (existing path)
    pg_results = db.search_insights_by_query(query_text=query_text, limit=max(limit, 12))

    # 2. Qdrant semantic search
    semantic_chunks: list[dict] = []
    try:
        semantic_chunks = semantic_search(query_text, limit=limit)
    except Exception as exc:
        logger.warning("Semantic search failed: %s", exc)

    # 3. Neo4j graph search for related sources
    entity_name = query_context.get("entity", query_text)
    graph_related: list[dict] = []
    try:
        graph_related = graph_search_related_sources(entity_name, limit=10)
    except Exception as exc:
        logger.warning("Graph search failed: %s", exc)

    # 4. Merge semantic results into the evidence pool
    #    Create pseudo-insight items from semantic chunks that aren't already in pg_results
    pg_source_ids = {item.get("source_id") for item in pg_results}
    for chunk in semantic_chunks:
        # Inject semantic results as evidence entries if not already present
        source_id = chunk.get("source_id")
        if source_id and source_id not in pg_source_ids:
            pg_results.append({
                "source_id": source_id,
                "source_name": chunk.get("source_name", ""),
                "source_url": chunk.get("source_url", ""),
                "evidence_ref": chunk.get("evidence_ref", ""),
                "insight": chunk.get("chunk", ""),
                "recommendation": "",
                "threat_level": "low",
                "confidence": round(chunk.get("similarity_score", 0.5), 4),
                "similarity_score": chunk.get("similarity_score", 0.0),
                "text_rank": 0.0,
                "critic_status": "approved",
                "created_at": None,
            })
            pg_source_ids.add(source_id)
        elif source_id and source_id in pg_source_ids:
            # Attach semantic score to existing items from this source
            for item in pg_results:
                if item.get("source_id") == source_id and "similarity_score" not in item:
                    item["similarity_score"] = chunk.get("similarity_score", 0.0)
                    break

    return pg_results, semantic_chunks, graph_related
