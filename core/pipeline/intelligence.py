"""Main intelligence query pipeline — synchronous batch variant."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from core import db
from core import llm as llm_module
from core.source_discovery import fetch_financial_snapshot
from core.memory import graph_find_connected_entities

from core.pipeline.query import _parse_query
from core.pipeline.ranking import (
    _rank_items,
    _detect_contradictions,
    _needs_refresh,
    _build_signal_shifts,
)
from core.pipeline.retrieval import (
    _enrich_for_query,
    _fallback_evidence_from_sources,
    _hybrid_retrieve,
)
from core.pipeline.enrichment import (
    _build_financial_performance,
    _build_historical_trends,
    _build_macro_context,
    _build_social_sentiment,
    _build_coverage_assessment,
    _build_filings_summary,
    _build_scenarios,
)

logger = logging.getLogger(__name__)


def _summarize_decision(
    query_text: str,
    ranked_items: list[dict],
    financials: dict | None = None,
    macro: dict | None = None,
    sentiment: dict | None = None,
    historical: dict | None = None,
    coverage_score: float = 0.5,
) -> dict:
    if not ranked_items:
        return {
            "answer_summary": "No strong evidence found for this query in current ingested intelligence.",
            "confidence": 0.25,
            "risk_level": "low",
            "recommendation": "Ingest additional relevant sources or broaden query terms before making a decision.",
        }

    top = ranked_items[0]
    avg_confidence = sum(float(item.get("confidence", 0.0) or 0.0) for item in ranked_items[:5]) / min(len(ranked_items), 5)
    max_threat = max(
        ({"low": 1, "medium": 2, "high": 3}.get(item.get("threat_level", "low"), 1) for item in ranked_items[:5]),
        default=1,
    )
    risk_level = "high" if max_threat >= 3 else "medium" if max_threat == 2 else "low"

    # --- LLM-powered executive summary ---
    evidence_dicts = [
        {
            "source_name": item.get("source_name", "unknown"),
            "insight": (item.get("insight") or "")[:200],
            "confidence": item.get("confidence", 0),
            "threat_level": item.get("threat_level", "low"),
        }
        for item in ranked_items[:5]
    ]
    llm_summary = llm_module.generate_executive_summary(
        query=query_text,
        top_evidence=evidence_dicts,
        financials=financials or {},
        macro=macro or {},
        sentiment=sentiment or {},
        historical=historical or {},
    )
    if llm_summary:
        answer_summary = llm_summary
    else:
        # --- Data-rich fallback when LLM is unavailable ---
        parts = [f"Analysis for '{query_text}': "]
        fin = financials or {}
        if fin.get("price"):
            parts.append(f"Current price ${fin['price']}")
            if fin.get("market_cap"):
                mc = fin["market_cap"]
                mc_str = f"${mc/1e12:.1f}T" if mc >= 1e12 else f"${mc/1e9:.1f}B" if mc >= 1e9 else f"${mc/1e6:.0f}M"
                parts.append(f"(market cap {mc_str}).")
        if fin.get("trailing_pe") and fin["trailing_pe"] != "n/a":
            parts.append(f"P/E {fin['trailing_pe']}.")
        if fin.get("revenue_growth") and fin["revenue_growth"] != "n/a":
            parts.append(f"Revenue growth {fin['revenue_growth']}.")
        n_sources = len(ranked_items)
        parts.append(f"Based on {n_sources} evidence sources, overall risk is {top.get('threat_level', 'low')}.")
        if top.get("insight"):
            parts.append(f"Top signal: {top['insight'][:150]}")
        answer_summary = " ".join(parts)

    # --- LLM-powered recommendation ---
    llm_rec = llm_module.generate_recommendation(
        query=query_text,
        decision_context={
            "risk_level": risk_level,
            "confidence": round(avg_confidence, 3),
            "answer_summary": answer_summary[:200],
            "current_price": financials.get("price") if financials else None,
        },
        contradictions=_detect_contradictions(ranked_items),
        coverage_score=coverage_score,
    )
    recommendation = llm_rec or (
        f"{'Exercise caution — ' if risk_level == 'high' else 'Monitor closely — ' if risk_level == 'medium' else ''}"
        f"Evidence confidence is {avg_confidence:.0%} across {min(len(ranked_items), 5)} sources. "
        f"{'Contradictory signals detected; verify before acting.' if _detect_contradictions(ranked_items) else 'Review supporting evidence before making decisions.'}"
    )

    return {
        "answer_summary": answer_summary,
        "confidence": round(avg_confidence, 3),
        "risk_level": risk_level,
        "recommendation": recommendation,
    }


def _synthesize_report(
    query_text: str,
    query_context: dict,
    decision: dict,
    evidence: list[dict],
    financial_snapshot: dict,
    historical_trends: dict,
    macro_context: dict,
    social_sentiment: dict,
    coverage: dict,
    filings: dict,
    graph_related: list[dict],
    semantic_chunks: list[dict],
) -> dict:
    top_sources = [item.get("source_name", "unknown") for item in evidence[:3]]
    coverage_score = float(coverage.get("score", 0.0)) if coverage.get("available") else 0.0

    # --- LLM-powered market narrative ---
    evidence_dicts = [
        {
            "source_name": item.get("source_name", "unknown"),
            "insight": (item.get("insight") or "")[:200],
            "confidence": item.get("confidence", 0),
            "threat_level": item.get("threat_level", "low"),
        }
        for item in evidence[:6]
    ]
    llm_narrative = llm_module.generate_market_narrative(
        query=query_text,
        top_evidence=evidence_dicts,
        financials=financial_snapshot,
        historical=historical_trends,
        macro=macro_context,
        sentiment=social_sentiment,
        coverage_score=coverage_score,
        executive_verdict=decision["answer_summary"],
    )

    if llm_narrative:
        narrative = llm_narrative
    else:
        # Fallback to template-based narrative
        narrative_parts = []
        if top_sources:
            narrative_parts.append(f"Signals cluster around {', '.join(top_sources)}.")
        else:
            narrative_parts.append("Limited source diversity in current evidence.")
        narrative_parts.append(
            f"Query intent is interpreted as {query_context.get('intent', 'general')} "
            f"within {query_context.get('timeframe', 'current')} horizon."
        )
        if historical_trends.get("available"):
            trend = historical_trends.get("trend_direction", "stable")
            quarters = historical_trends.get("quarters_available", 0)
            narrative_parts.append(f"Historical data shows {trend} trend over {quarters} quarters.")
        if social_sentiment.get("available"):
            narrative_parts.append(f"Social sentiment: {social_sentiment.get('sentiment_label', 'neutral')}.")
        if coverage.get("available"):
            score = coverage.get("score", 0)
            if score < 0.3:
                narrative_parts.append("Coverage is thin — consider adding more data sources for robust analysis.")
            elif score >= 0.7:
                narrative_parts.append("Good data coverage across multiple sources.")
        if evidence:
            narrative_parts.append("Current intelligence indicates active movement that warrants monitored execution.")
        else:
            narrative_parts.append("Evidence is thin; run broader coverage and revisit before material decisions.")
        narrative = " ".join(narrative_parts)

    # --- LLM-powered trend analysis ---
    ticker = query_context.get("ticker")
    currency = financial_snapshot.get("currency")
    trend_analysis_text = None
    if historical_trends.get("available") and ticker:
        trend_analysis_text = llm_module.generate_trend_analysis(
            ticker=ticker,
            quarterly_data=historical_trends.get("quarters", []),
            annual_data=historical_trends.get("annual", []),
            currency_code=currency,
        )

    contradictions = _detect_contradictions(evidence)
    financial_performance = _build_financial_performance(financial_snapshot)
    scenarios = _build_scenarios(
        decision, evidence,
        financials=financial_snapshot,
        historical=historical_trends,
        macro=macro_context,
        query_text=query_text,
    )
    signal_shifts = _build_signal_shifts(evidence)

    # Build related entities from graph
    related_entities = []
    for g in graph_related[:5]:
        related_entities.append({
            "source_name": g.get("source_name", ""),
            "url": g.get("source_url", ""),
            "threat_level": g.get("threat_level", ""),
            "evidence_ref": g.get("evidence_ref", ""),
        })

    return {
        "query": query_text,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "executive_summary": decision["answer_summary"],
        "decision_card": {
            "recommendation": decision["recommendation"],
            "confidence": decision["confidence"],
            "risk_level": decision["risk_level"],
        },
        "financial_performance": financial_performance,
        "historical_trends": historical_trends,
        "trend_analysis": trend_analysis_text,
        "macro_context": macro_context,
        "social_sentiment": social_sentiment,
        "filings": filings,
        "coverage": coverage,
        "related_entities": related_entities,
        "market_narrative": narrative,
        "why_it_matters": decision["recommendation"],
        "key_signal_shifts": signal_shifts,
        "scenarios": scenarios,
        "contradictions": contradictions,
        "semantic_search_results": len(semantic_chunks),
        "citations": [
            {
                "source": item.get("source_name"),
                "evidence_ref": item.get("evidence_ref"),
                "confidence": item.get("confidence"),
                "rank_score": item.get("rank_score"),
                "semantic_score": item.get("semantic_score"),
            }
            for item in evidence[:8]
        ],
    }


def run_market_intelligence_query(query_text: str, limit: int = 20) -> dict:
    """Main intelligence query pipeline with hybrid retrieval and multi-provider data.

    Uses parallel LLM dispatch (asyncio) to run all generation calls
    concurrently instead of serially (~60% faster).
    """
    query_context = _parse_query(query_text)
    ticker = query_context.get("ticker")
    background_task_id = None

    # 1. Background async priority ingestion (best-effort)
    try:
        from workers.tasks_agent import run_priority_ingestion

        background_task = run_priority_ingestion.delay(query_text=query_text, limit=100)
        background_task_id = background_task.id
    except Exception:
        logger.warning("run_priority_ingestion unavailable")
        try:
            from workers.tasks_agent import run_all_sources

            background_task = run_all_sources.delay(limit=100)
            background_task_id = background_task.id
        except Exception:
            logger.warning("run_all_sources also unavailable — skipping background ingestion")
            background_task_id = None

    # 2. Multi-provider enrichment (SEC, FMP, Alpha Vantage, FRED, Reddit, DDG + RSS)
    enrichment = None
    if _needs_refresh(db.search_insights_by_query(query_text=query_text, limit=5)):
        enrichment = _enrich_for_query(query_text=query_text, query_context=query_context)

    # 3. Hybrid retrieval: Postgres FTS + Qdrant semantic + Neo4j graph
    candidates, semantic_chunks, graph_related = _hybrid_retrieve(
        query_text, query_context, limit=max(limit, 12),
    )

    # Fallback if still thin after enrichment
    if not candidates and enrichment and enrichment.get("source_ids"):
        candidates = _fallback_evidence_from_sources(enrichment["source_ids"], limit=max(limit, 12))

    # 4. Rank with hybrid scoring (confidence + text_rank + recency + token + semantic + source)
    ranked = _rank_items(candidates, query_context=query_context)
    top_evidence = ranked[:limit]

    # 5. Build all intelligence sections (data gathering — no LLM calls)
    financial_snapshot = fetch_financial_snapshot(query_text, pre_resolved_ticker=ticker)
    historical_trends = _build_historical_trends(ticker)
    macro_context = _build_macro_context()
    social_sentiment = _build_social_sentiment(ticker)
    coverage = _build_coverage_assessment(
        ticker,
        financial_snapshot=financial_snapshot,
        social_sentiment=social_sentiment,
    )
    coverage_score = float(coverage.get("score", 0.0)) if coverage.get("available") else 0.0
    filings = _build_filings_summary(ticker)

    # Prepare evidence dicts for LLM (shared by multiple prompts)
    evidence_dicts = [
        {
            "source_name": item.get("source_name", "unknown"),
            "insight": (item.get("insight") or "")[:200],
            "confidence": item.get("confidence", 0),
            "threat_level": item.get("threat_level", "low"),
        }
        for item in top_evidence[:6]
    ]
    contradictions = _detect_contradictions(top_evidence)

    # 6. Parallel LLM generation (all 5 calls run concurrently with semaphore)
    llm_results = None
    try:
        llm_results = asyncio.run(
            llm_module.generate_parallel_intelligence(
                query=query_text,
                top_evidence=evidence_dicts,
                financials=financial_snapshot,
                macro=macro_context,
                sentiment=social_sentiment,
                historical=historical_trends,
                coverage_score=coverage_score,
                ticker=ticker,
                contradictions=contradictions,
            )
        )
    except Exception as exc:
        logger.warning("Parallel LLM dispatch failed, falling back to serial: %s", exc)

    # 7. Build decision from LLM results (or fallback to serial _summarize_decision)
    if llm_results and llm_results.get("executive_summary"):
        avg_confidence = sum(float(item.get("confidence", 0.0) or 0.0) for item in top_evidence[:5]) / max(min(len(top_evidence), 5), 1)
        max_threat = max(
            ({"low": 1, "medium": 2, "high": 3}.get(item.get("threat_level", "low"), 1) for item in top_evidence[:5]),
            default=1,
        )
        risk_level = "high" if max_threat >= 3 else "medium" if max_threat == 2 else "low"
        decision = {
            "answer_summary": llm_results["executive_summary"],
            "confidence": round(avg_confidence, 3),
            "risk_level": risk_level,
            "recommendation": llm_results.get("recommendation") or (
                f"{'Exercise caution — ' if risk_level == 'high' else 'Monitor closely — ' if risk_level == 'medium' else ''}"
                f"Evidence confidence is {avg_confidence:.0%} across {min(len(top_evidence), 5)} sources."
            ),
        }
    else:
        # Fallback: serial path
        decision = _summarize_decision(
            query_text=query_text,
            ranked_items=top_evidence,
            financials=financial_snapshot,
            macro=macro_context,
            sentiment=social_sentiment,
            historical=historical_trends,
            coverage_score=coverage_score,
        )
        llm_results = llm_results or {}

    # 8. Connected entity discovery via graph
    connected_entities: list[dict] = []
    try:
        entity_name = query_context.get("entity", query_text)
        connected_entities = graph_find_connected_entities(entity_name, limit=5)
    except Exception as exc:
        logger.warning("Graph entity lookup failed: %s", exc)

    # 9. Synthesize report (uses pre-computed LLM results instead of serial calls)
    narrative = llm_results.get("market_narrative") or ""
    trend_analysis_text = llm_results.get("trend_analysis")
    scenarios_from_llm = llm_results.get("scenarios")

    # Build scenarios (use LLM results or fallback to arithmetic)
    if scenarios_from_llm and len(scenarios_from_llm) == 3:
        total_prob = sum(float(s.get("probability", 0.33)) for s in scenarios_from_llm)
        if total_prob > 0:
            for s in scenarios_from_llm:
                s["probability"] = round(float(s.get("probability", 0.33)) / total_prob, 3)
                s.setdefault("trigger_signals", [])
                s.setdefault("assumption", "")
                s.setdefault("impact", "")
        scenarios = scenarios_from_llm
    else:
        scenarios = _build_scenarios(
            decision, top_evidence,
            financials=financial_snapshot,
            historical=historical_trends,
            macro=macro_context,
            query_text=query_text,
        )

    if not narrative:
        # Template fallback for narrative
        top_sources = [item.get("source_name", "unknown") for item in top_evidence[:3]]
        narrative_parts = []
        if top_sources:
            narrative_parts.append(f"Signals cluster around {', '.join(top_sources)}.")
        narrative_parts.append(
            f"Query intent is interpreted as {query_context.get('intent', 'general')} "
            f"within {query_context.get('timeframe', 'current')} horizon."
        )
        if top_evidence:
            narrative_parts.append("Current intelligence indicates active movement that warrants monitored execution.")
        narrative = " ".join(narrative_parts)

    financial_performance = _build_financial_performance(financial_snapshot)
    signal_shifts = _build_signal_shifts(top_evidence)

    # Build related entities from graph
    related_entities = []
    for g in graph_related[:5]:
        related_entities.append({
            "source_name": g.get("source_name", ""),
            "url": g.get("source_url", ""),
            "threat_level": g.get("threat_level", ""),
            "evidence_ref": g.get("evidence_ref", ""),
        })

    report = {
        "query": query_text,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "executive_summary": decision["answer_summary"],
        "decision_card": {
            "recommendation": decision["recommendation"],
            "confidence": decision["confidence"],
            "risk_level": decision["risk_level"],
        },
        "financial_performance": financial_performance,
        "historical_trends": historical_trends,
        "trend_analysis": trend_analysis_text,
        "macro_context": macro_context,
        "social_sentiment": social_sentiment,
        "filings": filings,
        "coverage": coverage,
        "related_entities": related_entities,
        "market_narrative": narrative,
        "why_it_matters": decision["recommendation"],
        "key_signal_shifts": signal_shifts,
        "scenarios": scenarios,
        "contradictions": contradictions,
        "semantic_search_results": len(semantic_chunks),
        "citations": [
            {
                "source": item.get("source_name"),
                "evidence_ref": item.get("evidence_ref"),
                "confidence": item.get("confidence"),
                "rank_score": item.get("rank_score"),
                "semantic_score": item.get("semantic_score"),
            }
            for item in top_evidence[:8]
        ],
    }

    # 10. Persist the search result
    search_id = db.save_search_result(
        query_text=query_text,
        answer_summary=decision["answer_summary"],
        confidence=decision["confidence"],
        risk_level=decision["risk_level"],
        recommendation=decision["recommendation"],
        evidence_items=top_evidence,
    )

    return {
        "search_id": search_id,
        "query_context": {
            "entity": query_context.get("entity"),
            "ticker": ticker,
            "timeframe": query_context.get("timeframe"),
            "intent": query_context.get("intent"),
        },
        "report": report,
        "knowledge_status": {
            "evidence_count": len(top_evidence),
            "semantic_matches": len(semantic_chunks),
            "graph_related_sources": len(graph_related),
            "connected_entities": connected_entities,
            "enrichment_triggered": enrichment is not None,
            "background_priority_task_id": background_task_id,
            "enrichment": enrichment,
        },
        "evidence": [
            {
                "source_id": item.get("source_id"),
                "source_name": item.get("source_name"),
                "evidence_ref": item.get("evidence_ref"),
                "insight": item.get("insight"),
                "confidence": item.get("confidence"),
                "recency_score": item.get("recency_score"),
                "source_quality": item.get("source_quality"),
                "token_relevance": item.get("token_relevance"),
                "semantic_score": item.get("semantic_score"),
                "rank_score": item.get("rank_score"),
            }
            for item in top_evidence
        ],
    }
