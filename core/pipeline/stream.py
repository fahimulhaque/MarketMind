"""Streaming intelligence query — yields progressive SSE events."""

from __future__ import annotations

import json as _json
import logging

from core import db
from core import llm as llm_module
from core.source_discovery import (
    discover_query_sources,
    fetch_financial_snapshot,
    run_full_enrichment,
)
from core.memory import graph_find_connected_entities
from workers.tasks_ingest import execute_ingest

from core.pipeline.query import _parse_query
from core.pipeline.ranking import (
    _rank_items,
    _detect_contradictions,
    _needs_refresh,
    _build_signal_shifts,
)
from core.pipeline.retrieval import (
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


def run_market_intelligence_query_stream(query_text: str, limit: int = 20):
    """Generator that yields SSE-formatted stage events for progressive UI.

    Each yield is a string in the format:
        data: {"stage": "...", "progress": 0.0-1.0, "data": {...}}\\n\\n

    The dashboard consumes these via ReadableStream and progressively
    renders each section as it arrives.
    """

    def _event(stage: str, progress: float, data: dict | None = None, message: str = "") -> str:
        payload = {"stage": stage, "progress": round(progress, 2)}
        if data is not None:
            payload["data"] = data
        if message:
            payload["message"] = message
        return f"data: {_json.dumps(payload)}\n\n"

    # --- Stage 1: Parse query ---
    query_context = _parse_query(query_text)
    ticker = query_context.get("ticker")
    yield _event("query_parsed", 0.05, {
        "entity": query_context.get("entity"),
        "ticker": ticker,
        "timeframe": query_context.get("timeframe"),
        "intent": query_context.get("intent"),
    }, f"Identified entity: {query_context.get('entity')}")

    # --- Stage 2: Start enrichment ---
    yield _event("enrichment_started", 0.08, message="Fetching data from SEC, FRED, FMP, Reddit, and more...")

    enrichment = None
    try:
        existing = db.search_insights_by_query(query_text=query_text, limit=5)
        if _needs_refresh(existing):
            enrichment_summary = {}
            try:
                enrichment_summary = run_full_enrichment(query_text)
            except Exception as exc:
                logger.warning("Full enrichment failed: %s", exc)

            # Yield per-provider results from structured enrichment
            providers_run = []
            if enrichment_summary.get("providers_run"):
                providers_run = enrichment_summary["providers_run"]
            for pr in providers_run:
                yield _event("provider_complete", 0.12, {
                    "provider": pr.get("provider", ""),
                    "data_type": pr.get("data_type", ""),
                    "records": pr.get("records_stored", 0),
                    "success": pr.get("success", False),
                }, f"{pr.get('provider', 'Provider')}: {pr.get('records_stored', 0)} records")

            # Also run RSS source discovery
            discovered = discover_query_sources(query_text)
            rss_results: list[dict] = []
            rss_source_ids: list[int] = []
            for candidate in discovered[:5]:
                source = db.add_source(
                    name=candidate["name"],
                    url=candidate["url"],
                    connector_type=candidate["connector_type"],
                )
                rss_source_ids.append(source["id"])
                try:
                    result = execute_ingest(source_id=source["id"], force_refresh=True)
                    rss_results.append({"source_name": source["name"], "status": "ok", "changed": result.get("changed", False)})
                except Exception as error:
                    rss_results.append({"source_name": source["name"], "status": "failed", "error": str(error)})

            enrichment = {
                "discovered_sources": len(discovered),
                "attempted_refresh": len(rss_results),
                "source_ids": rss_source_ids,
                "results": rss_results,
                "provider_enrichment": enrichment_summary,
            }
    except Exception as exc:
        logger.warning("Enrichment stage failed: %s", exc)
        yield _event("warning", 0.18, {"type": "enrichment_degraded"}, f"Some data sources unavailable: {exc}")

    yield _event("enrichment_complete", 0.20, message="Data collection complete")

    # --- Stage 3: Hybrid retrieval ---
    yield _event("retrieval_started", 0.22, message="Searching Postgres, Qdrant vectors, and Neo4j graph...")

    candidates, semantic_chunks, graph_related = _hybrid_retrieve(
        query_text, query_context, limit=max(limit, 12),
    )
    if not candidates and enrichment and enrichment.get("source_ids"):
        candidates = _fallback_evidence_from_sources(enrichment["source_ids"], limit=max(limit, 12))

    yield _event("retrieval_complete", 0.30, {
        "postgres_hits": len(candidates),
        "semantic_hits": len(semantic_chunks),
        "graph_hits": len(graph_related),
    }, f"Found {len(candidates)} evidence items")

    # --- Stage 4: Rank ---
    ranked = _rank_items(candidates, query_context=query_context)
    top_evidence = ranked[:limit]
    yield _event("ranking_complete", 0.35, {
        "top_score": top_evidence[0].get("rank_score") if top_evidence else 0,
        "evidence_count": len(top_evidence),
    }, f"Ranked {len(top_evidence)} items by confidence + relevance")

    # --- Stage 5: Financial snapshot ---
    yield _event("financial_snapshot_started", 0.38, message="Fetching financial data...")
    financial_snapshot = fetch_financial_snapshot(query_text)
    financial_performance = _build_financial_performance(financial_snapshot)
    yield _event("financial_snapshot", 0.42, financial_performance,
                 f"Market cap: {financial_performance.get('summary', 'n/a')[:60]}")

    # --- Stage 5b: Analyst Consensus ---
    if ticker:
        yield _event("analyst_consensus_started", 0.43, message="Fetching analyst ratings and price targets...")
        try:
            from core.pipeline.yfinance_analyst import fetch_analyst_consensus
            analyst_consensus = fetch_analyst_consensus(ticker)
            yield _event("analyst_consensus", 0.435, analyst_consensus,
                         f"Analyst Consensus: {analyst_consensus.get('analyst_count', 0)} analysts")
        except Exception as exc:
            logger.warning("Analyst consensus fetch failed: %s", exc)

    # --- Stage 6: Historical trends ---
    yield _event("historical_started", 0.44, message="Loading historical financials...")
    historical_trends = _build_historical_trends(ticker)
    trend_analysis_text = None
    if historical_trends.get("available") and ticker:
        yield _event("trend_analysis_started", 0.46, message="AI analyzing financial trends...")
        trend_analysis_text = llm_module.generate_trend_analysis(
            ticker=ticker,
            quarterly_data=historical_trends.get("quarters", []),
            annual_data=historical_trends.get("annual", []),
        )
    yield _event("historical_trends", 0.50, {
        "trends": historical_trends,
        "trend_analysis": trend_analysis_text,
    }, f"Trend: {historical_trends.get('trend_direction', 'n/a')}")

    # --- Stage 7: Macro context ---
    yield _event("macro_started", 0.52, message="Loading macro indicators (GDP, CPI, VIX, Fed Rate)...")
    macro_context = _build_macro_context()
    yield _event("macro_context", 0.56, macro_context,
                 macro_context.get("summary", "No macro data")[:80])

    # --- Stage 8: Social sentiment ---
    yield _event("sentiment_started", 0.58, message="Analyzing social sentiment from Reddit...")
    social_sentiment = _build_social_sentiment(ticker)
    yield _event("social_sentiment", 0.62, social_sentiment,
                 social_sentiment.get("summary", "No social data")[:80])

    # --- Stage 8b: Market News ---
    if ticker:
        yield _event("market_news_started", 0.63, message="Fetching recent market news...")
        try:
            from core.pipeline.yfinance_analyst import fetch_market_news
            market_news = fetch_market_news(ticker)
            yield _event("market_news", 0.64, market_news,
                         f"Market news: {len(market_news.get('articles', []))} articles found")
        except Exception as exc:
            logger.warning("Market news fetch failed: %s", exc)

    # --- Stage 9: Coverage assessment (with real-time overlays) ---
    coverage = _build_coverage_assessment(
        ticker,
        financial_snapshot=financial_snapshot,
        social_sentiment=social_sentiment,
    )
    coverage_score = float(coverage.get("score", 0.0)) if coverage.get("available") else 0.0
    yield _event("coverage", 0.65, coverage,
                 f"Coverage score: {coverage.get('score', 0):.0%}")

    # --- Stage 10: SEC filings ---
    yield _event("filings_started", 0.67, message="Loading SEC filings...")
    filings = _build_filings_summary(ticker)
    yield _event("filings", 0.70, filings,
                 f"{filings.get('count', 0)} filings found")

    # --- Stage 10b: Insider Activity ---
    if ticker:
        yield _event("insider_activity_started", 0.71, message="Fetching recent insider trading activity...")
        try:
            from core.pipeline.yfinance_analyst import fetch_insider_activity
            insider_activity = fetch_insider_activity(ticker)
            yield _event("insider_activity", 0.715, insider_activity,
                         f"Insider trading: {insider_activity.get('net_direction', 'NEUTRAL')}")
        except Exception as exc:
            logger.warning("Insider activity fetch failed: %s", exc)

    # --- Stage 11: AI Decision Analysis (streaming) ---
    yield _event("analyzing", 0.72, message="AI generating executive decision...")

    # Stream executive summary token-by-token
    decision_tokens: list[str] = []
    for token in llm_module.generate_executive_summary_stream(
        query=query_text,
        top_evidence=[
            {
                "source_name": item.get("source_name", "unknown"),
                "insight": (item.get("insight") or "")[:200],
                "confidence": item.get("confidence", 0),
                "threat_level": item.get("threat_level", "low"),
            }
            for item in top_evidence[:5]
        ],
        financials=financial_snapshot,
        macro=macro_context,
        sentiment=social_sentiment,
        historical=historical_trends,
    ):
        decision_tokens.append(token)
        yield _event("decision_token", 0.74, {"token": token})

    llm_summary = "".join(decision_tokens).strip() if decision_tokens else None

    # Generate recommendation (batch — short output, streaming not worth it)
    avg_confidence = sum(float(item.get("confidence", 0.0) or 0.0) for item in top_evidence[:5]) / max(min(len(top_evidence), 5), 1)
    max_threat = max(
        ({"low": 1, "medium": 2, "high": 3}.get(item.get("threat_level", "low"), 1) for item in top_evidence[:5]),
        default=1,
    )
    risk_level = "high" if max_threat >= 3 else "medium" if max_threat == 2 else "low"

    if llm_summary:
        answer_summary = llm_summary
    else:
        # Data-rich fallback
        parts = [f"Analysis for '{query_text}': "]
        fin = financial_snapshot or {}
        if fin.get("price"):
            parts.append(f"Current price ${fin['price']}")
            if fin.get("market_cap"):
                mc = fin["market_cap"]
                mc_str = f"${mc/1e12:.1f}T" if mc >= 1e12 else f"${mc/1e9:.1f}B" if mc >= 1e9 else f"${mc/1e6:.0f}M"
                parts.append(f"(market cap {mc_str}).")
        if fin.get("trailing_pe") and fin["trailing_pe"] != "n/a":
            parts.append(f"P/E {fin['trailing_pe']}.")
        n_sources = len(top_evidence)
        parts.append(f"Based on {n_sources} evidence sources, overall risk is {risk_level}.")
        answer_summary = " ".join(parts)

    llm_rec = llm_module.generate_recommendation(
        query=query_text,
        decision_context={
            "risk_level": risk_level,
            "confidence": round(avg_confidence, 3),
            "answer_summary": answer_summary[:200],
            "current_price": financial_snapshot.get("price"),
        },
        contradictions=_detect_contradictions(top_evidence),
        coverage_score=coverage_score,
    )
    recommendation = llm_rec or (
        f"{'Exercise caution — ' if risk_level == 'high' else 'Monitor closely — ' if risk_level == 'medium' else ''}"
        f"Evidence confidence is {avg_confidence:.0%} across {min(len(top_evidence), 5)} sources. "
        f"{'Contradictory signals detected; verify before acting.' if _detect_contradictions(top_evidence) else 'Review supporting evidence before making decisions.'}"
    )

    decision = {
        "answer_summary": answer_summary,
        "confidence": round(avg_confidence, 3),
        "risk_level": risk_level,
        "recommendation": recommendation,
    }

    yield _event("decision_ready", 0.78, {
        "executive_summary": decision["answer_summary"],
        "recommendation": decision["recommendation"],
        "confidence": decision["confidence"],
        "risk_level": decision["risk_level"],
    }, "Executive decision generated")

    # --- Stage 12: AI Market Narrative (streaming) ---
    yield _event("narrative_started", 0.80, message="AI writing market narrative...")
    evidence_dicts = [
        {
            "source_name": item.get("source_name", "unknown"),
            "insight": (item.get("insight") or "")[:200],
            "confidence": item.get("confidence", 0),
            "threat_level": item.get("threat_level", "low"),
        }
        for item in top_evidence[:6]
    ]

    narrative_tokens: list[str] = []
    for token in llm_module.generate_market_narrative_stream(
        query=query_text,
        top_evidence=evidence_dicts,
        financials=financial_snapshot,
        historical=historical_trends,
        macro=macro_context,
        sentiment=social_sentiment,
        coverage_score=coverage_score,
        executive_verdict=decision["answer_summary"],
    ):
        narrative_tokens.append(token)
        yield _event("narrative_token", 0.82, {"token": token})

    narrative = "".join(narrative_tokens).strip() if narrative_tokens else ""
    if not narrative:
        price_str = f"${financial_snapshot.get('price', 'unknown')}" if financial_snapshot.get('price') else "unknown price"
        narrative = (
            f"Market narrative for {query_text} could not be generated via AI. "
            f"Financial data shows {price_str} with {coverage_score:.0%} data coverage. "
            f"See individual sections below for available intelligence."
        )
    yield _event("narrative_ready", 0.85, {"market_narrative": narrative}, "Market narrative complete")

    # --- Stage 13: AI Scenario Planning ---
    yield _event("scenarios_started", 0.87, message="AI building scenario projections...")
    scenarios = _build_scenarios(
        decision, top_evidence,
        financials=financial_snapshot,
        historical=historical_trends,
        macro=macro_context,
        query_text=query_text,
    )
    contradictions = _detect_contradictions(top_evidence)
    signal_shifts = _build_signal_shifts(top_evidence)
    yield _event("scenarios_ready", 0.90, {
        "scenarios": scenarios,
        "contradictions": contradictions,
        "signal_shifts": signal_shifts,
    }, "Scenario analysis complete")

    # --- Stage 14: Competitive landscape (streaming) ---
    yield _event("competitive_started", 0.91, message="AI analyzing competitive landscape...")

    competitive_tokens: list[str] = []
    for token in llm_module.generate_competitive_landscape_stream(
        query=query_text,
        ticker=ticker or "",
        top_evidence=evidence_dicts,
        financials=financial_snapshot,
        sector=financial_snapshot.get("sector", ""),
        industry=financial_snapshot.get("industry", ""),
    ):
        competitive_tokens.append(token)
        yield _event("competitive_token", 0.92, {"token": token})

    competitive_landscape = "".join(competitive_tokens).strip() if competitive_tokens else ""
    if not competitive_landscape:
        competitive_landscape = (
            f"Competitive landscape analysis for {query_text} requires more data coverage "
            f"(current: {coverage_score:.0%}). See financial metrics and evidence sections "
            f"for available intelligence."
        )
    yield _event("competitive_landscape", 0.93, {
        "competitive_landscape": competitive_landscape,
    }, "Competitive landscape ready")

    # --- Stage 15: Price history context (reuse snapshot data, only fetch history series) ---
    price_context: dict = {"available": False}
    if ticker:
        yield _event("price_history_started", 0.94, message="Loading price history...")
        try:
            import yfinance as yf
            t = yf.Ticker(ticker)
            hist = t.history(period="1y")
            if not hist.empty:
                close_series = hist["Close"]
                current = float(close_series.iloc[-1])
                yr_high = float(close_series.max())
                yr_low = float(close_series.min())
                ytd_start = float(close_series.iloc[0])
                one_m_ago = float(close_series.iloc[-22]) if len(close_series) >= 22 else ytd_start
                three_m_ago = float(close_series.iloc[-66]) if len(close_series) >= 66 else ytd_start

                price_context = {
                    "available": True,
                    "current": round(current, 2),
                    "fifty_two_week_high": round(yr_high, 2),
                    "fifty_two_week_low": round(yr_low, 2),
                    "ytd_return": round((current / ytd_start - 1) * 100, 1) if ytd_start else None,
                    "one_month_return": round((current / one_m_ago - 1) * 100, 1) if one_m_ago else None,
                    "three_month_return": round((current / three_m_ago - 1) * 100, 1) if three_m_ago else None,
                    "range_position": round((current - yr_low) / (yr_high - yr_low) * 100, 0) if yr_high != yr_low else 50,
                }
            elif financial_snapshot.get("fifty_two_week_low") is not None:
                # Fallback: use snapshot data for range if history series failed
                low_52 = financial_snapshot["fifty_two_week_low"]
                high_52 = financial_snapshot.get("fifty_two_week_high", low_52)
                cur = financial_snapshot.get("price", low_52)
                price_context = {
                    "available": True,
                    "current": round(float(cur), 2) if cur else None,
                    "fifty_two_week_high": round(float(high_52), 2) if high_52 else None,
                    "fifty_two_week_low": round(float(low_52), 2) if low_52 else None,
                    "range_position": round((float(cur) - float(low_52)) / (float(high_52) - float(low_52)) * 100, 0)
                    if cur and high_52 and low_52 and float(high_52) != float(low_52)
                    else 50,
                }
        except Exception as exc:
            logger.warning("Price history fetch failed for %s: %s", ticker, exc)

    yield _event("price_history", 0.95, price_context,
                 f"{'52W range position: ' + str(price_context.get('range_position', '?')) + '%' if price_context.get('available') else 'No price history'}")

    # --- Graph connections ---
    connected_entities: list[dict] = []
    try:
        entity_name = query_context.get("entity", query_text)
        connected_entities = graph_find_connected_entities(entity_name, limit=5)
    except Exception as exc:
        logger.warning("Graph entity lookup failed in stream: %s", exc)

    related_entities = []
    for g in graph_related[:5]:
        related_entities.append({
            "source_name": g.get("source_name", ""),
            "url": g.get("source_url", ""),
            "threat_level": g.get("threat_level", ""),
            "evidence_ref": g.get("evidence_ref", ""),
        })

    # --- Persist and complete ---
    search_id = db.save_search_result(
        query_text=query_text,
        answer_summary=decision["answer_summary"],
        confidence=decision["confidence"],
        risk_level=decision["risk_level"],
        recommendation=decision["recommendation"],
        evidence_items=top_evidence,
    )

    yield _event("complete", 1.0, {
        "search_id": search_id,
        "query_context": {
            "entity": query_context.get("entity"),
            "ticker": ticker,
            "timeframe": query_context.get("timeframe"),
            "intent": query_context.get("intent"),
        },
        "related_entities": related_entities,
        "connected_entities": connected_entities,
        "enrichment_triggered": enrichment is not None,
        "enrichment": enrichment,
        "evidence_count": len(top_evidence),
        "semantic_matches": len(semantic_chunks),
        "graph_related_sources": len(graph_related),
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
    }, "Intelligence report complete")
