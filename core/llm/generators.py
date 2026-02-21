"""Synchronous and async generation functions for market intelligence."""

from __future__ import annotations

import asyncio

from core.llm.providers import (
    _is_cloud_provider,
    ollama_generate,
    ollama_generate_async,
)
from core.llm.prompts import (
    _SYSTEM_ANALYST,
    _SYSTEM_COMPETITIVE,
    _SYSTEM_SCENARIO,
    _build_competitive_landscape_prompt,
    _build_executive_summary_prompt,
    _build_market_narrative_prompt,
    _build_recommendation_prompt,
    _build_scenarios_prompt,
    _build_trend_analysis_prompt,
)
from core.llm.formatters import _parse_json_array


# ---------------------------------------------------------------------------
# Structured generation functions (synchronous — used by streaming pipeline)
# ---------------------------------------------------------------------------


def generate_executive_summary(
    query: str,
    top_evidence: list[dict],
    financials: dict,
    macro: dict,
    sentiment: dict,
    historical: dict,
) -> str | None:
    """Generate an LLM-powered executive summary grounded in evidence."""
    prompt = _build_executive_summary_prompt(query, top_evidence, financials, macro, sentiment, historical)
    return ollama_generate(prompt, system=_SYSTEM_ANALYST, temperature=0.25, max_tokens=384)


def generate_narrative(
    query: str,
    top_evidence: list[dict],
    financials: dict,
    historical: dict,
    macro: dict,
    sentiment: dict,
    coverage_score: float,
) -> str | None:
    """Generate a cohesive multi-paragraph market narrative."""
    prompt = _build_market_narrative_prompt(query, top_evidence, financials, historical, macro, sentiment, coverage_score)
    return ollama_generate(prompt, system=_SYSTEM_ANALYST, temperature=0.3, max_tokens=512)


# Alias used by search.py
generate_market_narrative = generate_narrative


def generate_scenarios(
    query: str,
    top_evidence: list[dict],
    financials: dict,
    historical: dict,
    macro: dict,
) -> list[dict] | None:
    """Generate LLM-reasoned bull/base/bear scenarios as structured JSON."""
    prompt = _build_scenarios_prompt(query, top_evidence, financials, historical, macro)
    result = ollama_generate(prompt, system=_SYSTEM_SCENARIO, temperature=0.3, max_tokens=512)
    if not result:
        return None
    return _parse_json_array(result)


def generate_recommendation(
    query: str,
    decision_context: dict,
    contradictions: list[dict],
    coverage_score: float,
) -> str | None:
    """Generate a specific, actionable recommendation."""
    prompt = _build_recommendation_prompt(query, decision_context, contradictions, coverage_score)
    return ollama_generate(prompt, system=_SYSTEM_ANALYST, temperature=0.25, max_tokens=192)


def generate_trend_analysis(
    ticker: str,
    quarterly_data: list[dict],
    annual_data: list[dict],
) -> str | None:
    """Generate an LLM analysis of financial trends."""
    if not quarterly_data and not annual_data:
        return None
    prompt = _build_trend_analysis_prompt(ticker, quarterly_data, annual_data)
    return ollama_generate(prompt, system=_SYSTEM_ANALYST, temperature=0.25, max_tokens=256)


def generate_competitive_landscape(
    query: str,
    ticker: str,
    top_evidence: list[dict],
    financials: dict,
    sector: str = "",
    industry: str = "",
) -> str | None:
    """Generate a competitive landscape analysis."""
    prompt = _build_competitive_landscape_prompt(query, ticker, top_evidence, financials, sector, industry)
    return ollama_generate(prompt, system=_SYSTEM_COMPETITIVE, temperature=0.3, max_tokens=384)


# ---------------------------------------------------------------------------
# Parallel intelligence generation (async — used by batch pipeline)
# ---------------------------------------------------------------------------


async def generate_parallel_intelligence(
    query: str,
    top_evidence: list[dict],
    financials: dict,
    macro: dict,
    sentiment: dict,
    historical: dict,
    coverage_score: float,
    ticker: str | None = None,
    contradictions: list[dict] | None = None,
) -> dict:
    """Run all independent LLM generation calls in parallel.

    Uses asyncio.gather with a semaphore to limit concurrency.
    Returns a dict with keys: executive_summary, recommendation,
    market_narrative, trend_analysis, scenarios.
    """
    # Build all prompts upfront (CPU-only, fast)
    summary_prompt = _build_executive_summary_prompt(
        query, top_evidence, financials, macro, sentiment, historical,
    )
    narrative_prompt = _build_market_narrative_prompt(
        query, top_evidence, financials, historical, macro, sentiment, coverage_score,
    )

    # Scenarios prompt
    scenarios_prompt = _build_scenarios_prompt(
        query, top_evidence, financials, historical, macro,
    )

    # Trend analysis prompt (may be None)
    trend_prompt = None
    quarterly = historical.get("quarters", []) if historical else []
    annual = historical.get("annual", []) if historical else []
    if (quarterly or annual) and ticker:
        trend_prompt = _build_trend_analysis_prompt(ticker, quarterly, annual)

    # Build task definitions: (prompt, system, temperature, max_tokens)
    task_defs = [
        (summary_prompt, _SYSTEM_ANALYST, 0.25, 384),
        (narrative_prompt, _SYSTEM_ANALYST, 0.3, 512),
        (scenarios_prompt, _SYSTEM_SCENARIO, 0.3, 512),
    ]

    if trend_prompt:
        task_defs.append((trend_prompt, _SYSTEM_ANALYST, 0.25, 256))
    else:
        task_defs.append(None)  # placeholder

    if _is_cloud_provider():
        # Sequential with 1s gaps to avoid rate limiting on free-tier APIs
        results: list = []
        for defn in task_defs:
            if defn is None:
                results.append(None)
            else:
                result = await ollama_generate_async(
                    defn[0], system=defn[1], temperature=defn[2], max_tokens=defn[3],
                )
                results.append(result)
                await asyncio.sleep(1.0)  # rate-limit gap
    else:
        # Ollama: fire all in parallel (semaphore limits concurrency)
        tasks = []
        for defn in task_defs:
            if defn is None:
                tasks.append(_noop_coro())
            else:
                tasks.append(
                    ollama_generate_async(defn[0], system=defn[1], temperature=defn[2], max_tokens=defn[3])
                )
        raw_results = await asyncio.gather(*tasks, return_exceptions=True)
        results = [r if not isinstance(r, Exception) else None for r in raw_results]

    executive_summary = results[0] if len(results) > 0 else None
    market_narrative = results[1] if len(results) > 1 else None
    scenarios_raw = results[2] if len(results) > 2 else None
    trend_analysis = results[3] if len(results) > 3 else None

    # Parse scenarios JSON
    scenarios = None
    if scenarios_raw:
        scenarios = _parse_json_array(scenarios_raw)

    # Generate recommendation (depends on executive_summary, so do it after)
    recommendation = None
    if executive_summary:
        rec_prompt = _build_recommendation_prompt(
            query,
            {
                "risk_level": "medium",
                "confidence": 0.5,
                "answer_summary": executive_summary[:200],
                "current_price": financials.get("price") if financials else None,
            },
            contradictions or [],
            coverage_score,
        )
        recommendation = await ollama_generate_async(
            rec_prompt, system=_SYSTEM_ANALYST, temperature=0.25, max_tokens=192,
        )

    return {
        "executive_summary": executive_summary,
        "recommendation": recommendation,
        "market_narrative": market_narrative,
        "trend_analysis": trend_analysis,
        "scenarios": scenarios,
    }


async def _noop_coro():
    """No-op coroutine for padding asyncio.gather when a task is skipped."""
    return None
