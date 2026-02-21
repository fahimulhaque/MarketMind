"""Streaming generation variants — word-by-word SSE token streams."""

from __future__ import annotations

from typing import Generator

from core.llm.providers import ollama_generate_stream
from core.llm.prompts import (
    _SYSTEM_ANALYST,
    _SYSTEM_COMPETITIVE,
    _build_competitive_landscape_prompt,
    _build_executive_summary_prompt,
    _build_market_narrative_prompt,
)


def generate_executive_summary_stream(
    query: str,
    top_evidence: list[dict],
    financials: dict,
    macro: dict,
    sentiment: dict,
    historical: dict,
) -> Generator[str, None, None]:
    """Stream executive summary tokens word-by-word."""
    prompt = _build_executive_summary_prompt(query, top_evidence, financials, macro, sentiment, historical)
    yield from ollama_generate_stream(prompt, system=_SYSTEM_ANALYST, temperature=0.25, max_tokens=384)


def generate_market_narrative_stream(
    query: str,
    top_evidence: list[dict],
    financials: dict,
    historical: dict,
    macro: dict,
    sentiment: dict,
    coverage_score: float,
    executive_verdict: str = "",
) -> Generator[str, None, None]:
    """Stream market narrative tokens word-by-word."""
    prompt = _build_market_narrative_prompt(query, top_evidence, financials, historical, macro, sentiment, coverage_score, executive_verdict)
    yield from ollama_generate_stream(prompt, system=_SYSTEM_ANALYST, temperature=0.3, max_tokens=512)


def generate_competitive_landscape_stream(
    query: str,
    ticker: str,
    top_evidence: list[dict],
    financials: dict,
    sector: str = "",
    industry: str = "",
) -> Generator[str, None, None]:
    """Stream competitive landscape tokens word-by-word."""
    prompt = _build_competitive_landscape_prompt(query, ticker, top_evidence, financials, sector, industry)
    yield from ollama_generate_stream(prompt, system=_SYSTEM_COMPETITIVE, temperature=0.3, max_tokens=384)
