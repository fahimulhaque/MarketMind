"""Formatting helpers for LLM prompt data blocks."""

from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


def _fmt(value: Any) -> str:
    """Format a number compactly."""
    if value is None:
        return "n/a"
    try:
        n = float(value)
    except (TypeError, ValueError):
        return str(value)
    a = abs(n)
    if a >= 1e12:
        return f"${n / 1e12:.1f}T"
    if a >= 1e9:
        return f"${n / 1e9:.1f}B"
    if a >= 1e6:
        return f"${n / 1e6:.1f}M"
    return f"${n:,.0f}"


def _format_evidence_block(items: list[dict]) -> str:
    if not items:
        return "No evidence available."
    lines = []
    for i, item in enumerate(items, 1):
        source = item.get("source_name", "unknown")
        insight = item.get("insight", "")[:200]
        conf = item.get("confidence", 0)
        threat = item.get("threat_level", "low")
        lines.append(f"{i}. [{source}] (confidence={conf}, threat={threat}) {insight}")
    return "\n".join(lines)


def _format_financials_block(snapshot: dict) -> str:
    if not snapshot or not snapshot.get("symbol"):
        return "No financial snapshot available."
    src = snapshot.get("source", "market_data")
    parts = [
        f"Symbol: {snapshot.get('symbol')}",
        f"Price: {snapshot.get('price')} {snapshot.get('currency', '')} (Source: {src})".strip(),
        f"Market Cap: {_fmt(snapshot.get('market_cap'))} (Source: {src})",
        f"P/E (trailing): {snapshot.get('trailing_pe', 'n/a')} (Source: {src})",
        f"P/E (forward): {snapshot.get('forward_pe', 'n/a')} (Source: {src})",
        f"Revenue Growth YoY: {_pct(snapshot.get('revenue_growth'))} (Source: {src})",
        f"Earnings Growth YoY: {_pct(snapshot.get('earnings_growth'))} (Source: {src})",
        f"Gross Margin: {_pct(snapshot.get('gross_margin'))} (Source: {src})",
        f"Operating Margin: {_pct(snapshot.get('operating_margin'))} (Source: {src})",
        f"Net Margin: {_pct(snapshot.get('profit_margin'))} (Source: {src})",
        f"Debt/Equity: {snapshot.get('debt_to_equity', 'n/a')} (Source: {src})",
        f"52W Range: {snapshot.get('fifty_two_week_range', 'n/a')} (Source: {src})",
    ]
    return "\n".join(parts)


def _format_macro_block(macro: dict) -> str:
    if not macro or not macro.get("available"):
        return "No macro data available."
    indicators = macro.get("indicators", {})
    if not indicators:
        return "Macro data flag set but no indicators populated."
    lines = []
    for sid, info in indicators.items():
        name = info.get("name", sid)
        val = info.get("value")
        date = info.get("date", "")
        lines.append(f"{name}: {val if val is not None else 'n/a'} (as of {date})")
    return "\n".join(lines)


def _format_sentiment_block(sentiment: dict) -> str:
    if not sentiment or not sentiment.get("available"):
        return "No social sentiment data available."
    return (
        f"Mentions (7d): {sentiment.get('total_mentions_7d', 0)}\n"
        f"Avg Sentiment: {sentiment.get('avg_sentiment', 0):.2f} ({sentiment.get('sentiment_label', 'neutral')})\n"
        f"Days of data: {sentiment.get('days_data', 0)}"
    )


def _format_trend_block(historical: dict) -> str:
    if not historical or not historical.get("available"):
        return "No historical financial data available."
    quarters = historical.get("quarters", [])[:4]
    if not quarters:
        return "Historical flag set but no periods available."
    lines = [f"Trend direction: {historical.get('trend_direction', 'unknown')}"]
    for q in quarters:
        lines.append(
            f"  {q.get('period_end', '?')}: Rev={_fmt(q.get('revenue'))} NI={_fmt(q.get('net_income'))}"
        )
    return "\n".join(lines)


def _pct(value: Any) -> str:
    if value is None:
        return "n/a"
    try:
        return f"{float(value) * 100:.1f}%"
    except (TypeError, ValueError):
        return str(value)


def _parse_json_array(text: str) -> list[dict] | None:
    """Extract a JSON array from LLM output, handling markdown fences."""
    cleaned = text.strip()
    # Strip markdown code fences
    if cleaned.startswith("```"):
        lines = cleaned.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        cleaned = "\n".join(lines).strip()
    # Find the array boundaries
    start = cleaned.find("[")
    end = cleaned.rfind("]")
    if start == -1 or end == -1:
        return None
    try:
        return json.loads(cleaned[start : end + 1])
    except json.JSONDecodeError:
        logger.warning("Failed to parse LLM JSON output")
        return None
