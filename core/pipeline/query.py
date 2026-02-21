"""Query parsing and entity resolution for the intelligence pipeline."""

from __future__ import annotations

import logging

from core.entities import resolve_entity

logger = logging.getLogger(__name__)


def _parse_query(query_text: str) -> dict:
    """Parse query text into structured context with entity resolution."""
    lowered = query_text.lower().strip()
    tokens = [token for token in lowered.replace(",", " ").split() if token]

    timeframe = "current"
    if any(token in tokens for token in ["quarter", "q1", "q2", "q3", "q4"]):
        timeframe = "quarter"
    elif any(token in tokens for token in ["year", "annual", "yoy"]):
        timeframe = "year"
    elif any(token in tokens for token in ["week", "today", "latest", "recent"]):
        timeframe = "recent"

    intent = "general"
    if any(token in tokens for token in ["risk", "threat", "exposure"]):
        intent = "risk"
    elif any(token in tokens for token in ["growth", "revenue", "earnings", "profit", "margin"]):
        intent = "financial"
    elif any(token in tokens for token in ["pricing", "competition", "market", "strategy"]):
        intent = "market"

    # Entity resolution — resolve free-text to canonical ticker/name
    entity_record = None
    try:
        entity_record = resolve_entity(query_text)
    except Exception as exc:
        logger.warning("Entity resolution failed for %r: %s", query_text, exc)

    entity_name = query_text.split()[0] if query_text.split() else query_text
    ticker = None
    if entity_record:
        entity_name = entity_record.get("name") or entity_name
        ticker = entity_record.get("ticker")

    return {
        "entity": entity_name,
        "ticker": ticker,
        "entity_record": entity_record,
        "timeframe": timeframe,
        "intent": intent,
        "tokens": tokens,
    }
