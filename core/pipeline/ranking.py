"""Evidence ranking, relevance scoring, and validation helpers."""

from __future__ import annotations

import re as _re
from datetime import datetime, timezone
from typing import Any


def _recency_score(created_at: Any) -> float:
    if created_at is None:
        return 0.0
    now = datetime.now(timezone.utc)
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)
    age_hours = max((now - created_at).total_seconds() / 3600.0, 0.0)
    return 1.0 / (1.0 + age_hours / 24.0)


def _source_quality_factor(source_name: str | None, evidence_ref: str | None) -> float:
    source = (source_name or "").lower()
    ref = (evidence_ref or "").lower()

    if "sec" in source or "edgar" in source or "sec.gov" in ref:
        return 1.0
    if "yahoo finance" in source or "finance.yahoo.com" in ref:
        return 0.98
    if "fmp" in source or "alpha vantage" in source:
        return 0.95
    if "google news" in source or "news.google.com" in ref:
        return 0.9
    if "rss" in source:
        return 0.85
    if "reddit" in source:
        return 0.7
    if "duckduckgo" in source:
        return 0.75
    return 0.8


def _token_relevance(query_tokens: list[str], insight: str | None, recommendation: str | None) -> float:
    text = f"{insight or ''} {recommendation or ''}".lower()
    if not query_tokens:
        return 0.0
    matches = sum(1 for token in query_tokens if token in text)
    return min(matches / max(len(query_tokens), 1), 1.0)


def _entity_relevance(ticker: str | None, entity_name: str | None, item: dict) -> float:
    """Score how relevant an evidence item is to the resolved entity.

    Returns 1.0 (strong match) to 0.0 (no match).
    """
    if not ticker and not entity_name:
        return 0.5  # Can't assess — neutral score

    title = (item.get("source_name") or "").lower()
    insight = (item.get("insight") or "").lower()
    evidence_ref = (item.get("evidence_ref") or "").lower()
    text = f"{title} {insight} {evidence_ref}"

    score = 0.0
    tick_lower = (ticker or "").lower()
    name_lower = (entity_name or "").lower()

    # Stop-word exclusion: common corporate suffixes that cause false positives
    _ENTITY_STOP_WORDS = {
        "limited", "inc", "corp", "company", "group", "holdings",
        "technologies", "international", "services", "the", "and",
        "new", "one", "first", "global", "systems", "solutions",
        "enterprises", "partners", "capital", "financial", "industries",
        "associates", "consulting", "management", "ltd", "plc", "llc",
        "co", "sa", "ag", "nv", "se", "gmbh",
    }
    name_parts = [
        p for p in name_lower.replace(",", "").replace(".", "").split()
        if len(p) > 2 and p not in _ENTITY_STOP_WORDS
    ]

    # Ticker matching — use word-boundary regex to avoid "A" matching everything
    if tick_lower and len(tick_lower) >= 1:
        tick_pattern = _re.compile(r'\b' + _re.escape(tick_lower) + r'\b', _re.IGNORECASE)
        if tick_pattern.search(title):
            score = max(score, 1.0)
        elif tick_pattern.search(text):
            score = max(score, 0.8)

    # Company name in title
    if name_lower and name_lower in title:
        score = max(score, 0.95)
    # Major name parts in title (e.g. "Tesla" from "Tesla, Inc.")
    elif name_parts:
        name_hits = sum(1 for p in name_parts if p in title)
        if name_hits > 0:
            score = max(score, 0.85 * (name_hits / len(name_parts)))

    # Company name fragments in body
    if score < 0.5 and name_parts:
        body_hits = sum(1 for p in name_parts if p in text)
        if body_hits > 0:
            score = max(score, 0.4 * (body_hits / len(name_parts)))

    return round(score, 4)


def _validate_financial_snapshot(snapshot: dict) -> list[str]:
    """Sanity check financial data for improbable values (The 'Math Bug' fix)."""
    warnings = []
    if not snapshot:
        return warnings

    # 1. Growth validation
    for key in ["revenue_growth", "earnings_growth"]:
        val = snapshot.get(key)
        if val is not None and isinstance(val, (int, float)):
             # > 500% growth is suspicious unless it's a tiny base or startup
            if val > 5.0:
                warnings.append(f"EXTREME_VALUE: {key} > 500% ({val:.1%}). Verify source.")
            elif val < -0.9:
                warnings.append(f"EXTREME_VALUE: {key} < -90% ({val:.1%}). Verify source.")

    # 2. Margin validation
    if snapshot.get("gross_margin") is not None and snapshot.get("operating_margin") is not None:
        gm = float(snapshot["gross_margin"])
        om = float(snapshot["operating_margin"])
        if om > gm:
             warnings.append(f"LOGIC_ERROR: Operating Margin ({om:.1%}) > Gross Margin ({gm:.1%}).")

    return warnings


def _rank_items(items: list[dict], query_context: dict) -> list[dict]:
    ranked: list[dict] = []
    tokens = query_context.get("tokens", [])
    ticker = query_context.get("ticker")
    entity_name = query_context.get("entity")

    # Sector filtering (The 'Competitor Bug' fix)
    # Handle case where entity_record is explicitly None
    entity_rec = query_context.get("entity_record") or {}
    entity_sector = (entity_rec.get("sector") or "").lower()

    for item in items:
        confidence = float(item.get("confidence", 0.0) or 0.0)
        text_rank = float(item.get("text_rank", 0.0) or 0.0)
        recency = _recency_score(item.get("created_at"))
        critic_status = item.get("critic_status", "approved")

        # Penalize unapproved items slightly more
        critic_factor = 1.0 if critic_status == "approved" else 0.5

        source_factor = _source_quality_factor(item.get("source_name"), item.get("evidence_ref"))
        token_relevance = _token_relevance(tokens, item.get("insight"), item.get("recommendation"))
        semantic_score = float(item.get("similarity_score", 0.0) or 0.0)
        entity_rel = _entity_relevance(ticker, entity_name, item)

        # Sector mismatch penalty
        sector_penalty = 1.0
        item_text = (dict(item).get("insight") or "").lower()
        if entity_sector and len(entity_sector) > 3:
             # Very crude check: if we know the sector, and the item mentions a DIFFERENT known sector heavily?
             # For now, let's keep it simple: if the item mentions the *correct* sector, boost it slightly.
             if entity_sector in item_text:
                 sector_penalty = 1.1

        # Check for cross-pollution from other search feeds (e.g. "Google News: Apple" showing up for "Microsoft")
        pollution_penalty = 1.0
        src_name = str(item.get("source_name", "")).lower()
        if "google news:" in src_name or "yahoo finance news:" in src_name:
             if entity_name and entity_name.lower() not in src_name:
                 # This source is likely from a different, older search query -> downweight heavily
                 pollution_penalty = 0.2

        # Adjusted Weights (The 'Priority Bug' fix)
        # - Reduced Recency (0.12 -> 0.05) to stop "news buzz" from flooding results
        # - Increased Entity Relevance (0.30 -> 0.35)
        # - Increased Source Quality (0.08 -> 0.15) to favor SEC/FMP
        rank_score = (
            (0.35 * entity_rel)
            + (0.15 * source_factor)      # Boosted
            + (0.15 * confidence)
            + (0.10 * semantic_score)
            + (0.10 * text_rank)
            + (0.10 * token_relevance)
            + (0.05 * recency)            # Reduced heavily
        )
        rank_score *= (critic_factor * sector_penalty * pollution_penalty)

        enriched = dict(item)
        enriched["recency_score"] = round(recency, 4)
        enriched["source_quality"] = round(source_factor, 4)
        enriched["token_relevance"] = round(token_relevance, 4)
        enriched["semantic_score"] = round(semantic_score, 4)
        enriched["entity_relevance"] = entity_rel
        enriched["rank_score"] = round(rank_score, 4)
        ranked.append(enriched)

    ranked.sort(key=lambda x: x["rank_score"], reverse=True)

    # Hard filter: drop low entity_relevance items unless too few relevant results
    relevant = [r for r in ranked if r.get("entity_relevance", 0) > 0.3]
    if len(relevant) >= 3:
        ranked = relevant

    # Deduplicate: keep highest-scored item per (source_name, insight_hash)
    import hashlib
    seen: dict[str, dict] = {}
    for item in ranked:
        insight_text = (item.get("insight") or "")[:200].strip().lower()
        src = item.get("source_name", "")
        dedup_key = f"{src}::{hashlib.md5(insight_text.encode()).hexdigest()}"
        if dedup_key not in seen or item["rank_score"] > seen[dedup_key]["rank_score"]:
            seen[dedup_key] = item
    ranked = sorted(seen.values(), key=lambda x: x["rank_score"], reverse=True)

    return ranked


def _detect_contradictions(items: list[dict]) -> list[dict]:
    threat_levels = {item.get("threat_level", "low") for item in items[:8]}
    contradictions: list[dict] = []

    if "high" in threat_levels and "low" in threat_levels:
        contradictions.append(
            {
                "type": "threat_level_conflict",
                "detail": "Evidence contains both high-risk and low-risk interpretations.",
            }
        )

    action_words = ["act", "immediate", "respond", "accelerate", "launch"]
    wait_words = ["monitor", "continue", "observe", "hold", "wait"]
    action_found = False
    wait_found = False

    for item in items[:8]:
        recommendation = (item.get("recommendation") or "").lower()
        if any(word in recommendation for word in action_words):
            action_found = True
        if any(word in recommendation for word in wait_words):
            wait_found = True

    if action_found and wait_found:
        contradictions.append(
            {
                "type": "recommendation_conflict",
                "detail": "Evidence recommends both immediate action and monitor-only posture.",
            }
        )

    return contradictions


def _build_signal_shifts(items: list[dict]) -> list[str]:
    shifts: list[str] = []
    for item in items[:3]:
        source = item.get("source_name", "unknown source")
        threat = item.get("threat_level", "low")
        confidence = item.get("confidence", 0.0)
        shifts.append(f"{source}: {threat} risk signal at confidence {confidence}.")
    if not shifts:
        shifts.append("No strong market shift detected from current evidence.")
    # Deduplicate while preserving order
    return list(dict.fromkeys(shifts))


def _needs_refresh(items: list[dict], min_evidence: int = 3, stale_after_hours: int = 18) -> bool:
    if len(items) < min_evidence:
        return True

    freshest = max(
        [item.get("created_at") for item in items if item.get("created_at") is not None],
        default=None,
    )
    if freshest is None:
        return True
    if freshest.tzinfo is None:
        freshest = freshest.replace(tzinfo=timezone.utc)

    age_hours = (datetime.now(timezone.utc) - freshest).total_seconds() / 3600.0
    return age_hours > stale_after_hours
