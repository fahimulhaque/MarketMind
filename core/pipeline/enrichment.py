"""Data enrichment helpers — build structured context blocks from DB."""

from __future__ import annotations

import logging
from typing import Any

from core import db
from core import llm as llm_module

logger = logging.getLogger(__name__)

# Macro series IDs referenced by the FRED provider
_MACRO_SERIES_IDS = [
    "GDP", "CPIAUCSL", "UNRATE", "FEDFUNDS", "DGS10", "VIXCLS",
    "SP500", "T10Y2Y", "DCOILWTICO", "USSLIND", "INDPRO", "CSUSHPINSA",
]


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------


def _format_compact_number(value: Any) -> str:
    if value is None:
        return "n/a"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)

    abs_number = abs(number)
    if abs_number >= 1_000_000_000_000:
        return f"{number / 1_000_000_000_000:.2f}T"
    if abs_number >= 1_000_000_000:
        return f"{number / 1_000_000_000:.2f}B"
    if abs_number >= 1_000_000:
        return f"{number / 1_000_000:.2f}M"
    return f"{number:,.2f}"


def _format_ratio_percent(value: Any) -> str:
    if value is None:
        return "n/a"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    return f"{number * 100:.1f}%"


# ---------------------------------------------------------------------------
# Financial performance
# ---------------------------------------------------------------------------


def _build_financial_performance(financial_snapshot: dict) -> dict:
    summary_lines = []
    if financial_snapshot.get("symbol"):
        summary_lines.append(f"Symbol: {financial_snapshot.get('symbol')}")
    if financial_snapshot.get("price") is not None:
        summary_lines.append(
            f"Market Price: {financial_snapshot.get('price')} {financial_snapshot.get('currency') or ''}".strip()
        )
    if financial_snapshot.get("market_cap") is not None:
        summary_lines.append(f"Market Cap: {_format_compact_number(financial_snapshot.get('market_cap'))}")
    if financial_snapshot.get("fifty_two_week_range"):
        summary_lines.append(f"52W Range: {financial_snapshot.get('fifty_two_week_range')}")

    valuation = {
        "trailing_pe": financial_snapshot.get("trailing_pe"),
        "forward_pe": financial_snapshot.get("forward_pe"),
        "peg_ratio": financial_snapshot.get("peg_ratio"),
    }

    _rev_growth = _format_ratio_percent(financial_snapshot.get("revenue_growth"))
    _earn_growth = _format_ratio_percent(financial_snapshot.get("earnings_growth"))
    growth = {
        "revenue_growth_yoy": _rev_growth,
        "revenue_growth": _rev_growth,  # alias for dashboard compat
        "earnings_growth_yoy": _earn_growth,
        "earnings_growth": _earn_growth,  # alias for dashboard compat
    }

    _gross = _format_ratio_percent(financial_snapshot.get("gross_margin"))
    _oper = _format_ratio_percent(financial_snapshot.get("operating_margin"))
    _net = _format_ratio_percent(financial_snapshot.get("profit_margin"))
    profitability = {
        "gross_margin": _gross,
        "gross_margins": _gross,  # alias for dashboard compat
        "operating_margin": _oper,
        "operating_margins": _oper,  # alias for dashboard compat
        "net_margin": _net,
        "profit_margins": _net,  # alias for dashboard compat
    }

    liquidity = {
        "debt_to_equity": financial_snapshot.get("debt_to_equity"),
        "current_ratio": financial_snapshot.get("current_ratio"),
        "next_earnings_date": financial_snapshot.get("next_earnings_date"),
    }

    return {
        "summary": "; ".join(summary_lines)
        if summary_lines
        else "No reliable financial snapshot was available from free public finance data at query time.",
        "market_cap": financial_snapshot.get("market_cap"),
        "beta": financial_snapshot.get("beta"),
        "sector": financial_snapshot.get("sector"),
        "industry": financial_snapshot.get("industry"),
        "valuation": valuation,
        "growth": growth,
        "profitability": profitability,
        "liquidity": liquidity,
    }


# ---------------------------------------------------------------------------
# Historical financial trends from stored financial_periods
# ---------------------------------------------------------------------------


def _build_historical_trends(ticker: str | None) -> dict:
    """Build quarterly & annual revenue/earnings/margin trends from DB."""
    if not ticker:
        return {"available": False, "quarters": [], "annual": []}

    try:
        quarterly = db.get_financial_history(ticker, period_type="quarterly", limit=12)
        annual = db.get_financial_history(ticker, period_type="annual", limit=5)
    except Exception as exc:
        logger.warning("Failed to load financial history for %s: %s", ticker, exc)
        return {"available": False, "quarters": [], "annual": []}

    # Inline yfinance fallback: if no quarterly data has revenue, fetch from yfinance
    has_revenue = any(
        (r.get("income_statement") or {}).get("revenue") or
        (r.get("income_statement") or {}).get("totalRevenue")
        for r in quarterly
    )
    if not has_revenue and ticker:
        try:
            from core.pipeline.yfinance_inline import inline_fetch_yfinance_quarterly
            fetched = inline_fetch_yfinance_quarterly(ticker)
            if fetched > 0:
                quarterly = db.get_financial_history(ticker, period_type="quarterly", limit=12)
                logger.info("yfinance inline fallback populated %d quarters for %s", fetched, ticker)
        except Exception as exc:
            logger.warning("yfinance inline fallback failed for %s: %s", ticker, exc)

    def _extract_period(row: dict) -> dict:
        inc = row.get("income_statement") or {}
        bs = row.get("balance_sheet") or {}
        km = row.get("key_metrics") or {}
        return {
            "period_end": str(row.get("period_end_date", "")),
            "fiscal_year": row.get("fiscal_year"),
            "fiscal_quarter": row.get("fiscal_quarter"),
            "revenue": inc.get("revenue") or inc.get("totalRevenue"),
            "net_income": inc.get("netIncome") or inc.get("net_income"),
            "gross_profit": inc.get("grossProfit") or inc.get("gross_profit"),
            "operating_income": inc.get("operatingIncome") or inc.get("operating_income"),
            "eps": inc.get("eps") or km.get("eps"),
            "total_assets": bs.get("totalAssets") or bs.get("total_assets"),
            "total_debt": bs.get("totalDebt") or bs.get("total_debt"),
            "source_provider": row.get("source_provider", ""),
        }

    q_data = [_extract_period(r) for r in quarterly]
    a_data = [_extract_period(r) for r in annual]

    # Compute simple trend direction
    trend_direction = "stable"
    if len(q_data) >= 2:
        rev_recent = q_data[0].get("revenue")
        rev_prev = q_data[1].get("revenue")
        if rev_recent and rev_prev:
            try:
                change = (float(rev_recent) - float(rev_prev)) / abs(float(rev_prev))
                if change > 0.05:
                    trend_direction = "growing"
                elif change < -0.05:
                    trend_direction = "declining"
            except (TypeError, ValueError, ZeroDivisionError):
                pass

    return {
        "available": len(q_data) > 0 or len(a_data) > 0,
        "trend_direction": trend_direction,
        "quarters_available": len(q_data),
        "quarters": q_data[:8],  # Last 8 quarters
        "annual": a_data[:5],  # Last 5 years
    }


# ---------------------------------------------------------------------------
# Macro economic context from FRED indicators
# ---------------------------------------------------------------------------


def _build_macro_context() -> dict:
    """Load latest macro indicator values from DB."""
    try:
        latest = db.get_latest_macro_values(_MACRO_SERIES_IDS)
    except Exception as exc:
        logger.warning("Failed to load macro indicators: %s", exc)
        return {"available": False, "indicators": {}}

    if not latest:
        return {"available": False, "indicators": {}}

    # Summarize key indicators
    summary_parts = []
    gdp = latest.get("GDP")
    if gdp:
        summary_parts.append(f"GDP: {_format_compact_number(gdp['value'])}")
    cpi = latest.get("CPIAUCSL")
    if cpi:
        summary_parts.append(f"CPI: {cpi['value']:.1f}")
    unrate = latest.get("UNRATE")
    if unrate:
        summary_parts.append(f"Unemployment: {unrate['value']:.1f}%")
    fedfunds = latest.get("FEDFUNDS")
    if fedfunds:
        summary_parts.append(f"Fed Rate: {fedfunds['value']:.2f}%")
    vix = latest.get("VIXCLS")
    if vix:
        summary_parts.append(f"VIX: {vix['value']:.1f}")

    return {
        "available": True,
        "summary": " | ".join(summary_parts) if summary_parts else "Macro data available but no key series populated.",
        "indicators": latest,
    }


# ---------------------------------------------------------------------------
# Social sentiment from Reddit/social signals
# ---------------------------------------------------------------------------


def _build_social_sentiment(ticker: str | None) -> dict:
    """Load recent social signals for the entity."""
    if not ticker:
        return {"available": False, "signals": [], "summary": ""}

    try:
        signals = db.get_social_signals(ticker, days=7)
    except Exception as exc:
        logger.warning("Failed to load social signals for %s: %s", ticker, exc)
        return {"available": False, "signals": [], "summary": ""}

    if not signals:
        return {"available": False, "signals": [], "summary": "No recent social signals found."}

    total_mentions = sum(int(s.get("mention_count", 0)) for s in signals)
    avg_sent = 0.0
    if signals:
        sentiments = [float(s.get("avg_sentiment", 0.0)) for s in signals]
        avg_sent = sum(sentiments) / len(sentiments) if sentiments else 0.0

    sentiment_label = "neutral"
    if avg_sent > 0.2:
        sentiment_label = "bullish"
    elif avg_sent < -0.2:
        sentiment_label = "bearish"

    # Extract top posts across all signal days
    top_posts = []
    for s in signals:
        posts = s.get("top_posts") or []
        if isinstance(posts, list):
            top_posts.extend(posts)
    top_posts = top_posts[:5]

    return {
        "available": True,
        "total_mentions_7d": total_mentions,
        "avg_sentiment": round(avg_sent, 3),
        "sentiment_label": sentiment_label,
        "summary": f"{total_mentions} mentions over 7 days, sentiment: {sentiment_label} ({avg_sent:.2f})",
        "days_data": len(signals),
        "top_posts": top_posts,
    }


# ---------------------------------------------------------------------------
# Entity coverage assessment
# ---------------------------------------------------------------------------


def _build_coverage_assessment(
    ticker: str | None,
    financial_snapshot: dict | None = None,
    social_sentiment: dict | None = None,
) -> dict:
    """Load entity coverage score and breakdown, overlaying real-time signals."""
    if not ticker:
        return {"available": False, "score": 0.0, "breakdown": {}}

    try:
        coverage = db.get_entity_coverage(ticker)
    except Exception as exc:
        logger.warning("Failed to load coverage for %s: %s", ticker, exc)
        coverage = None

    # Start with DB coverage or empty baseline
    breakdown = {
        "has_financials": (coverage or {}).get("has_financials", False),
        "financials_quarters": (coverage or {}).get("financials_quarters", 0),
        "has_filings": (coverage or {}).get("has_filings", False),
        "filings_count": (coverage or {}).get("filings_count", 0),
        "has_macro": (coverage or {}).get("has_macro", False),
        "has_social": (coverage or {}).get("has_social", False),
        "has_news": (coverage or {}).get("has_news", False),
        "has_price": (coverage or {}).get("has_price", False),
    }

    # Overlay real-time financial snapshot signals
    fin = financial_snapshot or {}
    if fin.get("price") is not None and fin.get("trailing_pe") is not None:
        breakdown["has_financials"] = True
    if fin.get("fifty_two_week_range") or (fin.get("fifty_two_week_low") is not None):
        breakdown["has_price"] = True

    # Overlay real-time social sentiment signals
    soc = social_sentiment or {}
    if soc.get("available"):
        breakdown["has_social"] = True

    # Recompute score from breakdown (weighted sum)
    score_parts = 0.0
    total_weight = 0.0
    weights = {
        "has_financials": 0.25, "has_filings": 0.15, "has_macro": 0.10,
        "has_social": 0.10, "has_news": 0.20, "has_price": 0.20,
    }
    for key, weight in weights.items():
        total_weight += weight
        if breakdown.get(key):
            score_parts += weight
    computed_score = round(score_parts / total_weight, 4) if total_weight > 0 else 0.0

    # Use the higher of DB score vs computed score
    db_score = round(float((coverage or {}).get("coverage_score", 0)), 4)
    final_score = max(db_score, computed_score)

    return {
        "available": True,
        "score": final_score,
        "breakdown": breakdown,
        "last_updated": str((coverage or {}).get("last_updated", "")),
    }


# ---------------------------------------------------------------------------
# Filings summary (SEC 10-K, 10-Q, 8-K, etc.)
# ---------------------------------------------------------------------------


def _build_filings_summary(ticker: str | None) -> dict:
    """Load recent SEC filings for the entity."""
    if not ticker:
        return {"available": False, "filings": []}

    try:
        filings = db.get_entity_filings(ticker, limit=10)
    except Exception as exc:
        logger.warning("Failed to load filings for %s: %s", ticker, exc)
        return {"available": False, "filings": []}

    if not filings:
        return {"available": False, "filings": []}

    return {
        "available": True,
        "count": len(filings),
        "filings": [
            {
                "type": f.get("filing_type", ""),
                "date": str(f.get("filing_date", "")),
                "description": f.get("description", ""),
                "url": f.get("filing_url", ""),
            }
            for f in filings[:10]
        ],
    }


# ---------------------------------------------------------------------------
# Scenario building
# ---------------------------------------------------------------------------


def _build_scenarios(
    decision: dict,
    evidence: list[dict],
    financials: dict | None = None,
    historical: dict | None = None,
    macro: dict | None = None,
    query_text: str = "",
) -> list[dict]:
    base_confidence = float(decision.get("confidence", 0.5) or 0.5)
    top_source = evidence[0].get("source_name", "current evidence") if evidence else "current evidence"

    # --- Try LLM-powered scenarios first ---
    evidence_dicts = [
        {
            "source_name": item.get("source_name", "unknown"),
            "insight": (item.get("insight") or "")[:200],
            "confidence": item.get("confidence", 0),
            "threat_level": item.get("threat_level", "low"),
        }
        for item in evidence[:5]
    ]
    llm_scenarios = llm_module.generate_scenarios(
        query=query_text or "market analysis",
        top_evidence=evidence_dicts,
        financials=financials or {},
        historical=historical or {},
        macro=macro or {},
    )
    if llm_scenarios and len(llm_scenarios) == 3:
        # Validate and normalize probabilities
        total_prob = sum(float(s.get("probability", 0.33)) for s in llm_scenarios)
        if total_prob > 0:
            for s in llm_scenarios:
                s["probability"] = round(float(s.get("probability", 0.33)) / total_prob, 3)
                s.setdefault("trigger_signals", [])
                s.setdefault("assumption", "")
                s.setdefault("impact", "")
        return llm_scenarios

    # --- Fallback to arithmetic scenarios ---
    bull_raw = min(base_confidence + 0.12, 0.92)
    base_raw = max(min(base_confidence, 0.8), 0.1)
    bear_raw = max(1.0 - base_confidence + 0.05, 0.1)
    total = bull_raw + base_raw + bear_raw

    bull_prob = round(bull_raw / total, 3)
    base_prob = round(base_raw / total, 3)
    bear_prob = round(max(1.0 - bull_prob - base_prob, 0.0), 3)

    return [
        {
            "name": "bull",
            "probability": bull_prob,
            "assumption": "Positive execution and demand signals hold across latest sources.",
            "impact": f"Upside scenario if momentum from {top_source} continues.",
            "trigger_signals": ["accelerating revenue growth", "margin expansion", "positive narrative shift"],
        },
        {
            "name": "base",
            "probability": base_prob,
            "assumption": "Current trajectory persists without major external shocks.",
            "impact": "Moderate performance with manageable risk and incremental changes.",
            "trigger_signals": ["stable guidance", "mixed but non-deteriorating sentiment", "controlled risk levels"],
        },
        {
            "name": "bear",
            "probability": bear_prob,
            "assumption": "Competitive pressure or macro events weaken current momentum.",
            "impact": "Downside risk rises; defensive posture and tighter monitoring required.",
            "trigger_signals": ["negative earnings revisions", "rising risk indicators", "narrative deterioration"],
        },
    ]
