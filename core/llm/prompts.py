"""System prompts and prompt builders for all LLM generation tasks."""

from __future__ import annotations

from core.llm.formatters import (
    _fmt,
    _format_evidence_block,
    _format_financials_block,
    _format_macro_block,
    _format_sentiment_block,
    _format_trend_block,
)


# ---------------------------------------------------------------------------
# System prompts
# ---------------------------------------------------------------------------

# Analyst system prompt (used for executive summary, narrative, recommendation, etc.)
_SYSTEM_ANALYST = (
    "You are the voice of a proprietary financial terminal. "
    "Write as an authoritative analyst delivering a briefing to a portfolio manager. "
    "State facts and conclusions directly — never say 'Based on the provided data', "
    "'According to the data', 'The data suggests', or similar hedging phrases. "
    "The reader knows the data came from this terminal; do not reference your own process. "
    "Cite specific numbers inline (e.g. 'Revenue grew 12% YoY to $53.8B'). "
    "Be concise, assertive, and decision-ready. Avoid filler sentences. "
    "Resolve conflicts between Evidence and Financials; do not hallucinate missing data if Summary has it."
)

_SYSTEM_SCENARIO = (
    "You are a scenario planning strategist at a hedge fund. "
    "Construct three scenarios (bull, base, bear) with specific probability estimates, "
    "concrete assumptions tied to real metrics, and measurable trigger signals. "
    "Probabilities must reflect the actual data — if financials are strong, bull should be higher. "
    "Write assertively. Never say 'Based on the provided data' or similar hedges. "
    "Output valid JSON only, no other text."
)

_SYSTEM_COMPETITIVE = (
    "You are a competitive intelligence analyst delivering a terminal briefing. "
    "Identify key competitors, market positioning, competitive advantages and threats. "
    "Be specific about market share, product differentiation, and strategic moves. "
    "State findings directly — never reference 'the data' or your own analysis process. "
    "Cite evidence inline with specific numbers."
)


# ---------------------------------------------------------------------------
# Prompt builders
# ---------------------------------------------------------------------------


def _build_executive_summary_prompt(
    query: str,
    top_evidence: list[dict],
    financials: dict,
    macro: dict,
    sentiment: dict,
    historical: dict,
) -> str:
    """Build the executive summary prompt (shared by batch + stream)."""
    evidence_block = _format_evidence_block(top_evidence[:5])
    fin_block = _format_financials_block(financials)
    macro_block = _format_macro_block(macro)
    sentiment_block = _format_sentiment_block(sentiment)
    trend_block = _format_trend_block(historical)

    return f"""Analyze the following market intelligence for the query: "{query}"

=== TOP EVIDENCE ===
{evidence_block}

=== FINANCIAL SNAPSHOT ===
{fin_block}

=== HISTORICAL TRENDS ===
{trend_block}

=== MACRO CONTEXT ===
{macro_block}

=== SOCIAL SENTIMENT ===
{sentiment_block}

Write a structured executive summary using strict Markdown.
Follow this EXACT format:

# [Punchy, data-driven Headline (max 8 words)]

## VERDICT
[One clear sentence stating Bullish/Bearish/Neutral stance with conviction level.]

## KEY DRIVERS
* **[Driver 1]**: [Brief explanation citing specific numbers]
* **[Driver 2]**: [Brief explanation citing specific numbers]
* **[Risk/Catalyst]**: [Brief explanation]

Do not use preamble. Go straight to the # Headline."""


def _build_market_narrative_prompt(
    query: str,
    top_evidence: list[dict],
    financials: dict,
    historical: dict,
    macro: dict,
    sentiment: dict,
    coverage_score: float,
    executive_verdict: str = "",
) -> str:
    """Build the market narrative prompt (shared by batch + stream)."""
    evidence_block = _format_evidence_block(top_evidence[:6])
    fin_block = _format_financials_block(financials)
    trend_block = _format_trend_block(historical)
    macro_block = _format_macro_block(macro)
    sentiment_block = _format_sentiment_block(sentiment)

    coverage_note = ""
    if coverage_score < 0.3:
        coverage_note = "NOTE: Data coverage is thin (score {:.0%}). Acknowledge gaps explicitly.".format(coverage_score)
    elif coverage_score >= 0.7:
        coverage_note = "Data coverage is good (score {:.0%}).".format(coverage_score)

    # Verdict consistency constraint
    verdict_note = ""
    if executive_verdict:
        verdict_note = (
            f"\n=== EXECUTIVE VERDICT (already issued) ===\n"
            f"{executive_verdict}\n\n"
            f"IMPORTANT: Your narrative MUST be consistent with the above verdict. "
            f"Do not contradict the recommendation or risk assessment.\n"
        )

    return f"""Write a market intelligence narrative for: "{query}"

=== EVIDENCE ===
{evidence_block}

=== FINANCIALS ===
{fin_block}

=== HISTORICAL TRENDS ===
{trend_block}

=== MACRO ENVIRONMENT ===
{macro_block}

=== SOCIAL SENTIMENT ===
{sentiment_block}

{coverage_note}
{verdict_note}
Write a deep-dive analysis using these Markdown sections:

## CURRENT SITUATION
[What the data shows regarding financial health, trajectory, and key metrics. Cite numbers.]

## MARKET DYNAMICS
[Macro environment, competitive pressures, and sentiment signals.]

## OUTLOOK & WATCHLIST
[Upcoming catalysts, risk factors, and what to monitor next.]

Ground every claim in specific data from above. No generic statements."""


def _build_competitive_landscape_prompt(
    query: str,
    ticker: str,
    top_evidence: list[dict],
    financials: dict,
    sector: str = "",
    industry: str = "",
) -> str:
    """Build the competitive landscape prompt (shared by batch + stream)."""
    evidence_block = _format_evidence_block(top_evidence[:6])
    fin_block = _format_financials_block(financials)

    sector_info = ""
    if sector or industry:
        sector_info = f"Sector: {sector}  |  Industry: {industry}"

    return f"""Analyze the competitive landscape for {query} ({ticker}):

{sector_info}

=== FINANCIAL POSITION ===
{fin_block}

=== MARKET EVIDENCE ===
{evidence_block}

Write a competitive analysis using these Markdown sections:

## COMPETITIVE POSITION
[Key competitors, market share dynamics, and positioning.]

## ADVANTAGES
[Moats, unique strengths, or distinct capabilities.]

## STRATEGIC THREATS
[Vulnerabilities and moves to watch in the next 6-12 months.]

Ground every claim in the data provided."""


def _build_scenarios_prompt(
    query: str,
    top_evidence: list[dict],
    financials: dict,
    historical: dict,
    macro: dict,
) -> str:
    """Build the scenario planning prompt."""
    evidence_block = _format_evidence_block(top_evidence[:5])
    fin_block = _format_financials_block(financials)
    trend_block = _format_trend_block(historical)
    macro_block = _format_macro_block(macro)

    return f"""Given this market intelligence for "{query}":

=== EVIDENCE ===
{evidence_block}

=== FINANCIALS ===
{fin_block}

=== HISTORICAL TRENDS ===
{trend_block}

=== MACRO ===
{macro_block}

Generate three scenarios. Respond ONLY with a JSON array, no other text:
[
  {{
    "name": "bull",
    "probability": <0.0-1.0>,
    "assumption": "<specific assumption grounded in the data>",
    "impact": "<concrete impact description with numbers if possible>",
    "trigger_signals": ["<signal 1>", "<signal 2>", "<signal 3>"]
  }},
  {{
    "name": "base",
    "probability": <0.0-1.0>,
    "assumption": "...",
    "impact": "...",
    "trigger_signals": ["...", "...", "..."]
  }},
  {{
    "name": "bear",
    "probability": <0.0-1.0>,
    "assumption": "...",
    "impact": "...",
    "trigger_signals": ["...", "...", "..."]
  }}
]

Probabilities must sum to 1.0. Base assumptions on actual data provided. Verify any percentage calculations against the current price to ensure they are mathematically accurate.
"""


def _build_recommendation_prompt(
    query: str,
    decision_context: dict,
    contradictions: list[dict],
    coverage_score: float,
) -> str:
    """Build the recommendation prompt."""
    contra_text = ""
    if contradictions:
        contra_lines = [f"- {c.get('type', '')}: {c.get('detail', '')}" for c in contradictions]
        contra_text = "CONTRADICTIONS:\n" + "\n".join(contra_lines)

    price_text = ""
    if decision_context.get('current_price') is not None:
        price_text = f"Current Price: {decision_context.get('current_price')}\n"

    return f"""Decision context for "{query}":

Risk Level: {decision_context.get('risk_level', 'unknown')}
Confidence: {decision_context.get('confidence', 0)}
{price_text}Current Summary: {decision_context.get('answer_summary', '')}
Data Coverage: {coverage_score:.0%}
{contra_text}

Write a direct, assertive recommendation in 2-3 sentences. Plain text only, no markdown.
Rules:
1. Start with a clear action verb (BUY / SELL / HOLD / ACCUMULATE / REDUCE / MONITOR).
2. State specific conditions or price triggers to watch.
3. Include timeline or urgency.
4. NEVER start with 'Based on the provided data', 'The data suggests', or any similar hedge.
5. Write as if you ARE the terminal delivering a verdict, not an AI summarizing data.
6. Ensure any price targets or percentage changes are mathematically accurate and calculated based on the Current Price (if provided).

If data coverage is low, state what is missing and recommend gathering it before acting."""


def _build_trend_analysis_prompt(
    ticker: str,
    quarterly_data: list[dict],
    annual_data: list[dict],
) -> str:
    """Build the trend analysis prompt."""
    q_lines = []
    for q in quarterly_data[:8]:
        q_lines.append(
            f"  {q.get('period_end', '?')}: "
            f"Rev={_fmt(q.get('revenue'))} "
            f"NI={_fmt(q.get('net_income'))} "
            f"GP={_fmt(q.get('gross_profit'))} "
            f"EPS={q.get('eps', 'n/a')}"
        )

    a_lines = []
    for a in annual_data[:5]:
        a_lines.append(
            f"  {a.get('period_end', '?')}: "
            f"Rev={_fmt(a.get('revenue'))} "
            f"NI={_fmt(a.get('net_income'))} "
            f"GP={_fmt(a.get('gross_profit'))}"
        )

    return f"""Analyze the financial trends for {ticker}:

QUARTERLY (most recent first):
{chr(10).join(q_lines) if q_lines else '  No quarterly data'}

ANNUAL (most recent first):
{chr(10).join(a_lines) if a_lines else '  No annual data'}

Write a 2-3 sentence analysis covering:
1. Revenue trajectory (growing/declining/stable, acceleration/deceleration)
2. Margin trends (gross/net margin compression or expansion)
3. Any inflection points or notable quarter-over-quarter changes

Use specific numbers and percentages from the data."""
