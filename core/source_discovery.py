"""Source discovery and multi-provider data enrichment.

Orchestrates entity resolution, provider dispatch, and coverage tracking.
Keeps the existing Yahoo Finance snapshot logic for real-time price data
and adds structured provider enrichment from SEC EDGAR, FMP, Alpha Vantage,
FRED, DuckDuckGo, and Reddit.
"""

from __future__ import annotations

import logging
from urllib.parse import quote_plus
from typing import Optional

import httpx

from core.config import get_settings
from core.entities import resolve_entity
from core import db

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Provider registry (lazy imports to avoid startup cost if keys missing)
# ---------------------------------------------------------------------------


def _get_configured_providers() -> list:
    """Return list of BaseProvider instances that are configured."""
    providers = []

    from connectors.providers.sec_edgar import SecEdgarProvider
    sec = SecEdgarProvider()
    if sec.is_configured():
        providers.append(sec)

    from connectors.providers.fmp import FmpProvider
    fmp = FmpProvider()
    if fmp.is_configured():
        providers.append(fmp)

    from connectors.providers.alpha_vantage import AlphaVantageProvider
    av = AlphaVantageProvider()
    if av.is_configured():
        providers.append(av)

    from connectors.providers.fred import FredProvider
    fred = FredProvider()
    if fred.is_configured():
        providers.append(fred)

    from connectors.providers.ddg import DdgProvider
    ddg = DdgProvider()
    if ddg.is_configured():
        providers.append(ddg)

    from connectors.providers.reddit import RedditProvider
    reddit = RedditProvider()
    if reddit.is_configured():
        providers.append(reddit)

    from connectors.providers.finviz import FinvizProvider
    finviz = FinvizProvider()
    if finviz.is_configured():
        providers.append(finviz)

    from connectors.providers.polygon import PolygonProvider
    polygon = PolygonProvider()
    if polygon.is_configured():
        providers.append(polygon)

    from connectors.providers.cboe import CboeProvider
    cboe = CboeProvider()
    if cboe.is_configured():
        providers.append(cboe)

    from connectors.providers.finra import FinraProvider
    finra = FinraProvider()
    if finra.is_configured():
        providers.append(finra)

    return providers


# ---------------------------------------------------------------------------
# Yahoo helpers (kept for real-time price snapshot)
# ---------------------------------------------------------------------------


def _extract_raw(value):
    if isinstance(value, dict):
        if "raw" in value:
            return value.get("raw")
        if "fmt" in value:
            return value.get("fmt")
    return value


def resolve_yahoo_symbol(query_text: str) -> str | None:
    settings = get_settings()
    lookup_queries = [query_text]
    first_token = query_text.split()[0] if query_text.split() else None
    if first_token and first_token.lower() != query_text.lower():
        lookup_queries.append(first_token)

    for lookup in lookup_queries:
        try:
            response = httpx.get(
                "https://query2.finance.yahoo.com/v1/finance/search",
                params={"q": lookup, "quotesCount": 1, "newsCount": 0},
                timeout=15.0,
                headers={"User-Agent": settings.ingest_user_agent},
            )
            response.raise_for_status()
            payload = response.json()
            quotes = payload.get("quotes", [])
            if quotes:
                symbol = quotes[0].get("symbol")
                if isinstance(symbol, str) and symbol:
                    return symbol
        except Exception as exc:
            logger.warning("Ticker lookup failed for '%s': %s", lookup, exc)
            continue
    return None


# ---------------------------------------------------------------------------
# Existing discover_query_sources (RSS/web source generation)
# ---------------------------------------------------------------------------


def discover_query_sources(query_text: str, pre_resolved_ticker: str | None = None) -> list[dict]:
    """Generate ingestible source URLs for a query (Google News + Yahoo)."""
    encoded_query = quote_plus(query_text)
    sources = [
        {
            "name": f"Google News: {query_text}",
            "url": f"https://news.google.com/rss/search?q={encoded_query}",
            "connector_type": "rss",
        }
    ]

    symbol = pre_resolved_ticker or resolve_yahoo_symbol(query_text)
    if symbol:
        sources.extend(
            [
                {
                    "name": f"Yahoo Finance Quote: {symbol}",
                    "url": f"https://finance.yahoo.com/quote/{symbol}",
                    "connector_type": "web",
                },
                {
                    "name": f"Yahoo Finance News: {symbol}",
                    "url": f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={symbol}&region=US&lang=en-US",
                    "connector_type": "rss",
                },
            ]
        )

    return sources


# ---------------------------------------------------------------------------
# Full enrichment: multi-provider dispatch
# ---------------------------------------------------------------------------


def run_full_enrichment(query_text: str, pre_resolved_ticker: str | None = None) -> dict:
    """Run all configured providers for a query, update coverage.

    This is the main entry point for deep data enrichment.  It:
    1. Resolves query → canonical entity
    2. Discovers RSS/web sources (existing)
    3. Dispatches to all configured structured providers
    4. Updates entity coverage score

    Returns a summary dict with provider results.
    """
    summary = {
        "entity": None,
        "providers_run": [],
        "total_records": 0,
        "rss_sources_discovered": 0,
        "coverage": None,
    }

    # 1. Entity resolution
    entity = resolve_entity(query_text, pre_resolved_ticker)
    if not entity:
        logger.warning("Could not resolve entity for %r — falling back to RSS only", query_text)
        # Still do RSS discovery
        rss_sources = discover_query_sources(query_text)
        summary["rss_sources_discovered"] = len(rss_sources)
        return summary

    summary["entity"] = {
        "id": entity.get("id"),
        "name": entity.get("name"),
        "ticker": entity.get("ticker"),
        "cik": entity.get("cik"),
        "sector": entity.get("sector"),
    }

    # 2. RSS/web sources
    rss_sources = discover_query_sources(query_text, pre_resolved_ticker)
    summary["rss_sources_discovered"] = len(rss_sources)

    # 3. Dispatch to all structured providers
    providers = _get_configured_providers()
    for provider in providers:
        try:
            if not provider.rate_limit_ok():
                logger.info("Skipping %s — rate limit reached", provider.provider_name)
                continue
            results = provider.fetch_company_data(entity)
            for r in results:
                summary["providers_run"].append({
                    "provider": r.provider,
                    "data_type": r.data_type,
                    "records_stored": r.records_stored,
                    "success": r.success,
                    "error": r.error,
                })
                summary["total_records"] += r.records_stored
        except Exception as exc:
            logger.error("Provider %s failed for %s: %s", provider.provider_name, entity.get("ticker"), exc)
            summary["providers_run"].append({
                "provider": provider.provider_name,
                "data_type": "all",
                "records_stored": 0,
                "success": False,
                "error": str(exc),
            })

    # 4. Update entity coverage
    try:
        coverage = db.update_entity_coverage(entity["id"], entity["ticker"])
        summary["coverage"] = {
            "score": float(coverage.get("coverage_score", 0)),
            "has_financials": coverage.get("has_financials", False),
            "financials_quarters": coverage.get("financials_quarters", 0),
            "has_filings": coverage.get("has_filings", False),
            "has_macro": coverage.get("has_macro", False),
            "has_social": coverage.get("has_social", False),
            "has_news": coverage.get("has_news", False),
            "has_price": coverage.get("has_price", False),
        }
    except Exception as exc:
        logger.warning("Coverage update failed: %s", exc)

    logger.info(
        "Enrichment complete for %s: %d providers, %d records, coverage=%.2f",
        entity.get("ticker"),
        len(summary["providers_run"]),
        summary["total_records"],
        (summary.get("coverage") or {}).get("score", 0),
    )
    return summary


# ---------------------------------------------------------------------------
# FMP fallback enrichment for financial snapshot gaps
# ---------------------------------------------------------------------------


def _fmp_enrich_snapshot(snapshot: dict) -> dict:
    """Patch None fields in a yfinance snapshot using FMP profile + ratios-ttm.

    Only called when FMP_API_KEY is configured. Mutates and returns the snapshot.
    """
    symbol = snapshot.get("symbol")
    if not symbol:
        return snapshot

    fill_fields = [
        "market_cap", "revenue_growth", "earnings_growth", "gross_margin",
        "operating_margin", "profit_margin", "peg_ratio", "beta",
        "trailing_pe", "debt_to_equity", "current_ratio", "dividend_yield",
    ]

    # Quick check: any None fields to fill?
    needs_fill = any(snapshot.get(f) is None for f in fill_fields)
    if not needs_fill:
        return snapshot

    try:
        from connectors.providers.fmp import FmpProvider
        fmp = FmpProvider()
        if not fmp.is_configured() or not fmp.rate_limit_ok():
            return snapshot

        filled_any = False

        # Try profile endpoint first (market_cap, beta, sector, etc.)
        profile = fmp.fetch_profile(symbol)
        if profile:
            for key in ["market_cap", "beta", "sector", "industry", "dividend_yield", "avg_volume", "employees"]:
                if snapshot.get(key) is None and profile.get(key) is not None:
                    snapshot[key] = profile[key]
                    filled_any = True

        # Try ratios-ttm endpoint (margins, PE, PEG, growth)
        ratios = fmp.fetch_ratios_ttm(symbol)
        if ratios:
            for key in ["trailing_pe", "peg_ratio", "gross_margin", "operating_margin",
                        "profit_margin", "revenue_growth", "earnings_growth",
                        "debt_to_equity", "current_ratio", "dividend_yield"]:
                if snapshot.get(key) is None and ratios.get(key) is not None:
                    snapshot[key] = ratios[key]
                    filled_any = True

        if filled_any:
            snapshot["source"] = snapshot.get("source", "yfinance") + "+fmp"
            logger.info("FMP enriched snapshot for %s — filled gaps", symbol)

    except Exception as exc:
        logger.warning("FMP snapshot enrichment failed for %s: %s", symbol, exc)

    return snapshot


# ---------------------------------------------------------------------------
# Real-time financial snapshot (Yahoo Finance — kept for price layer)
# ---------------------------------------------------------------------------


def fetch_financial_snapshot(query_text: str, pre_resolved_ticker: str | None = None) -> dict:
    """Fetch real-time financial data using yfinance (reliable) with httpx fallback."""
    symbol = pre_resolved_ticker or resolve_yahoo_symbol(query_text)
    empty = {
        "symbol": None, "price": None, "currency": None, "market_cap": None,
        "fifty_two_week_range": None, "trailing_pe": None, "revenue_growth": None,
        "earnings_growth": None, "gross_margin": None, "operating_margin": None,
        "profit_margin": None, "debt_to_equity": None, "current_ratio": None,
        "forward_pe": None, "peg_ratio": None, "next_earnings_date": None,
        "sector": None, "industry": None, "beta": None, "dividend_yield": None,
        "fifty_two_week_low": None, "fifty_two_week_high": None,
        "avg_volume": None, "employees": None,
        "source": "yfinance",
    }
    if not symbol:
        return empty

    snapshot = dict(empty)
    snapshot["symbol"] = symbol

    # --- Primary: yfinance (most reliable) ---
    try:
        import yfinance as yf
        ticker = yf.Ticker(symbol)
        info = ticker.info or {}
        if info and info.get("regularMarketPrice") is not None:
            snapshot["price"] = info.get("regularMarketPrice") or info.get("currentPrice")
            snapshot["currency"] = info.get("currency")
            snapshot["market_cap"] = info.get("marketCap")
            snapshot["trailing_pe"] = info.get("trailingPE")
            snapshot["forward_pe"] = info.get("forwardPE")
            snapshot["peg_ratio"] = info.get("pegRatio")
            snapshot["revenue_growth"] = info.get("revenueGrowth")
            snapshot["earnings_growth"] = info.get("earningsGrowth") or info.get("earningsQuarterlyGrowth")
            snapshot["gross_margin"] = info.get("grossMargins")
            snapshot["operating_margin"] = info.get("operatingMargins")
            snapshot["profit_margin"] = info.get("profitMargins")
            raw_de = info.get("debtToEquity")
            snapshot["debt_to_equity"] = raw_de / 100 if raw_de and raw_de > 5 else raw_de
            snapshot["current_ratio"] = info.get("currentRatio")
            snapshot["sector"] = info.get("sector")
            snapshot["industry"] = info.get("industry")
            snapshot["beta"] = info.get("beta")
            snapshot["dividend_yield"] = info.get("dividendYield")
            snapshot["avg_volume"] = info.get("averageVolume")
            snapshot["employees"] = info.get("fullTimeEmployees")

            low_52 = info.get("fiftyTwoWeekLow")
            high_52 = info.get("fiftyTwoWeekHigh")
            snapshot["fifty_two_week_low"] = low_52
            snapshot["fifty_two_week_high"] = high_52
            if low_52 is not None and high_52 is not None:
                snapshot["fifty_two_week_range"] = f"{low_52} - {high_52}"

            # Next earnings date
            try:
                cal = ticker.calendar
                if cal is not None:
                    if isinstance(cal, dict):
                        ed = cal.get("Earnings Date")
                        if isinstance(ed, list) and ed:
                            snapshot["next_earnings_date"] = str(ed[0])
                        elif ed:
                            snapshot["next_earnings_date"] = str(ed)
            except Exception as exc:
                logger.warning("Earnings date extraction failed for %s: %s", symbol, exc)

            logger.info("yfinance snapshot for %s: price=%s, PE=%s, margin=%s",
                        symbol, snapshot["price"], snapshot["trailing_pe"], snapshot["gross_margin"])
            return _fmp_enrich_snapshot(snapshot)
    except Exception as exc:
        logger.warning("yfinance failed for %s: %s — falling back to chart API", symbol, exc)

    # --- Fallback: Yahoo chart API (usually still works for basic price) ---
    settings = get_settings()
    try:
        response = httpx.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
            params={"range": "1mo", "interval": "1d"},
            timeout=15.0,
            headers={"User-Agent": settings.ingest_user_agent},
        )
        response.raise_for_status()
        payload = response.json()
        result = payload.get("chart", {}).get("result", [{}])[0]
        metadata = result.get("meta", {}) if isinstance(result, dict) else {}
        snapshot["price"] = metadata.get("regularMarketPrice")
        snapshot["currency"] = metadata.get("currency")
        snapshot["market_cap"] = metadata.get("marketCap")
        low_52 = metadata.get("fiftyTwoWeekLow")
        high_52 = metadata.get("fiftyTwoWeekHigh")
        snapshot["fifty_two_week_low"] = low_52
        snapshot["fifty_two_week_high"] = high_52
        if low_52 is not None and high_52 is not None:
            snapshot["fifty_two_week_range"] = f"{low_52} - {high_52}"
        snapshot["trailing_pe"] = metadata.get("trailingPE")
        snapshot["source"] = "yahoo_chart_fallback"
    except Exception as exc:
        logger.warning("Chart API fallback also failed for %s: %s", symbol, exc)

    return _fmp_enrich_snapshot(snapshot)
