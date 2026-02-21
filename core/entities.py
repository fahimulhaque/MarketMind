"""Entity normalization layer.

Resolves free-text company queries to canonical entity records
(ticker, CIK, name, sector, industry) and caches results in Postgres.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Optional
import re

import httpx
from psycopg2.extras import RealDictCursor

from core.config import get_settings
from core.db import get_connection

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


def _ensure_entities_table() -> None:
    """Idempotent table creation (called from init_db too)."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS entities (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    ticker TEXT UNIQUE,
                    cik TEXT,
                    sector TEXT,
                    industry TEXT,
                    exchange TEXT,
                    entity_type TEXT NOT NULL DEFAULT 'company',
                    aliases JSONB DEFAULT '[]'::jsonb,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );
                """
            )
        conn.commit()


def _lookup_entity(query: str) -> Optional[dict]:
    """Check local DB for an entity by ticker, name, or alias (case-insensitive)."""
    q_upper = query.strip().upper()
    q_lower = query.strip().lower()
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Try ticker exact match first
            cur.execute(
                "SELECT * FROM entities WHERE UPPER(ticker) = %s LIMIT 1",
                (q_upper,),
            )
            row = cur.fetchone()
            if row:
                return dict(row)
            # Try name ilike
            cur.execute(
                "SELECT * FROM entities WHERE LOWER(name) = %s LIMIT 1",
                (q_lower,),
            )
            row = cur.fetchone()
            if row:
                return dict(row)
            # Try alias containment
            cur.execute(
                "SELECT * FROM entities WHERE aliases @> %s::jsonb LIMIT 1",
                (json.dumps([q_lower]),),
            )
            row = cur.fetchone()
            if row:
                return dict(row)
    return None


def _upsert_entity(
    *,
    name: str,
    ticker: str,
    cik: str = "",
    sector: str = "",
    industry: str = "",
    exchange: str = "",
    entity_type: str = "company",
    aliases: list[str] | None = None,
) -> dict:
    """Insert or update an entity, return the row."""
    alias_json = json.dumps(aliases or [])
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO entities (name, ticker, cik, sector, industry, exchange, entity_type, aliases, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW())
                ON CONFLICT (ticker) DO UPDATE SET
                    name = COALESCE(NULLIF(EXCLUDED.name, ''), entities.name),
                    cik = COALESCE(NULLIF(EXCLUDED.cik, ''), entities.cik),
                    sector = COALESCE(NULLIF(EXCLUDED.sector, ''), entities.sector),
                    industry = COALESCE(NULLIF(EXCLUDED.industry, ''), entities.industry),
                    exchange = COALESCE(NULLIF(EXCLUDED.exchange, ''), entities.exchange),
                    aliases = EXCLUDED.aliases,
                    updated_at = NOW()
                RETURNING *;
                """,
                (name, ticker, cik, sector, industry, exchange, entity_type, alias_json),
            )
            row = cur.fetchone()
        conn.commit()
    return dict(row)


# ---------------------------------------------------------------------------
# Yahoo symbol resolution (existing logic lifted + enriched)
# ---------------------------------------------------------------------------


def _resolve_via_yahoo(query_text: str) -> Optional[dict]:
    """Resolve query to ticker+metadata via Yahoo Finance search API."""
    try:
        query_clean = query_text.strip()
        attempts = [query_clean]
        
        # Prioritize explicit tickers in parentheses, e.g. (TMCV.NS)
        explicit_match = re.search(r'\(([A-Z0-9.-]+)\)', query_clean, re.IGNORECASE)
        if explicit_match:
            attempts.insert(0, explicit_match.group(1).upper())
            
        tokens = query_clean.split()
        if len(tokens) > 1:
            attempts.append(tokens[0])
            
        # Deduplicate attempts while preserving order
        seen = set()
        unique_attempts = []
        for a in attempts:
            if a not in seen:
                seen.add(a)
                unique_attempts.append(a)

        for attempt in unique_attempts:
            resp = httpx.get(
                "https://query2.finance.yahoo.com/v1/finance/search",
                params={"q": attempt, "quotesCount": 3, "newsCount": 0},
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=10,
            )
            if resp.status_code != 200:
                continue
            data = resp.json()
            quotes = data.get("quotes", [])
            if not quotes:
                continue
            # Pick first equity quote; skip crypto/futures
            for q in quotes:
                qtype = q.get("quoteType", "").upper()
                if qtype in ("EQUITY", "ETF"):
                    return {
                        "ticker": q.get("symbol", ""),
                        "name": q.get("shortname") or q.get("longname", ""),
                        "exchange": q.get("exchange", ""),
                        "entity_type": "etf" if qtype == "ETF" else "company",
                    }
            # Fallback: take first regardless
            q = quotes[0]
            return {
                "ticker": q.get("symbol", ""),
                "name": q.get("shortname") or q.get("longname", ""),
                "exchange": q.get("exchange", ""),
                "entity_type": "company",
            }
    except Exception as exc:
        logger.warning("Yahoo symbol resolution failed for %r: %s", query_text, exc)
    return None


# ---------------------------------------------------------------------------
# SEC EDGAR company lookup
# ---------------------------------------------------------------------------


def _resolve_cik_from_sec(ticker: str) -> str:
    """Resolve a ticker to CIK via SEC EDGAR company tickers JSON."""
    settings = get_settings()
    try:
        resp = httpx.get(
            "https://www.sec.gov/files/company_tickers.json",
            headers={"User-Agent": settings.sec_edgar_user_agent},
            timeout=15,
        )
        if resp.status_code != 200:
            return ""
        data = resp.json()
        ticker_upper = ticker.upper()
        for entry in data.values():
            if entry.get("ticker", "").upper() == ticker_upper:
                cik = str(entry.get("cik_str", ""))
                return cik.zfill(10)
    except Exception as exc:
        logger.warning("SEC CIK resolution failed for %r: %s", ticker, exc)
    return ""


# ---------------------------------------------------------------------------
# FMP profile enrichment
# ---------------------------------------------------------------------------


def _enrich_from_fmp(ticker: str) -> dict:
    """Fetch company profile from FMP for sector/industry info."""
    settings = get_settings()
    if not settings.fmp_api_key:
        return {}
    try:
        resp = httpx.get(
            f"https://financialmodelingprep.com/api/v3/profile/{ticker}",
            params={"apikey": settings.fmp_api_key},
            timeout=10,
        )
        if resp.status_code != 200:
            return {}
        data = resp.json()
        if not data:
            return {}
        p = data[0] if isinstance(data, list) else data
        return {
            "sector": p.get("sector", ""),
            "industry": p.get("industry", ""),
            "name": p.get("companyName", ""),
        }
    except Exception as exc:
        logger.warning("FMP profile enrichment failed for %r: %s", ticker, exc)
    return {}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def resolve_entity(query_text: str, pre_resolved_ticker: str | None = None) -> Optional[dict]:
    """Resolve free-text query to a canonical entity record.

    Resolution order:
    1. Local DB cache (by ticker / name / alias)
    2. Yahoo Finance search API
    3. SEC EDGAR CIK lookup
    4. FMP profile enrichment (sector/industry)

    Returns dict with id, name, ticker, cik, sector, industry or None.
    """
    _ensure_entities_table()

    # 1. Check cache
    if pre_resolved_ticker:
        existing = _lookup_entity(pre_resolved_ticker)
        if existing:
            return existing
            
    existing = _lookup_entity(query_text)
    if existing:
        return existing

    # 2. Yahoo resolution
    ticker = pre_resolved_ticker
    yahoo = {}
    if not ticker:
        yahoo = _resolve_via_yahoo(query_text)
        if not yahoo or not yahoo.get("ticker"):
            return None
        ticker = yahoo["ticker"]

    # Check again by resolved ticker (in case query was a name)
    existing = _lookup_entity(ticker)
    if existing:
        return existing

    # 3. SEC CIK
    cik = _resolve_cik_from_sec(ticker)

    # 4. FMP enrichment for sector/industry
    fmp = _enrich_from_fmp(ticker)

    # Build aliases
    aliases = list(
        {
            v.lower()
            for v in [query_text.strip(), yahoo.get("name", ""), ticker]
            if v
        }
    )

    # Map name properly if we skipped yahoo
    final_name = fmp.get("name") or (yahoo.get("name") if yahoo else None) or query_text.strip()
    
    entity = _upsert_entity(
        name=final_name,
        ticker=ticker,
        cik=cik,
        sector=fmp.get("sector", ""),
        industry=fmp.get("industry", ""),
        exchange=yahoo.get("exchange", "") if yahoo else "",
        entity_type=yahoo.get("entity_type", "company") if yahoo else "company",
        aliases=aliases,
    )
    return entity


# ---------------------------------------------------------------------------
# Autocomplete for search input
# ---------------------------------------------------------------------------


def autocomplete_tickers(query: str, limit: int = 6) -> list[dict]:
    """Fast ticker/company autocomplete for the search input.

    Returns list of {ticker, name, exchange, type} dicts.
    1. Check local DB for cached entities
    2. Fall back to Yahoo Finance search API for new tickers
    """
    suggestions: list[dict] = []
    seen_tickers: set[str] = set()

    # 1. Local DB search (instant)
    _ensure_entities_table()
    q_lower = query.strip().lower()
    q_upper = query.strip().upper()
    try:
        with get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT ticker, name, exchange, entity_type
                    FROM entities
                    WHERE UPPER(ticker) LIKE %s
                       OR LOWER(name) LIKE %s
                       OR aliases @> %s::jsonb
                    ORDER BY
                      CASE WHEN UPPER(ticker) = %s THEN 0
                           WHEN UPPER(ticker) LIKE %s THEN 1
                           ELSE 2 END
                    LIMIT %s
                    """,
                    (f"{q_upper}%", f"%{q_lower}%", json.dumps([q_lower]),
                     q_upper, f"{q_upper}%", limit),
                )
                for row in cur.fetchall():
                    t = row["ticker"]
                    if t and t not in seen_tickers:
                        suggestions.append({
                            "ticker": t,
                            "name": row["name"],
                            "exchange": row.get("exchange", ""),
                            "type": row.get("entity_type", "company"),
                        })
                        seen_tickers.add(t)
    except Exception as exc:
        logger.warning("Autocomplete DB lookup failed: %s", exc)

    # 2. Yahoo Finance search API for more results
    if len(suggestions) < limit:
        try:
            resp = httpx.get(
                "https://query2.finance.yahoo.com/v1/finance/search",
                params={"q": query.strip(), "quotesCount": limit, "newsCount": 0},
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=5,
            )
            if resp.status_code == 200:
                data = resp.json()
                for q_item in data.get("quotes", []):
                    qtype = q_item.get("quoteType", "").upper()
                    if qtype not in ("EQUITY", "ETF"):
                        continue
                    t = q_item.get("symbol", "")
                    if t and t not in seen_tickers:
                        suggestions.append({
                            "ticker": t,
                            "name": q_item.get("shortname") or q_item.get("longname", ""),
                            "exchange": q_item.get("exchange", ""),
                            "type": "etf" if qtype == "ETF" else "company",
                        })
                        seen_tickers.add(t)
                        if len(suggestions) >= limit:
                            break
        except Exception as exc:
            logger.warning("Autocomplete Yahoo lookup failed: %s", exc)

    return suggestions[:limit]
