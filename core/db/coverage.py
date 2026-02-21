"""Entity coverage CRUD — compute and query data coverage scores."""

from __future__ import annotations

from psycopg2.extras import RealDictCursor

from core.db.connection import get_connection


def update_entity_coverage(entity_id: int, ticker: str) -> dict:
    """Recompute and upsert the coverage score for an entity."""
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Count financial quarters
            cur.execute(
                "SELECT COUNT(*) as cnt FROM financial_periods WHERE ticker = %s AND period_type = 'quarterly'",
                (ticker,),
            )
            fin_q = cur.fetchone()["cnt"]

            # Count filings
            cur.execute("SELECT COUNT(*) as cnt FROM entity_filings WHERE ticker = %s", (ticker,))
            filing_cnt = cur.fetchone()["cnt"]

            # Check macro (global, not per-entity)
            cur.execute("SELECT COUNT(*) as cnt FROM macro_indicators WHERE observation_date > CURRENT_DATE - 30")
            macro_cnt = cur.fetchone()["cnt"]

            # Check social
            cur.execute(
                "SELECT COUNT(*) as cnt FROM social_signals WHERE ticker = %s AND signal_date > CURRENT_DATE - 7",
                (ticker,),
            )
            social_cnt = cur.fetchone()["cnt"]

            # Check news (insights from RSS/web)
            cur.execute(
                "SELECT COUNT(*) as cnt FROM insights WHERE source_url ILIKE '%%news%%' AND source_name ILIKE %s",
                (f"%{ticker}%",),
            )
            news_cnt = cur.fetchone()["cnt"]

            # Check price (sources table with Yahoo)
            cur.execute(
                "SELECT COUNT(*) as cnt FROM sources WHERE url ILIKE %s AND url ILIKE '%%yahoo%%'",
                (f"%{ticker}%",),
            )
            price_cnt = cur.fetchone()["cnt"]

            has_fin = fin_q > 0
            has_fil = filing_cnt > 0
            has_mac = macro_cnt > 0
            has_soc = social_cnt > 0
            has_news = news_cnt > 0
            has_price = price_cnt > 0

            # Weighted coverage score (0.0–1.0)
            score = 0.0
            if has_fin:
                score += 0.30 * min(fin_q / 8.0, 1.0)  # 8 quarters = full
            if has_fil:
                score += 0.20 * min(filing_cnt / 5.0, 1.0)
            if has_mac:
                score += 0.15
            if has_soc:
                score += 0.10
            if has_news:
                score += 0.15
            if has_price:
                score += 0.10

            cur.execute(
                """
                INSERT INTO entity_coverage
                    (entity_id, ticker, has_financials, financials_quarters,
                     has_filings, filings_count, has_macro, has_social,
                     has_news, has_price, coverage_score, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (ticker) DO UPDATE SET
                    has_financials = EXCLUDED.has_financials,
                    financials_quarters = EXCLUDED.financials_quarters,
                    has_filings = EXCLUDED.has_filings,
                    filings_count = EXCLUDED.filings_count,
                    has_macro = EXCLUDED.has_macro,
                    has_social = EXCLUDED.has_social,
                    has_news = EXCLUDED.has_news,
                    has_price = EXCLUDED.has_price,
                    coverage_score = EXCLUDED.coverage_score,
                    last_updated = NOW()
                RETURNING *;
                """,
                (entity_id, ticker, has_fin, fin_q, has_fil, filing_cnt,
                 has_mac, has_soc, has_news, has_price, round(score, 4)),
            )
            row = cur.fetchone()
        conn.commit()
    return dict(row) if row else {}


def get_entity_coverage(ticker: str) -> dict | None:
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM entity_coverage WHERE ticker = %s", (ticker,))
            row = cur.fetchone()
    return dict(row) if row else None
