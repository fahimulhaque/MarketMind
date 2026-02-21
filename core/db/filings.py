"""Entity filings CRUD — upsert and query SEC filings data."""

from __future__ import annotations

from psycopg2.extras import RealDictCursor

from core.db.connection import get_connection


def upsert_entity_filing(
    *,
    entity_id: int | None,
    ticker: str,
    cik: str = "",
    filing_type: str,
    filing_date: str,
    accession_number: str,
    filing_url: str = "",
    description: str = "",
    source_provider: str = "sec_edgar",
) -> dict:
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO entity_filings
                    (entity_id, ticker, cik, filing_type, filing_date,
                     accession_number, filing_url, description, source_provider)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (accession_number) DO UPDATE SET
                    filing_url = EXCLUDED.filing_url,
                    description = EXCLUDED.description
                RETURNING *;
                """,
                (entity_id, ticker, cik, filing_type, filing_date,
                 accession_number, filing_url, description, source_provider),
            )
            row = cur.fetchone()
        conn.commit()
    return dict(row) if row else {}


def get_entity_filings(ticker: str, filing_type: str | None = None, limit: int = 20) -> list[dict]:
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if filing_type:
                cur.execute(
                    """
                    SELECT * FROM entity_filings
                    WHERE ticker = %s AND filing_type = %s
                    ORDER BY filing_date DESC LIMIT %s
                    """,
                    (ticker, filing_type, limit),
                )
            else:
                cur.execute(
                    """
                    SELECT * FROM entity_filings
                    WHERE ticker = %s
                    ORDER BY filing_date DESC LIMIT %s
                    """,
                    (ticker, limit),
                )
            return [dict(r) for r in cur.fetchall()]
