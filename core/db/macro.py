"""Macro economic indicators CRUD — upsert and query FRED series data."""

from __future__ import annotations

from psycopg2.extras import RealDictCursor

from core.db.connection import get_connection


def upsert_macro_indicator(
    *, series_id: str, series_name: str, observation_date: str, value: float, source_provider: str = "fred"
) -> dict:
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO macro_indicators (series_id, series_name, observation_date, value, source_provider)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (series_id, observation_date) DO UPDATE SET
                    value = EXCLUDED.value,
                    series_name = EXCLUDED.series_name
                RETURNING *;
                """,
                (series_id, series_name, observation_date, value, source_provider),
            )
            row = cur.fetchone()
        conn.commit()
    return dict(row) if row else {}


def get_macro_series(series_id: str, limit: int = 100) -> list[dict]:
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT * FROM macro_indicators
                WHERE series_id = %s
                ORDER BY observation_date DESC
                LIMIT %s
                """,
                (series_id, limit),
            )
            return [dict(r) for r in cur.fetchall()]


def get_latest_macro_values(series_ids: list[str]) -> dict:
    """Return {series_id: {value, date}} for the most recent observation per series."""
    if not series_ids:
        return {}
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (series_id) series_id, series_name, observation_date, value
                FROM macro_indicators
                WHERE series_id = ANY(%s)
                ORDER BY series_id, observation_date DESC
                """,
                (series_ids,),
            )
            rows = cur.fetchall()
    return {
        r["series_id"]: {
            "name": r["series_name"],
            "value": float(r["value"]) if r["value"] is not None else None,
            "date": str(r["observation_date"]),
        }
        for r in rows
    }
