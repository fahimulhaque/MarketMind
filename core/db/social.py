"""Social signals CRUD — upsert and query Reddit/social sentiment data."""

from __future__ import annotations

import json as _json

from psycopg2.extras import RealDictCursor

from core.db.connection import get_connection


def upsert_social_signal(
    *,
    entity_id: int | None,
    ticker: str,
    platform: str,
    signal_date: str,
    mention_count: int,
    avg_sentiment: float,
    top_posts: list[dict] | None = None,
    source_provider: str = "reddit",
) -> dict:
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO social_signals
                    (entity_id, ticker, platform, signal_date, mention_count,
                     avg_sentiment, top_posts, source_provider)
                VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                ON CONFLICT (ticker, platform, signal_date) DO UPDATE SET
                    mention_count = EXCLUDED.mention_count,
                    avg_sentiment = EXCLUDED.avg_sentiment,
                    top_posts = EXCLUDED.top_posts
                RETURNING *;
                """,
                (entity_id, ticker, platform, signal_date, mention_count,
                 avg_sentiment, _json.dumps(top_posts or []), source_provider),
            )
            row = cur.fetchone()
        conn.commit()
    return dict(row) if row else {}


def get_social_signals(ticker: str, days: int = 7) -> list[dict]:
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT * FROM social_signals
                WHERE ticker = %s AND signal_date >= CURRENT_DATE - %s
                ORDER BY signal_date DESC
                """,
                (ticker, days),
            )
            return [dict(r) for r in cur.fetchall()]
