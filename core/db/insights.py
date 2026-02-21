"""Insights CRUD — insert, search, count, and list evidence items."""

from __future__ import annotations

from typing import Optional

from psycopg2.extras import RealDictCursor

from core.db.connection import get_connection


def insert_insight(
    source_id: int,
    source_name: str,
    source_url: str,
    insight: str,
    threat_level: str,
    recommendation: str,
    evidence_ref: str,
    content_hash: str,
    confidence: float,
    critic_status: str,
) -> None:
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO insights (
                    source_id,
                    source_name,
                    source_url,
                    insight,
                    threat_level,
                    recommendation,
                    evidence_ref,
                    content_hash,
                    confidence,
                    critic_status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    source_id,
                    source_name,
                    source_url,
                    insight,
                    threat_level,
                    recommendation,
                    evidence_ref,
                    content_hash,
                    confidence,
                    critic_status,
                ),
            )
            cursor.execute(
                """
                INSERT INTO audit_events (event_type, entity_type, entity_id, detail)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    "insight_created",
                    "source",
                    str(source_id),
                    f"threat={threat_level},confidence={confidence},critic_status={critic_status}",
                ),
            )
        connection.commit()


def count_insights(source_id: Optional[int] = None) -> int:
    with get_connection() as connection:
        with connection.cursor() as cursor:
            if source_id is not None:
                cursor.execute("SELECT COUNT(*) FROM insights WHERE source_id = %s", (source_id,))
            else:
                cursor.execute("SELECT COUNT(*) FROM insights")
            row = cursor.fetchone()
            return int(row[0] if row else 0)


def get_latest_insights(
    limit: int = 20,
    source_id: Optional[int] = None,
    offset: int = 0,
) -> list[dict]:
    with get_connection() as connection:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            if source_id is not None:
                cursor.execute(
                    """
                    SELECT source_id, source_name, source_url, insight, threat_level,
                              recommendation, evidence_ref, content_hash,
                              confidence, critic_status, created_at
                    FROM insights
                    WHERE source_id = %s
                    ORDER BY created_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    (source_id, limit, offset),
                )
            else:
                cursor.execute(
                    """
                    SELECT source_id, source_name, source_url, insight, threat_level,
                              recommendation, evidence_ref, content_hash,
                              confidence, critic_status, created_at
                    FROM insights
                    ORDER BY created_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    (limit, offset),
                )
            return [dict(row) for row in cursor.fetchall()]


def search_insights_by_query(query_text: str, limit: int = 50) -> list[dict]:
    with get_connection() as connection:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT source_id, source_name, source_url, insight, threat_level,
                       recommendation, evidence_ref, content_hash,
                       confidence, critic_status, created_at,
                       ts_rank_cd(
                           to_tsvector('english', coalesce(insight, '') || ' ' || coalesce(recommendation, '')),
                           plainto_tsquery('english', %s)
                       ) AS text_rank
                FROM insights
                WHERE to_tsvector('english', coalesce(insight, '') || ' ' || coalesce(recommendation, ''))
                      @@ plainto_tsquery('english', %s)
                ORDER BY text_rank DESC, created_at DESC
                LIMIT %s
                """,
                (query_text, query_text, limit),
            )
            return [dict(row) for row in cursor.fetchall()]
