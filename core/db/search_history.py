"""Search history CRUD — save results and retrieve search history."""

from __future__ import annotations

from psycopg2.extras import RealDictCursor

from core.db.connection import get_connection


def save_search_result(
    query_text: str,
    answer_summary: str,
    confidence: float,
    risk_level: str,
    recommendation: str,
    evidence_items: list[dict],
) -> int:
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO search_queries (query_text, answer_summary, confidence, risk_level, recommendation)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
                """,
                (query_text, answer_summary, confidence, risk_level, recommendation),
            )
            search_id = int(cursor.fetchone()[0])

            for item in evidence_items:
                cursor.execute(
                    """
                    INSERT INTO search_evidence (
                        search_query_id,
                        source_id,
                        source_name,
                        evidence_ref,
                        insight_excerpt,
                        confidence,
                        recency_score,
                        rank_score
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        search_id,
                        item.get("source_id"),
                        item.get("source_name"),
                        item.get("evidence_ref", ""),
                        item.get("insight", ""),
                        item.get("confidence", 0.0),
                        item.get("recency_score", 0.0),
                        item.get("rank_score", 0.0),
                    ),
                )

            cursor.execute(
                """
                INSERT INTO audit_events (event_type, entity_type, entity_id, detail)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    "search_query",
                    "search",
                    str(search_id),
                    f"query={query_text[:120]}",
                ),
            )
        connection.commit()
    return search_id


def get_search_history(limit: int = 20, offset: int = 0) -> list[dict]:
    with get_connection() as connection:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT id, query_text, answer_summary, confidence, risk_level, recommendation, created_at
                FROM search_queries
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
                """,
                (limit, offset),
            )
            items = [dict(row) for row in cursor.fetchall()]

        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            for item in items:
                cursor.execute(
                    """
                    SELECT source_id, source_name, evidence_ref, insight_excerpt,
                           confidence, recency_score, rank_score
                    FROM search_evidence
                    WHERE search_query_id = %s
                    ORDER BY rank_score DESC NULLS LAST, created_at DESC
                    LIMIT 10
                    """,
                    (item["id"],),
                )
                item["evidence"] = [dict(row) for row in cursor.fetchall()]

    return items
