"""Reports CRUD — insert, list, get, log runs, and observability metrics."""

from __future__ import annotations

from typing import Optional

from psycopg2.extras import RealDictCursor

from core.db.connection import get_connection


def insert_report(source_id: int, title: str, content_markdown: str) -> int:
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO reports (source_id, title, content_markdown)
                VALUES (%s, %s, %s)
                RETURNING id
                """,
                (source_id, title, content_markdown),
            )
            report_id = int(cursor.fetchone()[0])
            cursor.execute(
                """
                INSERT INTO audit_events (event_type, entity_type, entity_id, detail)
                VALUES (%s, %s, %s, %s)
                """,
                ("report_created", "report", str(report_id), f"source_id={source_id}"),
            )
        connection.commit()
    return report_id


def list_reports(limit: int = 20, offset: int = 0, source_id: Optional[int] = None) -> list[dict]:
    with get_connection() as connection:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            if source_id is not None:
                cursor.execute(
                    """
                    SELECT id, source_id, title, content_markdown, created_at
                    FROM reports
                    WHERE source_id = %s
                    ORDER BY created_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    (source_id, limit, offset),
                )
            else:
                cursor.execute(
                    """
                    SELECT id, source_id, title, content_markdown, created_at
                    FROM reports
                    ORDER BY created_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    (limit, offset),
                )
            return [dict(row) for row in cursor.fetchall()]


def get_report(report_id: int) -> Optional[dict]:
    with get_connection() as connection:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT id, source_id, title, content_markdown, created_at
                FROM reports
                WHERE id = %s
                """,
                (report_id,),
            )
            row = cursor.fetchone()
            return dict(row) if row else None


def log_report_run(
    source_id: Optional[int],
    report_id: Optional[int],
    status: str,
    duration_ms: Optional[int],
    detail: str,
) -> None:
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO report_runs (source_id, report_id, status, duration_ms, detail)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (source_id, report_id, status, duration_ms, detail),
            )
            cursor.execute(
                """
                INSERT INTO audit_events (event_type, entity_type, entity_id, detail)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    "report_run",
                    "source",
                    str(source_id) if source_id else None,
                    f"status={status};duration_ms={duration_ms}",
                ),
            )
        connection.commit()


def get_observability_metrics() -> dict:
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM reports")
            reports_count = int(cursor.fetchone()[0])

            cursor.execute("SELECT COUNT(*) FROM failed_ingestions")
            failed_ingestions = int(cursor.fetchone()[0])

            cursor.execute("SELECT COUNT(*) FROM report_runs WHERE status = 'succeeded'")
            report_success = int(cursor.fetchone()[0])

            cursor.execute("SELECT COUNT(*) FROM report_runs WHERE status = 'failed'")
            report_failed = int(cursor.fetchone()[0])

            cursor.execute("SELECT COUNT(*) FROM search_queries")
            search_queries_total = int(cursor.fetchone()[0])

            cursor.execute(
                """
                SELECT COALESCE(AVG(duration_ms), 0)
                FROM report_runs
                WHERE status = 'succeeded' AND duration_ms IS NOT NULL
                """
            )
            avg_duration = float(cursor.fetchone()[0])

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM search_queries sq
                WHERE (
                    SELECT COUNT(*)
                    FROM search_evidence se
                    WHERE se.search_query_id = sq.id
                ) >= 3
                """
            )
            searches_with_three_plus_citations = int(cursor.fetchone()[0])

            cursor.execute(
                """
                SELECT COALESCE(AVG(evidence_count), 0)
                FROM (
                    SELECT COUNT(*)::float AS evidence_count
                    FROM search_evidence
                    GROUP BY search_query_id
                ) x
                """
            )
            avg_evidence_per_answer = float(cursor.fetchone()[0])

            cursor.execute(
                """
                SELECT COALESCE(AVG(CASE WHEN recency_score < 0.4 THEN 1.0 ELSE 0.0 END), 0)
                FROM search_evidence
                """
            )
            stale_evidence_ratio = float(cursor.fetchone()[0])

            cursor.execute(
                """
                SELECT COALESCE(AVG(CASE WHEN confidence < 0.5 THEN 1.0 ELSE 0.0 END), 0)
                FROM search_queries
                """
            )
            low_confidence_answer_ratio = float(cursor.fetchone()[0])

            citation_coverage_rate = (
                searches_with_three_plus_citations / search_queries_total
                if search_queries_total
                else 0.0
            )

            return {
                "reports_total": reports_count,
                "failed_ingestions_total": failed_ingestions,
                "report_generation_success_total": report_success,
                "report_generation_failure_total": report_failed,
                "report_generation_avg_duration_ms": avg_duration,
                "search_queries_total": search_queries_total,
                "search_quality": {
                    "citation_coverage_rate": citation_coverage_rate,
                    "avg_evidence_per_answer": avg_evidence_per_answer,
                    "stale_evidence_ratio": stale_evidence_ratio,
                    "low_confidence_answer_ratio": low_confidence_answer_ratio,
                    "contradiction_count": 0,
                },
                "frontend_error_rate_5m": None,
            }
