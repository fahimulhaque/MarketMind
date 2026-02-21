"""Data retention and deletion — purge old records, manage deletion requests."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from psycopg2.extras import RealDictCursor

from core.config import get_settings
from core.db.connection import get_connection


def list_retention_runs(limit: int = 50, offset: int = 0) -> list[dict]:
    with get_connection() as connection:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT id, status, detail, created_at
                FROM retention_runs
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
                """,
                (limit, offset),
            )
            return [dict(row) for row in cursor.fetchall()]


def create_deletion_request(source_id: int, reason: str, requested_by: str | None) -> dict:
    with get_connection() as connection:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                INSERT INTO deletion_requests (source_id, reason, requested_by)
                VALUES (%s, %s, %s)
                RETURNING id, source_id, reason, requested_by, status, detail, created_at, executed_at
                """,
                (source_id, reason, requested_by),
            )
            request_row = dict(cursor.fetchone())
            cursor.execute(
                """
                INSERT INTO audit_events (event_type, entity_type, entity_id, detail)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    "deletion_requested",
                    "source",
                    str(source_id),
                    f"request_id={request_row['id']};reason={reason[:120]}",
                ),
            )
        connection.commit()
    return request_row


def list_deletion_requests(limit: int = 50, offset: int = 0, status: str | None = None) -> list[dict]:
    with get_connection() as connection:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            if status:
                cursor.execute(
                    """
                    SELECT id, source_id, reason, requested_by, status, detail, created_at, executed_at
                    FROM deletion_requests
                    WHERE status = %s
                    ORDER BY created_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    (status, limit, offset),
                )
            else:
                cursor.execute(
                    """
                    SELECT id, source_id, reason, requested_by, status, detail, created_at, executed_at
                    FROM deletion_requests
                    ORDER BY created_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    (limit, offset),
                )
            return [dict(row) for row in cursor.fetchall()]


def get_deletion_request(request_id: int) -> dict | None:
    with get_connection() as connection:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT id, source_id, reason, requested_by, status, detail, created_at, executed_at
                FROM deletion_requests
                WHERE id = %s
                """,
                (request_id,),
            )
            row = cursor.fetchone()
            return dict(row) if row else None


def mark_deletion_request(request_id: int, status: str, detail: str) -> None:
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE deletion_requests
                SET status = %s,
                    detail = %s,
                    executed_at = CASE WHEN %s IN ('executed', 'failed') THEN NOW() ELSE executed_at END
                WHERE id = %s
                """,
                (status, detail, status, request_id),
            )
            cursor.execute(
                """
                INSERT INTO audit_events (event_type, entity_type, entity_id, detail)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    "deletion_request_status",
                    "deletion_request",
                    str(request_id),
                    f"status={status};detail={detail[:200]}",
                ),
            )
        connection.commit()


def delete_source_records(source_id: int) -> dict:
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("DELETE FROM search_evidence WHERE source_id = %s", (source_id,))
            search_evidence_deleted = cursor.rowcount

            cursor.execute("DELETE FROM reports WHERE source_id = %s", (source_id,))
            reports_deleted = cursor.rowcount

            cursor.execute("DELETE FROM insights WHERE source_id = %s", (source_id,))
            insights_deleted = cursor.rowcount

            cursor.execute("DELETE FROM source_snapshots WHERE source_id = %s", (source_id,))
            snapshots_deleted = cursor.rowcount

            cursor.execute("DELETE FROM ingest_runs WHERE source_id = %s", (source_id,))
            ingest_runs_deleted = cursor.rowcount

            cursor.execute("DELETE FROM failed_ingestions WHERE source_id = %s", (source_id,))
            failed_ingestions_deleted = cursor.rowcount

            cursor.execute(
                """
                UPDATE sources
                SET name = %s,
                    deleted_at = NOW()
                WHERE id = %s AND deleted_at IS NULL
                """,
                (f"[deleted-source-{source_id}]", source_id),
            )
            source_soft_deleted = cursor.rowcount

            cursor.execute(
                """
                INSERT INTO audit_events (event_type, entity_type, entity_id, detail)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    "source_deleted",
                    "source",
                    str(source_id),
                    (
                        f"source_soft_deleted={source_soft_deleted};snapshots={snapshots_deleted};insights={insights_deleted};"
                        f"reports={reports_deleted};search_evidence={search_evidence_deleted};"
                        f"ingest_runs={ingest_runs_deleted};failed_ingestions={failed_ingestions_deleted}"
                    ),
                ),
            )
        connection.commit()

    return {
        "source_soft_deleted": source_soft_deleted,
        "snapshots_deleted": snapshots_deleted,
        "insights_deleted": insights_deleted,
        "reports_deleted": reports_deleted,
        "search_evidence_deleted": search_evidence_deleted,
        "ingest_runs_deleted": ingest_runs_deleted,
        "failed_ingestions_deleted": failed_ingestions_deleted,
    }


def run_retention_purge() -> dict:
    settings = get_settings()
    now = datetime.now(timezone.utc)

    cutoff_insights = now - timedelta(days=settings.retention_insights_days)
    cutoff_snapshots = now - timedelta(days=settings.retention_snapshots_days)
    cutoff_reports = now - timedelta(days=settings.retention_reports_days)
    cutoff_search = now - timedelta(days=settings.retention_search_days)
    cutoff_audit = now - timedelta(days=settings.retention_audit_days)

    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("DELETE FROM insights WHERE created_at < %s", (cutoff_insights,))
            insights_deleted = cursor.rowcount

            cursor.execute("DELETE FROM source_snapshots WHERE observed_at < %s", (cutoff_snapshots,))
            snapshots_deleted = cursor.rowcount

            cursor.execute("DELETE FROM reports WHERE created_at < %s", (cutoff_reports,))
            reports_deleted = cursor.rowcount

            cursor.execute(
                """
                DELETE FROM search_queries
                WHERE created_at < %s
                """,
                (cutoff_search,),
            )
            search_queries_deleted = cursor.rowcount

            cursor.execute("DELETE FROM audit_events WHERE created_at < %s", (cutoff_audit,))
            audit_deleted = cursor.rowcount

            detail = (
                f"insights={insights_deleted};snapshots={snapshots_deleted};reports={reports_deleted};"
                f"search_queries={search_queries_deleted};audit={audit_deleted}"
            )
            cursor.execute(
                """
                INSERT INTO retention_runs (status, detail)
                VALUES (%s, %s)
                """,
                ("succeeded", detail),
            )
            cursor.execute(
                """
                INSERT INTO audit_events (event_type, entity_type, entity_id, detail)
                VALUES (%s, %s, %s, %s)
                """,
                ("retention_run", "retention", None, detail),
            )
        connection.commit()

    return {
        "insights_deleted": insights_deleted,
        "snapshots_deleted": snapshots_deleted,
        "reports_deleted": reports_deleted,
        "search_queries_deleted": search_queries_deleted,
        "audit_deleted": audit_deleted,
    }
