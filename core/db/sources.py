"""Sources CRUD — add, get, list, snapshot, and ingest tracking."""

from __future__ import annotations

from typing import Optional

from psycopg2.extras import RealDictCursor

from core.db.connection import get_connection


def add_source(name: str, url: str, connector_type: str = "web") -> dict:
    with get_connection() as connection:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                INSERT INTO sources (name, url, connector_type)
                VALUES (%s, %s, %s)
                ON CONFLICT (url) DO UPDATE
                SET name = EXCLUDED.name,
                    connector_type = EXCLUDED.connector_type,
                    deleted_at = NULL
                RETURNING id, name, url, connector_type;
                """,
                (name, url, connector_type),
            )
            source = dict(cursor.fetchone())
            cursor.execute(
                """
                INSERT INTO audit_events (event_type, entity_type, entity_id, detail)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    "source_upserted",
                    "source",
                    str(source["id"]),
                    f"name={name},connector_type={connector_type}",
                ),
            )
        connection.commit()
    return source


def get_source(source_id: int) -> Optional[dict]:
    with get_connection() as connection:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                "SELECT id, name, url, connector_type FROM sources WHERE id = %s AND deleted_at IS NULL",
                (source_id,),
            )
            row = cursor.fetchone()
            return dict(row) if row else None


def list_sources(limit: int = 100, offset: int = 0) -> list[dict]:
    with get_connection() as connection:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT id, name, url, connector_type, created_at
                FROM sources
                WHERE deleted_at IS NULL
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
                """,
                (limit, offset),
            )
            return [dict(row) for row in cursor.fetchall()]


def get_latest_snapshot_hash(source_id: int) -> Optional[str]:
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT content_hash
                FROM source_snapshots
                WHERE source_id = %s
                ORDER BY observed_at DESC
                LIMIT 1;
                """,
                (source_id,),
            )
            row = cursor.fetchone()
            return row[0] if row else None


def get_last_ingest_time(source_id: int):
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT created_at
                FROM ingest_runs
                WHERE source_id = %s AND status IN ('succeeded', 'skipped')
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (source_id,),
            )
            row = cursor.fetchone()
            return row[0] if row else None


def log_ingest_run(source_id: int, status: str, detail: str) -> None:
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO ingest_runs (source_id, status, detail)
                VALUES (%s, %s, %s)
                """,
                (source_id, status, detail),
            )
            cursor.execute(
                """
                INSERT INTO audit_events (event_type, entity_type, entity_id, detail)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    "ingest_run",
                    "source",
                    str(source_id),
                    f"status={status};detail={detail}",
                ),
            )
        connection.commit()


def log_failed_ingestion(
    source_id: int | None,
    source_url: str | None,
    error_type: str,
    error_message: str,
    retryable: bool,
) -> None:
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO failed_ingestions (source_id, source_url, error_type, error_message, retryable)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (source_id, source_url, error_type, error_message[:2000], retryable),
            )
            cursor.execute(
                """
                INSERT INTO audit_events (event_type, entity_type, entity_id, detail)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    "ingest_failed",
                    "source",
                    str(source_id) if source_id else None,
                    f"type={error_type};retryable={retryable}",
                ),
            )
        connection.commit()


def insert_snapshot(source_id: int, content_hash: str, content_excerpt: str) -> None:
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO source_snapshots (source_id, content_hash, content_excerpt)
                VALUES (%s, %s, %s)
                """,
                (source_id, content_hash, content_excerpt),
            )
        connection.commit()


def source_exists(source_id: int) -> bool:
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT 1 FROM sources WHERE id = %s AND deleted_at IS NULL
                """,
                (source_id,),
            )
            return cursor.fetchone() is not None
