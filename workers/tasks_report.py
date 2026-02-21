"""Report generation — build Markdown intelligence reports per source."""
from datetime import datetime, timezone

from rules.reporter import build_markdown_report
from core import db
from workers.celery_app import celery_app


@celery_app.task(name="workers.tasks_report.generate_report", rate_limit="10/m")
def generate_report(source_id: int) -> dict:
    db.init_db()
    started = datetime.now(timezone.utc)

    source = db.get_source(source_id)
    if not source:
        db.log_report_run(source_id, None, "failed", None, "source_not_found")
        raise ValueError(f"Source {source_id} not found")

    insights = db.get_latest_insights(limit=50, source_id=source_id, offset=0)
    title, markdown = build_markdown_report(
        source_name=source["name"],
        source_url=source["url"],
        insights=insights,
    )

    report_id = db.insert_report(source_id=source_id, title=title, content_markdown=markdown)
    duration_ms = int((datetime.now(timezone.utc) - started).total_seconds() * 1000)
    db.log_report_run(source_id, report_id, "succeeded", duration_ms, "report_generated")

    return {
        "report_id": report_id,
        "source_id": source_id,
        "title": title,
        "duration_ms": duration_ms,
    }
