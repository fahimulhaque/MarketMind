"""Background agent tasks — source-priority ingestion triggered by intelligence queries."""
from workers.celery_app import celery_app
from core import db
from workers.tasks_ingest import run_ingest


@celery_app.task(name="workers.tasks_agent.run_all_sources")
def run_all_sources(limit: int = 100) -> dict:
    db.init_db()
    sources = db.list_sources(limit=limit, offset=0)

    triggered = []
    for source in sources:
        task = run_ingest.delay(source["id"])
        triggered.append({"source_id": source["id"], "task_id": task.id})

    return {
        "triggered_count": len(triggered),
        "tasks": triggered,
    }


def _score_source_priority(source: dict, hot_queries: list[str], last_ingest) -> float:
    score = 0.0
    name = (source.get("name") or "").lower()
    url = (source.get("url") or "").lower()

    for query in hot_queries:
        tokens = [token for token in query.lower().split() if len(token) > 2]
        if any(token in name or token in url for token in tokens):
            score += 2.0

    if last_ingest is None:
        score += 2.0
    else:
        from datetime import datetime, timezone

        delta_hours = (datetime.now(timezone.utc) - last_ingest).total_seconds() / 3600.0
        score += min(delta_hours / 24.0, 3.0)

    return score


@celery_app.task(name="workers.tasks_agent.run_priority_ingestion")
def run_priority_ingestion(query_text: str, limit: int = 100) -> dict:
    db.init_db()
    sources = db.list_sources(limit=limit, offset=0)
    history = db.get_search_history(limit=30, offset=0)

    hot_queries = [query_text]
    hot_queries.extend(item.get("query_text", "") for item in history[:10])

    ranked = []
    for source in sources:
        last_ingest = db.get_last_ingest_time(source["id"])
        priority = _score_source_priority(source, hot_queries, last_ingest)
        ranked.append((priority, source))

    ranked.sort(key=lambda item: item[0], reverse=True)

    triggered = []
    for priority, source in ranked[: min(25, len(ranked))]:
        task = run_ingest.delay(source["id"])
        triggered.append(
            {
                "source_id": source["id"],
                "source_name": source["name"],
                "priority": round(priority, 3),
                "task_id": task.id,
            }
        )

    return {
        "query_text": query_text,
        "triggered_count": len(triggered),
        "tasks": triggered,
    }
