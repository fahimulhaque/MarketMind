"""Compliance tasks — GDPR source deletion and data-retention purges."""
from core import db
from core.memory import delete_source_memory
from workers.celery_app import celery_app


@celery_app.task(name="workers.tasks_compliance.execute_deletion_request")
def execute_deletion_request(request_id: int) -> dict:
    db.init_db()
    request = db.get_deletion_request(request_id)
    if not request:
        return {
            "request_id": request_id,
            "status": "not_found",
        }

    source_id = request["source_id"]
    db.mark_deletion_request(request_id, "running", "deletion_started")

    try:
        if not db.source_exists(source_id):
            db.mark_deletion_request(request_id, "failed", "source_not_found_or_already_deleted")
            return {
                "request_id": request_id,
                "source_id": source_id,
                "status": "failed",
                "detail": "source_not_found_or_already_deleted",
            }

        memory_result = delete_source_memory(source_id)
        delete_result = db.delete_source_records(source_id)

        detail = (
            f"db={delete_result};memory={memory_result}"
        )
        db.mark_deletion_request(request_id, "executed", detail)

        return {
            "request_id": request_id,
            "source_id": source_id,
            "status": "executed",
            "db": delete_result,
            "memory": memory_result,
        }
    except Exception as error:
        db.mark_deletion_request(request_id, "failed", f"error={error}")
        raise


@celery_app.task(name="workers.tasks_compliance.run_retention")
def run_retention() -> dict:
    db.init_db()
    result = db.run_retention_purge()
    return {
        "status": "succeeded",
        "result": result,
    }
