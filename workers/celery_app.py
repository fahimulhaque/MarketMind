"""Celery application factory — configures the broker, backend, and task autodiscovery."""
from celery import Celery

from core.config import get_settings

settings = get_settings()

celery_app = Celery(
    "marketmind",
    broker=settings.redis_url,
    backend=settings.redis_url,
    include=[
        "workers.tasks_ingest",
        "workers.tasks_report",
        "workers.tasks_agent",
        "workers.tasks_compliance",
    ],
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
)
