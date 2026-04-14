"""Celery application instance for MLHandler.

Broker and result backend both use a local Redis instance.
The task module is registered via `include` so auto-discovery is not needed.
"""
from celery import Celery
from app.core.config import REDIS_URL

celery_app = Celery(
    "mlhandler",
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=["app.tasks"],
)

celery_app.conf.update(
    task_track_started=True,
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    result_expires=3600,
    broker_connection_retry_on_startup=True,
)
