from celery import Celery

from .config import settings

celery_app = Celery(
    "llm_obs_worker",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
    include=["app.tasks"],
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=4,
)
