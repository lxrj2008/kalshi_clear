"""APScheduler wiring for KalshiClear jobs."""

from __future__ import annotations

from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_MISSED
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from config import KalshiSettings
from utils.notifications import send_throttled_email


def job_event_listener(event, logger, settings: KalshiSettings) -> None:
    """Handle APScheduler job error/missed events with logging and throttled email."""
    event_type = "error" if event.code == EVENT_JOB_ERROR else "missed"
    subject = f"[KalshiClear] Scheduler {event_type}: {event.job_id}"
    body = (
        f"Job: {event.job_id}\n"
        f"Type: {event_type}\n"
        f"Scheduled: {getattr(event, 'scheduled_run_time', None)}\n"
        f"Exception: {getattr(event, 'exception', None)}\n"
        f"Traceback: {getattr(event, 'traceback', '')}\n"
    )
    logger.error("Scheduler %s for job %s", event_type, event.job_id)
    key = f"scheduler:{event.job_id}:{event_type}"
    send_throttled_email(
        key=key,
        subject=subject,
        body=body,
        logger=logger,
        settings=settings,
        min_interval_seconds=300.0,
    )


def build_scheduler(*, logger, settings: KalshiSettings) -> BackgroundScheduler:
    scheduler = BackgroundScheduler(job_defaults={"max_instances": 1, "coalesce": True})
    scheduler.add_listener(
        lambda event: job_event_listener(event, logger, settings),
        EVENT_JOB_ERROR | EVENT_JOB_MISSED,
    )
    return scheduler


def add_default_jobs(
    *,
    scheduler: BackgroundScheduler,
    run_tags_filters,
    run_series,
    run_events,
    run_markets,
) -> None:
    scheduler.add_job(
        run_tags_filters,
        CronTrigger(minute=0),
        id="tags_filters_job",
        replace_existing=True,
    )
    scheduler.add_job(
        run_series,
        CronTrigger(minute=5),
        id="series_job",
        replace_existing=True,
    )
    scheduler.add_job(
        run_events,
        CronTrigger(minute=10),
        id="events_job",
        replace_existing=True,
    )
    scheduler.add_job(
        run_markets,
        CronTrigger(minute=15),
        kwargs={"use_created_filter": True},
        id="markets_job",
        replace_existing=True,
    )


__all__ = ["build_scheduler", "add_default_jobs", "job_event_listener"]

