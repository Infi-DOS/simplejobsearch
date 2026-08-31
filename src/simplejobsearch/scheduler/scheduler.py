from __future__ import annotations

import logging

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from ..config import get_settings
from ..db import apply_migrations
from .tasks import morning_review_reminder_task, nightly_search_task


def build_scheduler() -> BlockingScheduler:
    settings = get_settings()
    scheduler = BlockingScheduler(
        timezone=settings.timezone,
        job_defaults={"coalesce": True, "max_instances": 1, "misfire_grace_time": 3600},
    )
    scheduler.add_job(
        nightly_search_task,
        CronTrigger(
            hour=settings.scheduler.search_hour,
            minute=settings.scheduler.search_minute,
            timezone=settings.timezone,
        ),
        id="daily_discovery",
        replace_existing=True,
    )
    scheduler.add_job(
        morning_review_reminder_task,
        CronTrigger(
            hour=settings.scheduler.reminder_hour,
            minute=settings.scheduler.reminder_minute,
            timezone=settings.timezone,
        ),
        id="review_reminder",
        replace_existing=True,
    )
    return scheduler


def run_scheduler() -> None:
    logging.basicConfig(level=logging.INFO)
    apply_migrations()
    settings = get_settings()
    if not settings.scheduler.enabled:
        logging.getLogger(__name__).warning("Scheduler disabled by SCHEDULER_ENABLED")
        return
    build_scheduler().start()


if __name__ == "__main__":
    run_scheduler()

