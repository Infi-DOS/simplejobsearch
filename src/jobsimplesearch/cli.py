from __future__ import annotations

import argparse
import json

from .db import apply_migrations, database
from .pipeline.orchestrator import continue_after_review
from .search.collector import run_daily_search
from .workflow import bootstrap_latest_batch


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="jobsimplesearch")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("migrate", help="Apply additive database migrations")
    commands.add_parser("search", help="Run discovery and PRE_DESCRIPTION rules")
    commands.add_parser("continue", help="Continue the latest reviewed daily batch")
    commands.add_parser("run-nightly", help="Run the same nightly task used by APScheduler")
    commands.add_parser(
        "run-review-reminder",
        help="Run the same review-reminder task used by APScheduler",
    )
    email_test = commands.add_parser(
        "email-test",
        help="Send a production email template with synthetic data",
    )
    email_test.add_argument(
        "message_type",
        choices=("search", "review", "complete"),
        help="Email template to send",
    )
    commands.add_parser("scheduler", help="Run the APScheduler service")
    commands.add_parser("web", help="Run the NiceGUI web service")
    commands.add_parser(
        "windows-nightly-worker",
        help="Windows task: search, ensure portal availability, then send email",
    )
    commands.add_parser(
        "windows-review-worker",
        help="Windows task: ensure portal availability, then send the reminder",
    )
    commands.add_parser(
        "windows-pipeline-worker",
        help="Windows task: continue pipeline, restore portal, then email results",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "migrate":
        applied = apply_migrations()
        with database() as connection:
            batch = bootstrap_latest_batch(connection)
        print(json.dumps({
            "applied": applied,
            "bootstrapped_batch": dict(batch) if batch is not None else None,
        }, indent=2, default=str))
    elif args.command == "search":
        print(json.dumps(run_daily_search(), indent=2, default=str))
    elif args.command == "continue":
        result = continue_after_review()
        print(json.dumps(result, indent=2, default=str))
        return 1 if result["status"] == "FAILED" else 0
    elif args.command == "run-nightly":
        from .scheduler.tasks import nightly_search_task

        print(json.dumps(nightly_search_task(), indent=2, default=str))
    elif args.command == "run-review-reminder":
        from .scheduler.tasks import morning_review_reminder_task

        print(json.dumps(morning_review_reminder_task(), indent=2, default=str))
    elif args.command == "email-test":
        from .notifications.diagnostics import send_email_test

        result = send_email_test(args.message_type)
        print(json.dumps(result, indent=2, default=str))
        return 0 if result["notification"]["status"] == "SENT" else 1
    elif args.command == "scheduler":
        from .scheduler.scheduler import run_scheduler

        run_scheduler()
    elif args.command == "web":
        from .ui.app import run_web

        run_web()
    elif args.command == "windows-nightly-worker":
        from .windows_automation import run_windows_nightly_worker

        print(json.dumps(run_windows_nightly_worker(), indent=2, default=str))
    elif args.command == "windows-review-worker":
        from .windows_automation import run_windows_review_worker

        print(json.dumps(run_windows_review_worker(), indent=2, default=str))
    elif args.command == "windows-pipeline-worker":
        from .windows_automation import run_windows_pipeline_worker

        result = run_windows_pipeline_worker()
        print(json.dumps(result, indent=2, default=str))
        return 1 if result["status"] == "FAILED" else 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
