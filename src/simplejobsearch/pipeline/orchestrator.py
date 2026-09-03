from __future__ import annotations

from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import date

from ..db import apply_migrations, database
from ..notifications.email import (
    deliver_notification,
    notification_not_attempted,
    send_pipeline_complete_email,
)
from ..workflow import (
    batch_job_ids,
    batch_summary,
    mark_review_state,
    now_iso,
    record_query_metrics,
    refresh_batch_counts,
    resolve_batch,
    unfinished_ai_job_ids,
    unfinished_pipeline_job_ids,
    update_batch,
)
from .ai_extractor import run_ai_extraction
from .details import fetch_approved_details
from .metadata_gate import run_metadata_gate
from .post_ai import run_post_ai


class PendingReviewError(RuntimeError):
    pass


@dataclass(frozen=True)
class RunActivity:
    details_fetched: int = 0
    details_failed: int = 0
    metadata_evaluated: int = 0
    metadata_pass: int = 0
    metadata_reject: int = 0
    ai_processed: int = 0
    ai_extracted: int = 0
    ai_failed: int = 0
    post_ai_processed: int = 0
    shortlist: int = 0
    review: int = 0
    reject: int = 0


@dataclass(frozen=True)
class BatchTotals:
    details_fetched: int = 0
    metadata_pass: int = 0
    metadata_reject: int = 0
    ai_extracted: int = 0
    shortlist: int = 0
    review: int = 0
    reject: int = 0
    post_ai_classified: int = 0
    ai_failed: int = 0
    unfinished: int = 0


@dataclass(frozen=True)
class PipelineSummary:
    batch_id: int | None = None
    batch_date: str | None = None
    status: str = "COMPLETE"
    already_complete: bool = False
    no_op: bool = False
    message: str = "Pipeline completed."
    last_error: str | None = None
    this_run: RunActivity = RunActivity()
    batch_totals: BatchTotals = BatchTotals()

    def to_dict(self) -> dict:
        return asdict(self)


def _batch_totals(counts: dict) -> BatchTotals:
    shortlist = counts["shortlist_count"]
    review = counts["final_review_count"]
    reject = counts["reject_count"]
    return BatchTotals(
        details_fetched=counts["details_fetched_count"],
        metadata_pass=counts["metadata_pass_count"],
        metadata_reject=counts["metadata_reject_count"],
        ai_extracted=counts["ai_processed_count"],
        shortlist=shortlist,
        review=review,
        reject=reject,
        post_ai_classified=shortlist + review + reject,
        ai_failed=counts.get("ai_failed_count", 0),
        unfinished=counts.get("unfinished_count", 0),
    )


def _pipeline_summary(
    batch,
    counts: dict,
    *,
    activity: RunActivity | None = None,
    already_complete: bool = False,
    status: str = "COMPLETE",
    message: str | None = None,
    last_error: str | None = None,
) -> dict:
    summary = PipelineSummary(
        batch_id=batch["batch_id"],
        batch_date=batch["batch_date"],
        status=status,
        already_complete=already_complete,
        no_op=already_complete,
        message=message or (
            f"Batch {batch['batch_id']} is already COMPLETE. No pipeline work performed."
            if already_complete
            else "Pipeline completed."
        ),
        last_error=last_error,
        this_run=activity or RunActivity(),
        batch_totals=_batch_totals(counts),
    ).to_dict()
    if already_complete:
        summary["notification"] = notification_not_attempted(
            "pipeline_complete",
            "batch_already_complete",
        )
    return summary


PERSISTED_COUNT_FIELDS = (
    "new_job_count",
    "review_required_count",
    "details_fetched_count",
    "metadata_pass_count",
    "metadata_reject_count",
    "ai_processed_count",
    "shortlist_count",
    "final_review_count",
    "reject_count",
)


def _persisted_counts(counts: dict) -> dict[str, int]:
    return {name: int(counts[name]) for name in PERSISTED_COUNT_FIELDS}


def _unfinished_error(job_ids: list[str]) -> str:
    noun = "job" if len(job_ids) == 1 else "jobs"
    verb = "remains" if len(job_ids) == 1 else "remain"
    displayed = ", ".join(job_ids[:10])
    if len(job_ids) > 10:
        displayed += f", and {len(job_ids) - 10} more"
    return f"{len(job_ids)} approved pipeline {noun} {verb} unfinished: {displayed}"


def continue_after_review(
    *,
    batch_date: date | str | None = None,
    details_stage: Callable = fetch_approved_details,
    metadata_stage: Callable = run_metadata_gate,
    ai_stage: Callable = run_ai_extraction,
    post_ai_stage: Callable = run_post_ai,
    email_sender: Callable = send_pipeline_complete_email,
) -> dict:
    """Continue one reviewed batch through the existing reusable stages."""
    apply_migrations()
    with database() as connection:
        batch = resolve_batch(connection, batch_date)
        if batch["status"] == "COMPLETE":
            unfinished = unfinished_pipeline_job_ids(connection, batch["batch_id"])
            if not unfinished:
                return _pipeline_summary(
                    batch,
                    refresh_batch_counts(connection, batch["batch_id"]),
                    already_complete=True,
                )
            # Repair batches marked COMPLETE by older versions that did not
            # enforce the complete approved-pipeline invariant.
            update_batch(
                connection,
                batch["batch_id"],
                status="FAILED",
                pipeline_completed_at=None,
                last_error=_unfinished_error(unfinished),
            )
            batch = batch_summary(connection, batch["batch_id"])
        pending = mark_review_state(connection, batch["batch_id"])
        if pending:
            raise PendingReviewError(
                f"{pending} job(s) still require a human decision for batch {batch['batch_date']}"
            )
        job_ids = batch_job_ids(connection, batch["batch_id"])
        update_batch(
            connection,
            batch["batch_id"],
            status="PROCESSING",
            pipeline_started_at=batch["pipeline_started_at"] or now_iso(),
            pipeline_completed_at=None,
            last_error=None,
        )

    try:
        details = details_stage(job_ids=job_ids)
        metadata = metadata_stage(job_ids=job_ids)

        with database() as connection:
            ai_job_ids = unfinished_ai_job_ids(connection, batch["batch_id"])
        ai = ai_stage(job_ids=ai_job_ids)

        with database() as connection:
            post_ai_job_ids = unfinished_ai_job_ids(connection, batch["batch_id"])
        post_ai = post_ai_stage(job_ids=post_ai_job_ids)

        activity = RunActivity(
            details_fetched=int(details.get("fetched", 0)),
            details_failed=int(details.get("failed", 0)),
            metadata_evaluated=int(metadata.get("processed", 0)),
            metadata_pass=int(metadata.get("PASS", 0)),
            metadata_reject=int(metadata.get("REJECT", 0)),
            ai_processed=int(ai.get("attempted", 0)),
            ai_extracted=int(ai.get("extracted", 0)),
            ai_failed=int(ai.get("failed", 0)),
            post_ai_processed=int(post_ai.get("processed", 0)),
            shortlist=int(post_ai.get("SHORTLIST", 0)),
            review=int(post_ai.get("REVIEW", 0)),
            reject=int(post_ai.get("REJECT", 0)),
        )

        with database() as connection:
            counts = refresh_batch_counts(connection, batch["batch_id"])
            unfinished = unfinished_pipeline_job_ids(connection, batch["batch_id"])
            if unfinished:
                last_error = _unfinished_error(unfinished)
                update_batch(
                    connection,
                    batch["batch_id"],
                    status="FAILED",
                    pipeline_completed_at=None,
                    last_error=last_error,
                    **_persisted_counts(counts),
                )
                summary = _pipeline_summary(
                    batch,
                    counts,
                    activity=activity,
                    status="FAILED",
                    message=last_error,
                    last_error=last_error,
                )
            else:
                update_batch(
                    connection,
                    batch["batch_id"],
                    status="COMPLETE",
                    pipeline_completed_at=now_iso(),
                    last_error=None,
                    **_persisted_counts(counts),
                )
                current = batch_summary(connection, batch["batch_id"])
                if current.get("search_run_id"):
                    record_query_metrics(connection, current["search_run_id"])
                summary = _pipeline_summary(
                    batch,
                    counts,
                    activity=activity,
                )
    except Exception as exc:
        with database() as connection:
            update_batch(
                connection,
                batch["batch_id"],
                status="FAILED",
                pipeline_completed_at=None,
                last_error=f"{type(exc).__name__}: {exc}",
            )
        raise

    if summary["status"] == "FAILED":
        summary["notification"] = notification_not_attempted(
            "pipeline_complete",
            "batch_incomplete",
        )
        return summary

    summary["notification"] = deliver_notification(
        email_sender,
        summary,
        notification_type="pipeline_complete",
    )
    return summary
