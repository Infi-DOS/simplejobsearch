from __future__ import annotations

import sqlite3

from simplejobsearch.pipeline import details


class FakeConnection:
    def close(self):
        pass


def test_detail_service_does_not_reapply_pre_description_rules(monkeypatch):
    saved = []
    job = {"job_id": "li-1"}
    monkeypatch.setattr(details, "connect", lambda: FakeConnection())
    queue_arguments = {}
    monkeypatch.setattr(
        details.legacy,
        "load_detail_queue",
        lambda _connection, **kwargs: queue_arguments.update(kwargs) or [job],
    )
    monkeypatch.setattr(details.legacy, "mark_attempt", lambda *_args: None)
    monkeypatch.setattr(
        details.legacy,
        "save_success",
        lambda _connection, job_id, _payload: saved.append(job_id),
    )
    monkeypatch.setattr(
        details.legacy,
        "refresh_pre_classification",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("PRE_DESCRIPTION was re-applied")
        ),
    )

    result = details.fetch_approved_details(
        job_ids=["li-1"],
        client_factory=lambda: object(),
        fetcher=lambda _client, _job_id: {"description": "A full description"},
        wait=lambda: None,
    )

    assert saved == ["li-1"]
    assert result["fetched"] == 1
    assert result["failed"] == 0
    assert queue_arguments["job_ids"] == ("li-1",)


def test_detail_queue_applies_batch_filter_before_limit():
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.executescript(
        """
        CREATE TABLE jobs (
            job_id TEXT PRIMARY KEY,
            site TEXT,
            title TEXT,
            company TEXT,
            location TEXT,
            date_posted TEXT,
            job_url TEXT,
            is_remote INTEGER,
            human_decision TEXT,
            classifier_status TEXT,
            review_category TEXT,
            review_reason TEXT,
            suggested_action TEXT,
            details_status TEXT,
            details_attempt_count INTEGER,
            first_seen_at TEXT
        );
        CREATE VIEW approved_for_details AS SELECT * FROM jobs;
        INSERT INTO jobs VALUES
            ('old-job', 'linkedin', 'Old', 'A', '', '', '', 0, 'KEEP',
             'REVIEW', '', '', '', 'NOT_FETCHED', 0, '2026-09-01'),
            ('current-job', 'linkedin', 'Current', 'B', '', '', '', 0, 'KEEP',
             'REVIEW', '', '', '', 'NOT_FETCHED', 0, '2026-09-03');
        """
    )

    queue = details.legacy.load_detail_queue(
        connection,
        job_ids=["current-job"],
        limit=1,
    )

    assert [row["job_id"] for row in queue] == ["current-job"]

    unlimited_queue = details.legacy.load_detail_queue(
        connection,
        job_ids=["old-job", "current-job"],
        limit=0,
    )

    assert [row["job_id"] for row in unlimited_queue] == [
        "old-job",
        "current-job",
    ]

