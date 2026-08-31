from __future__ import annotations

from simplejobsearch.pipeline import details


class FakeConnection:
    def close(self):
        pass


def test_detail_service_does_not_reapply_pre_description_rules(monkeypatch):
    saved = []
    job = {"job_id": "li-1"}
    monkeypatch.setattr(details, "connect", lambda: FakeConnection())
    monkeypatch.setattr(details.legacy, "load_detail_queue", lambda _connection: [job])
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

