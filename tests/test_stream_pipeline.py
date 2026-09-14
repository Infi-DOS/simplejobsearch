from __future__ import annotations

import asyncio
import sqlite3
import threading

from simplejobsearch import config
from simplejobsearch.db import database
from simplejobsearch.pipeline.streaming import run_streaming_pipeline_async


def test_fetched_jobs_stream_into_ai_before_detail_stage_finishes(tmp_path, monkeypatch):
    path = tmp_path / "jobs.db"
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE jobs (
            job_id TEXT PRIMARY KEY,
            classifier_status TEXT,
            human_decision TEXT,
            details_status TEXT,
            metadata_gate_status TEXT,
            ai_status TEXT,
            post_ai_status TEXT
        );
        INSERT INTO jobs VALUES
            ('already-fetched', 'AUTO_KEEP', NULL, 'FETCHED', NULL, NULL, NULL),
            ('newly-fetched', 'AUTO_KEEP', NULL, 'NOT_FETCHED', NULL, NULL, NULL),
            ('metadata-reject', 'AUTO_KEEP', NULL, 'FETCHED', NULL, NULL, NULL);
        """
    )
    connection.close()
    monkeypatch.setenv("JOBSEARCH_DB_PATH", str(path))
    config.reset_settings_cache()

    events: list[str] = []
    new_job_reached_ai = threading.Event()
    streamed_before_details_returned = []

    def details_stage(*, job_ids, on_fetched):
        assert tuple(job_ids) == (
            "already-fetched",
            "newly-fetched",
            "metadata-reject",
        )
        events.append("details-start")
        with database() as db:
            db.execute(
                "UPDATE jobs SET details_status = 'FETCHED' WHERE job_id = 'newly-fetched'"
            )
            db.commit()
        on_fetched("newly-fetched")
        streamed_before_details_returned.append(new_job_reached_ai.wait(timeout=2))
        events.append("details-end")
        return {
            "processed": 1,
            "fetched": 1,
            "failed": 0,
            "fetched_job_ids": ["newly-fetched"],
            "failed_job_ids": [],
        }

    def metadata_stage(*, job_ids):
        job_id = job_ids[0]
        status = "REJECT" if job_id == "metadata-reject" else "PASS"
        events.append(f"metadata-{job_id}")
        with database() as db:
            db.execute(
                "UPDATE jobs SET metadata_gate_status = ? WHERE job_id = ?",
                (status, job_id),
            )
            db.commit()
        return {"processed": 1, "PASS": status == "PASS", "REJECT": status == "REJECT", "job_ids": [job_id]}

    async def ai_stream_stage(*, job_ids, total_hint):
        assert total_hint == 3
        processed = []
        async for job_id in job_ids:
            events.append(f"ai-{job_id}")
            processed.append(job_id)
            with database() as db:
                db.execute(
                    """
                    UPDATE jobs
                    SET ai_status = 'EXTRACTED', post_ai_status = 'SHORTLIST'
                    WHERE job_id = ?
                    """,
                    (job_id,),
                )
                db.commit()
            if job_id == "newly-fetched":
                new_job_reached_ai.set()
        return {
            "processed": len(processed),
            "attempted": len(processed),
            "extracted": len(processed),
            "failed": 0,
            "job_ids": processed,
        }

    def post_ai_stage(*, job_ids):
        assert job_ids == []
        return {"processed": 0, "SHORTLIST": 0, "REVIEW": 0, "REJECT": 0, "job_ids": []}

    result = asyncio.run(
        run_streaming_pipeline_async(
            job_ids=["already-fetched", "newly-fetched", "metadata-reject"],
            details_stage=details_stage,
            metadata_stage=metadata_stage,
            ai_stream_stage=ai_stream_stage,
            post_ai_stage=post_ai_stage,
        )
    )

    assert streamed_before_details_returned == [True]
    assert events.index("ai-newly-fetched") < events.index("details-end")
    assert set(result["ai"]["job_ids"]) == {"already-fetched", "newly-fetched"}
    assert result["metadata"]["processed"] == 3
    assert result["metadata"]["PASS"] == 2
    assert result["metadata"]["REJECT"] == 1
    assert result["post_ai"]["processed"] == 2
    assert result["post_ai"]["SHORTLIST"] == 2
    config.reset_settings_cache()
