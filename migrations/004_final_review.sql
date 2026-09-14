ALTER TABLE jobs ADD COLUMN final_decision TEXT
    CHECK (final_decision IS NULL OR final_decision IN ('ACCEPTED', 'REJECTED'));
ALTER TABLE jobs ADD COLUMN final_decided_at TEXT;
ALTER TABLE jobs ADD COLUMN final_notes TEXT;

ALTER TABLE daily_batches ADD COLUMN final_review_status TEXT NOT NULL
    DEFAULT 'NOT_STARTED'
    CHECK (final_review_status IN ('NOT_STARTED', 'AWAITING_REVIEW', 'FINALIZED'));
ALTER TABLE daily_batches ADD COLUMN final_review_started_at TEXT;
ALTER TABLE daily_batches ADD COLUMN final_review_completed_at TEXT;
ALTER TABLE daily_batches ADD COLUMN final_pending_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE daily_batches ADD COLUMN final_accepted_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE daily_batches ADD COLUMN final_rejected_count INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_jobs_final_review
    ON jobs(post_ai_status, final_decision);

UPDATE daily_batches
SET final_pending_count = (
        SELECT COUNT(*)
        FROM daily_batch_jobs b
        JOIN jobs j ON j.job_id = b.job_id
        WHERE b.batch_id = daily_batches.batch_id
          AND j.post_ai_status = 'SHORTLIST'
          AND (j.final_decision IS NULL OR TRIM(j.final_decision) = '')
    ),
    final_accepted_count = (
        SELECT COUNT(*)
        FROM daily_batch_jobs b
        JOIN jobs j ON j.job_id = b.job_id
        WHERE b.batch_id = daily_batches.batch_id
          AND j.final_decision = 'ACCEPTED'
    ),
    final_rejected_count = (
        SELECT COUNT(*)
        FROM daily_batch_jobs b
        JOIN jobs j ON j.job_id = b.job_id
        WHERE b.batch_id = daily_batches.batch_id
          AND j.final_decision = 'REJECTED'
    ),
    final_review_status = CASE
        WHEN status = 'COMPLETE' AND EXISTS (
            SELECT 1
            FROM daily_batch_jobs b
            JOIN jobs j ON j.job_id = b.job_id
            WHERE b.batch_id = daily_batches.batch_id
              AND j.post_ai_status = 'SHORTLIST'
              AND (j.final_decision IS NULL OR TRIM(j.final_decision) = '')
        ) THEN 'AWAITING_REVIEW'
        WHEN status = 'COMPLETE' THEN 'FINALIZED'
        ELSE 'NOT_STARTED'
    END,
    final_review_started_at = CASE
        WHEN status = 'COMPLETE' THEN COALESCE(pipeline_completed_at, updated_at)
        ELSE NULL
    END,
    final_review_completed_at = CASE
        WHEN status = 'COMPLETE' AND NOT EXISTS (
            SELECT 1
            FROM daily_batch_jobs b
            JOIN jobs j ON j.job_id = b.job_id
            WHERE b.batch_id = daily_batches.batch_id
              AND j.post_ai_status = 'SHORTLIST'
              AND (j.final_decision IS NULL OR TRIM(j.final_decision) = '')
        ) THEN COALESCE(pipeline_completed_at, updated_at)
        ELSE NULL
    END;
