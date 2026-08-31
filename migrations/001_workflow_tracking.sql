CREATE TABLE IF NOT EXISTS daily_batches (
    batch_id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_date TEXT NOT NULL UNIQUE,
    search_run_id TEXT,
    status TEXT NOT NULL DEFAULT 'SEARCH_PENDING'
        CHECK (status IN (
            'SEARCH_PENDING', 'SEARCHING', 'WAITING_FOR_REVIEW',
            'READY_TO_CONTINUE', 'PROCESSING', 'COMPLETE', 'FAILED'
        )),
    search_started_at TEXT,
    search_completed_at TEXT,
    review_completed_at TEXT,
    pipeline_started_at TEXT,
    pipeline_completed_at TEXT,
    new_job_count INTEGER NOT NULL DEFAULT 0,
    review_required_count INTEGER NOT NULL DEFAULT 0,
    details_fetched_count INTEGER NOT NULL DEFAULT 0,
    metadata_pass_count INTEGER NOT NULL DEFAULT 0,
    metadata_reject_count INTEGER NOT NULL DEFAULT 0,
    ai_processed_count INTEGER NOT NULL DEFAULT 0,
    shortlist_count INTEGER NOT NULL DEFAULT 0,
    final_review_count INTEGER NOT NULL DEFAULT 0,
    reject_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (search_run_id) REFERENCES search_runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_daily_batches_status
    ON daily_batches(status, batch_date);

CREATE TABLE IF NOT EXISTS daily_batch_jobs (
    batch_id INTEGER NOT NULL,
    job_id TEXT NOT NULL,
    is_new INTEGER NOT NULL DEFAULT 0 CHECK (is_new IN (0, 1)),
    observed_at TEXT NOT NULL,
    PRIMARY KEY (batch_id, job_id),
    FOREIGN KEY (batch_id) REFERENCES daily_batches(batch_id) ON DELETE CASCADE,
    FOREIGN KEY (job_id) REFERENCES jobs(job_id)
);

CREATE INDEX IF NOT EXISTS idx_daily_batch_jobs_job
    ON daily_batch_jobs(job_id, batch_id);

CREATE TABLE IF NOT EXISTS search_queries (
    query_key TEXT PRIMARY KEY,
    query_text TEXT NOT NULL,
    role_family TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    country TEXT,
    location TEXT,
    sort_order INTEGER NOT NULL DEFAULT 100,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS search_query_metrics (
    run_id TEXT NOT NULL,
    query_key TEXT NOT NULL,
    hits INTEGER NOT NULL DEFAULT 0,
    unique_jobs INTEGER NOT NULL DEFAULT 0,
    new_jobs INTEGER NOT NULL DEFAULT 0,
    auto_keep INTEGER NOT NULL DEFAULT 0,
    review INTEGER NOT NULL DEFAULT 0,
    auto_exclude INTEGER NOT NULL DEFAULT 0,
    metadata_pass INTEGER NOT NULL DEFAULT 0,
    metadata_reject INTEGER NOT NULL DEFAULT 0,
    final_shortlist INTEGER NOT NULL DEFAULT 0,
    final_review INTEGER NOT NULL DEFAULT 0,
    final_reject INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (run_id, query_key),
    FOREIGN KEY (run_id) REFERENCES search_runs(run_id),
    FOREIGN KEY (query_key) REFERENCES search_queries(query_key)
);

