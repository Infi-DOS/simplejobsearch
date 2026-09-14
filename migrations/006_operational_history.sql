CREATE TABLE IF NOT EXISTS task_runs (
    task_run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_name TEXT NOT NULL,
    batch_id INTEGER,
    batch_date TEXT,
    process_id INTEGER NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    status TEXT NOT NULL,
    notification_status TEXT,
    error TEXT,
    summary_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_task_runs_started ON task_runs(started_at);

CREATE TABLE IF NOT EXISTS notification_events (
    notification_id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_run_id INTEGER REFERENCES task_runs(task_run_id),
    batch_id INTEGER,
    batch_date TEXT,
    notification_type TEXT NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    status TEXT NOT NULL,
    sent INTEGER NOT NULL DEFAULT 0,
    reason TEXT,
    error TEXT
);
CREATE INDEX IF NOT EXISTS idx_notification_events_started
    ON notification_events(started_at);
