-- Add Switzerland as a second independently measurable discovery market.
-- The original five keys remain the Netherlands baseline; the *_ch keys keep
-- Swiss hits and downstream outcomes separate in search_query_metrics.

INSERT INTO search_queries (
    query_key,
    query_text,
    role_family,
    enabled,
    country,
    location,
    sort_order,
    created_at,
    updated_at
)
VALUES
    ('ai_ch', 'artificial intelligence', 'AI', 1,
     'Switzerland', 'Switzerland', 11, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    ('data_science_ch', 'data science', 'DS', 1,
     'Switzerland', 'Switzerland', 21, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    ('machine_learning_ch', 'machine learning', 'ML', 1,
     'Switzerland', 'Switzerland', 31, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    ('computer_vision_ch', 'computer vision', 'CV', 1,
     'Switzerland', 'Switzerland', 41, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    ('deep_learning_ch', 'deep learning', 'DL', 1,
     'Switzerland', 'Switzerland', 51, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)

ON CONFLICT(query_key) DO UPDATE SET
    query_text = excluded.query_text,
    role_family = excluded.role_family,
    enabled = 1,
    country = excluded.country,
    location = excluded.location,
    sort_order = excluded.sort_order,
    updated_at = CURRENT_TIMESTAMP;
