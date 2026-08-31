-- 002_five_search_queries.sql
--
-- Replace the old 11-query discovery baseline with the simpler 5-query
-- production baseline.
--
-- Historical rows are preserved for metrics/history; the old queries are only
-- disabled, not deleted.

-- ---------------------------------------------------------------------------
-- Disable old baseline
-- ---------------------------------------------------------------------------

UPDATE search_queries
SET
    enabled = 0,
    updated_at = CURRENT_TIMESTAMP
WHERE query_key IN (
    'data_scientist',
    'ml_engineer',
    'ml_scientist',
    'ai_engineer',
    'cv_engineer',
    'cv_scientist',
    'deep_learning_engineer',
    'research_scientist_ml',
    'research_engineer_ml',
    'applied_scientist_ml',
    'perception_engineer'
);


-- ---------------------------------------------------------------------------
-- Insert / activate the new five-query baseline
--
-- country/location are intentionally copied from the first existing query
-- configuration when possible. If no previous value exists, they remain NULL;
-- the collector's normal centralized search settings remain authoritative.
-- ---------------------------------------------------------------------------

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
    (
        'ai',
        'artificial intelligence',
        'AI',
        1,
        (
            SELECT country
            FROM search_queries
            WHERE country IS NOT NULL
            LIMIT 1
        ),
        (
            SELECT location
            FROM search_queries
            WHERE location IS NOT NULL
            LIMIT 1
        ),
        10,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
    ),
    (
        'data_science',
        'data science',
        'DS',
        1,
        (
            SELECT country
            FROM search_queries
            WHERE country IS NOT NULL
            LIMIT 1
        ),
        (
            SELECT location
            FROM search_queries
            WHERE location IS NOT NULL
            LIMIT 1
        ),
        20,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
    ),
    (
        'machine_learning',
        'machine learning',
        'ML',
        1,
        (
            SELECT country
            FROM search_queries
            WHERE country IS NOT NULL
            LIMIT 1
        ),
        (
            SELECT location
            FROM search_queries
            WHERE location IS NOT NULL
            LIMIT 1
        ),
        30,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
    ),
    (
        'computer_vision',
        'computer vision',
        'CV',
        1,
        (
            SELECT country
            FROM search_queries
            WHERE country IS NOT NULL
            LIMIT 1
        ),
        (
            SELECT location
            FROM search_queries
            WHERE location IS NOT NULL
            LIMIT 1
        ),
        40,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
    ),
    (
        'deep_learning',
        'deep learning',
        'DL',
        1,
        (
            SELECT country
            FROM search_queries
            WHERE country IS NOT NULL
            LIMIT 1
        ),
        (
            SELECT location
            FROM search_queries
            WHERE location IS NOT NULL
            LIMIT 1
        ),
        50,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
    )

ON CONFLICT(query_key) DO UPDATE SET
    query_text = excluded.query_text,
    role_family = excluded.role_family,
    enabled = 1,
    sort_order = excluded.sort_order,
    updated_at = CURRENT_TIMESTAMP;
