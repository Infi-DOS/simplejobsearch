ALTER TABLE search_query_metrics
    ADD COLUMN human_final_accepted INTEGER NOT NULL DEFAULT 0;
ALTER TABLE search_query_metrics
    ADD COLUMN human_final_rejected INTEGER NOT NULL DEFAULT 0;
