CREATE TABLE IF NOT EXISTS pipeline_bookmarks (
    table_name TEXT PRIMARY KEY,
    last_processed_date DATE NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
