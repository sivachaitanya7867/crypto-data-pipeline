CREATE TABLE IF NOT EXISTS stg.pipeline_state (
    pipeline_name TEXT PRIMARY KEY,
    last_run_date DATE
);

INSERT INTO stg.pipeline_state (pipeline_name, last_run_date)
VALUES ('crypto_pipeline', NULL)
ON CONFLICT (pipeline_name) DO NOTHING;