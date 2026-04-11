CREATE TABLE raw.crypto_market_raw (
    ingestion_id SERIAL PRIMARY KEY,
    coin_id TEXT,
    symbol TEXT,
    fetched_at TIMESTAMP,
    page_number INT,
    raw_payload JSONB,
    record_hash TEXT
);