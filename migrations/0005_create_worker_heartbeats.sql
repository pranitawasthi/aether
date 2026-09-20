CREATE TABLE IF NOT EXISTS worker_heartbeats (
    worker_id TEXT PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS worker_heartbeats_last_seen_idx
    ON worker_heartbeats (last_seen_at DESC);
