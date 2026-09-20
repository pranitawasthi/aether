CREATE TABLE IF NOT EXISTS agents (
    id UUID PRIMARY KEY,
    agent JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS agents_updated_at_idx ON agents (updated_at DESC);
