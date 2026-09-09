CREATE TABLE IF NOT EXISTS memory_records (
    agent_id UUID NOT NULL,
    scope TEXT NOT NULL CHECK (scope IN ('working', 'persistent')),
    key TEXT NOT NULL,
    record JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ,
    PRIMARY KEY (agent_id, scope, key)
);

CREATE INDEX IF NOT EXISTS memory_records_agent_updated_idx
    ON memory_records (agent_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS memory_records_expiry_idx
    ON memory_records (expires_at)
    WHERE expires_at IS NOT NULL;
