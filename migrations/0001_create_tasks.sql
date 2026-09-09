CREATE TABLE IF NOT EXISTS tasks (
    id UUID PRIMARY KEY,
    task JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS tasks_updated_at_idx ON tasks (updated_at DESC);
