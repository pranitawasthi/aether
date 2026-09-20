CREATE TABLE IF NOT EXISTS task_leases (
    task_id UUID PRIMARY KEY REFERENCES tasks(id) ON DELETE CASCADE,
    priority SMALLINT NOT NULL,
    sequence BIGSERIAL NOT NULL,
    available_at TIMESTAMPTZ NOT NULL,
    lease_owner TEXT,
    lease_token UUID,
    lease_expires_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS task_leases_claim_idx
    ON task_leases (priority DESC, sequence ASC)
    WHERE completed_at IS NULL AND lease_owner IS NULL;

CREATE INDEX IF NOT EXISTS task_leases_expiry_idx
    ON task_leases (lease_expires_at)
    WHERE completed_at IS NULL AND lease_expires_at IS NOT NULL;
