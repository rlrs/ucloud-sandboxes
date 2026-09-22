-- Qualification schema. Versioned independently of the deployed SQLite stores.
CREATE TABLE schema_version (singleton boolean PRIMARY KEY CHECK (singleton), version integer NOT NULL);
INSERT INTO schema_version VALUES (true, 1);

CREATE TABLE nodes (
    deployment_id text NOT NULL, node_id text NOT NULL, node_epoch text NOT NULL,
    restore_budget_mb bigint NOT NULL CHECK (restore_budget_mb > 0),
    reserved_restore_mb bigint NOT NULL DEFAULT 0 CHECK (reserved_restore_mb >= 0),
    PRIMARY KEY (deployment_id, node_id)
);
CREATE TABLE sandbox_identities (
    deployment_id text NOT NULL, sandbox_id text NOT NULL,
    generation bigint NOT NULL CHECK (generation > 0),
    PRIMARY KEY (deployment_id, sandbox_id)
);
CREATE TABLE sandboxes (
    deployment_id text NOT NULL, sandbox_id text NOT NULL,
    generation bigint NOT NULL CHECK (generation > 0),
    create_operation_id text NOT NULL, spec_hash text NOT NULL,
    node_id text NOT NULL, node_epoch text NOT NULL,
    state text NOT NULL CHECK (state IN ('parked', 'waking', 'running')),
    lifecycle_sequence bigint NOT NULL DEFAULT 0 CHECK (lifecycle_sequence >= 0),
    activity_epoch bigint NOT NULL DEFAULT 0 CHECK (activity_epoch >= 0),
    restore_mb bigint NOT NULL CHECK (restore_mb > 0),
    PRIMARY KEY (deployment_id, sandbox_id),
    FOREIGN KEY (deployment_id, sandbox_id) REFERENCES sandbox_identities,
    FOREIGN KEY (deployment_id, node_id) REFERENCES nodes
);
CREATE TABLE wake_operations (
    deployment_id text NOT NULL, operation_id uuid NOT NULL,
    sandbox_id text NOT NULL, generation bigint NOT NULL,
    create_operation_id text NOT NULL, spec_hash text NOT NULL,
    node_id text NOT NULL, node_epoch text NOT NULL,
    lifecycle_sequence bigint NOT NULL CHECK (lifecycle_sequence > 0),
    restore_mb bigint NOT NULL CHECK (restore_mb > 0),
    state text NOT NULL CHECK (state IN ('queued', 'dispatching', 'succeeded')),
    claim_token uuid, claim_until timestamptz,
    attempts integer NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz, last_reason text,
    PRIMARY KEY (deployment_id, operation_id),
    FOREIGN KEY (deployment_id, sandbox_id) REFERENCES sandboxes,
    CHECK ((claim_token IS NULL) = (claim_until IS NULL))
);
CREATE UNIQUE INDEX one_pending_wake ON wake_operations (deployment_id, sandbox_id)
    WHERE state != 'succeeded';
CREATE INDEX due_wakes ON wake_operations (deployment_id, next_attempt_at, created_at)
    WHERE state != 'succeeded';
CREATE TABLE restore_reservations (
    deployment_id text NOT NULL, operation_id uuid NOT NULL,
    node_id text NOT NULL, amount_mb bigint NOT NULL CHECK (amount_mb > 0),
    PRIMARY KEY (deployment_id, operation_id),
    FOREIGN KEY (deployment_id, operation_id) REFERENCES wake_operations,
    FOREIGN KEY (deployment_id, node_id) REFERENCES nodes
);
CREATE TABLE model_requests (
    deployment_id text NOT NULL, request_id text NOT NULL,
    sandbox_id text NOT NULL, generation bigint NOT NULL,
    registration_id text NOT NULL, lease_id text NOT NULL, lease_until timestamptz NOT NULL,
    response_hash text, operation_id uuid,
    accepted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    result_committed_at timestamptz, delivered_at timestamptz,
    PRIMARY KEY (deployment_id, request_id),
    FOREIGN KEY (deployment_id, sandbox_id) REFERENCES sandboxes,
    FOREIGN KEY (deployment_id, operation_id) REFERENCES wake_operations
);
CREATE TABLE model_responses (
    deployment_id text NOT NULL, request_id text NOT NULL, body bytea NOT NULL,
    status integer NOT NULL CHECK (status BETWEEN 100 AND 599), headers jsonb NOT NULL,
    PRIMARY KEY (deployment_id, request_id),
    FOREIGN KEY (deployment_id, request_id) REFERENCES model_requests
);
