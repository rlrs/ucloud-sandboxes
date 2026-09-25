-- PostgreSQL routing authority. All routing domains cut over together.
CREATE TABLE routing_schema_version (singleton boolean PRIMARY KEY CHECK(singleton), version integer NOT NULL);
INSERT INTO routing_schema_version VALUES (true, 1);
CREATE TABLE gateway_commands (
    command_id uuid PRIMARY KEY,
    command_key text NOT NULL CHECK(command_key ~ '^[0-9a-f]{64}$'),
    kind text NOT NULL CHECK(kind IN ('create','wake')),
    sandbox_id text NOT NULL,
    path text NOT NULL,
    headers jsonb NOT NULL,
    body bytea NOT NULL,
    state text NOT NULL DEFAULT 'queued' CHECK(state IN ('queued','running','done')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    deadline timestamptz NOT NULL,
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    claim_token uuid, claim_until timestamptz,
    attempts integer NOT NULL DEFAULT 0,
    generation bigint,
    result_status integer,
    result_headers jsonb,
    result_body bytea,
    completed_at timestamptz,
    CHECK((claim_token IS NULL)=(claim_until IS NULL))
);
CREATE UNIQUE INDEX gateway_commands_outstanding ON gateway_commands(command_key) WHERE state!='done';
CREATE INDEX gateway_commands_due ON gateway_commands(kind,next_attempt_at,created_at)
    WHERE state!='done';
CREATE INDEX gateway_commands_retention ON gateway_commands(completed_at) WHERE state='done';
CREATE TABLE IF NOT EXISTS sandboxes (
                    sandbox_id TEXT PRIMARY KEY CHECK (length(trim(sandbox_id)) > 0),
                    node_id TEXT NOT NULL CHECK (length(trim(node_id)) > 0),
                    job_id TEXT NOT NULL CHECK (length(trim(job_id)) > 0),
                    node_url TEXT NOT NULL CHECK (length(trim(node_url)) > 0),
                    resources_json TEXT NOT NULL,
                    spec_json TEXT NOT NULL,
                    state TEXT NOT NULL CHECK (length(trim(state)) > 0),
                    generation BIGINT NOT NULL CHECK (generation > 0),
                    create_operation_id TEXT NOT NULL
                        CHECK (length(create_operation_id) BETWEEN 1 AND 128),
                    spec_hash TEXT NOT NULL CHECK (
                        length(spec_hash) = 64
                        AND spec_hash ~ '^[0-9a-f]{64}$'
                    ),
                    delete_operation_id TEXT NOT NULL DEFAULT '',
                    node_epoch TEXT NOT NULL DEFAULT '',
                    activity_epoch BIGINT NOT NULL DEFAULT 0
                        CHECK (activity_epoch >= 0),
                    worker_state TEXT NOT NULL DEFAULT 'attached' CHECK (
                        worker_state IN ('attached', 'detaching', 'detached')
                    ),
                    storage_schema TEXT NOT NULL DEFAULT '',
                    snapshot_manifest_digest TEXT NOT NULL DEFAULT '',
                    snapshot_repository TEXT NOT NULL DEFAULT '',
                    snapshot_tag TEXT NOT NULL DEFAULT '',
                    storage_snapshot_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL CHECK (length(trim(created_at)) > 0),
                    updated_at TEXT NOT NULL CHECK (length(trim(updated_at)) > 0)
                );

CREATE TABLE IF NOT EXISTS sandbox_storage_dependencies (
                    sandbox_id TEXT PRIMARY KEY,
                    generation BIGINT NOT NULL,
                    storage_snapshot_json TEXT NOT NULL
                );

CREATE TABLE IF NOT EXISTS sandbox_generation_hwm (
                    sandbox_id TEXT PRIMARY KEY,
                    generation BIGINT NOT NULL CHECK (generation > 0)
                );

CREATE INDEX IF NOT EXISTS sandboxes_node_id
                ON sandboxes(node_id);

CREATE INDEX IF NOT EXISTS sandboxes_job_id
                ON sandboxes(job_id);

CREATE INDEX IF NOT EXISTS sandboxes_node_url
                ON sandboxes(node_url);

CREATE TABLE IF NOT EXISTS exec_sessions (
                    session_id TEXT PRIMARY KEY,
                    sandbox_id TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    job_id TEXT NOT NULL,
                    node_url TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

CREATE INDEX IF NOT EXISTS exec_sessions_sandbox ON exec_sessions(sandbox_id);

CREATE TABLE IF NOT EXISTS managed_processes (
                    sandbox_id TEXT PRIMARY KEY,
                    sandbox_generation BIGINT NOT NULL,
                    job_id TEXT NOT NULL,
                    spec_sha256 TEXT NOT NULL,
                    record_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

CREATE UNIQUE INDEX IF NOT EXISTS managed_process_identity
                ON managed_processes (
                    sandbox_id, sandbox_generation, job_id, spec_sha256
                );

CREATE TABLE IF NOT EXISTS sandbox_migrations (
                    migration_id TEXT PRIMARY KEY,
                    sandbox_id TEXT NOT NULL,
                    phase TEXT NOT NULL,
                    source_node_id TEXT NOT NULL,
                    source_job_id TEXT NOT NULL,
                    source_node_url TEXT NOT NULL,
                    destination_node_id TEXT NOT NULL,
                    destination_job_id TEXT NOT NULL,
                    destination_node_url TEXT NOT NULL,
                    generation BIGINT NOT NULL,
                    create_operation_id TEXT NOT NULL,
                    spec_hash TEXT NOT NULL,
                    storage_schema TEXT NOT NULL DEFAULT '',
                    snapshot_sha256 TEXT NOT NULL DEFAULT '',
                    storage_snapshot_json TEXT NOT NULL DEFAULT '{}',
                    source_fenced BIGINT NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    error TEXT NOT NULL DEFAULT ''
                );

CREATE UNIQUE INDEX IF NOT EXISTS
                    sandbox_migrations_active_sandbox
                ON sandbox_migrations (sandbox_id)
                WHERE phase != 'complete';

CREATE TABLE IF NOT EXISTS pending (
                    sandbox_id TEXT PRIMARY KEY,
                    resources_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    attempts BIGINT NOT NULL,
                    generation BIGINT NOT NULL DEFAULT 0,
                    operation_id TEXT NOT NULL DEFAULT '',
                    spec_hash TEXT NOT NULL DEFAULT '',
                    failure_reason TEXT NOT NULL DEFAULT ''
                );

CREATE TABLE IF NOT EXISTS image_builds (
                    image_id TEXT PRIMARY KEY,
                    tag TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    attempts BIGINT NOT NULL
                );

CREATE TABLE IF NOT EXISTS prepared_capacity (
                    prepare_id TEXT PRIMARY KEY,
                    resources_json TEXT NOT NULL,
                    count BIGINT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    image TEXT NOT NULL DEFAULT ''
                );

CREATE TABLE IF NOT EXISTS prepared_builders (
                    prepare_id TEXT PRIMARY KEY,
                    count BIGINT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL
                );

CREATE TABLE IF NOT EXISTS image_warmups (
                    warmup_id TEXT PRIMARY KEY,
                    image TEXT NOT NULL,
                    image_id TEXT NOT NULL DEFAULT '',
                    resources_json TEXT NOT NULL,
                    count BIGINT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    warmed_node_ids_json TEXT NOT NULL DEFAULT '[]',
                    attempts BIGINT NOT NULL DEFAULT 1
                );

CREATE TABLE IF NOT EXISTS program_requests (
                    request_id TEXT PRIMARY KEY,
                    rollout_id TEXT NOT NULL,
                    sandbox_id TEXT NOT NULL,
                    sandbox_generation BIGINT NOT NULL,
                    state TEXT NOT NULL,
                    resources_json TEXT NOT NULL,
                    accepted_at TEXT NOT NULL DEFAULT '',
                    parked_at TEXT NOT NULL DEFAULT '',
                    response_ready_at TEXT NOT NULL DEFAULT '',
                    wake_started_at TEXT NOT NULL DEFAULT '',
                    wake_completed_at TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL,
                    last_error TEXT NOT NULL DEFAULT ''
                );

CREATE INDEX IF NOT EXISTS program_requests_state_updated
                ON program_requests(state, updated_at);

CREATE INDEX IF NOT EXISTS program_requests_rollout
                ON program_requests(rollout_id, updated_at);

CREATE INDEX IF NOT EXISTS program_requests_sandbox
                ON program_requests(sandbox_id, sandbox_generation);

CREATE TABLE IF NOT EXISTS exec_losses (
                    session_id TEXT PRIMARY KEY,
                    sandbox_id TEXT NOT NULL,
                    generation BIGINT NOT NULL CHECK (generation > 0),
                    job_id TEXT NOT NULL,
                    lost_at TEXT NOT NULL
                );

CREATE INDEX IF NOT EXISTS exec_losses_time ON exec_losses(lost_at);

CREATE TABLE IF NOT EXISTS sandbox_losses (
                    sandbox_id TEXT PRIMARY KEY,
                    generation BIGINT NOT NULL CHECK (generation > 0),
                    job_id TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    lost_at TEXT NOT NULL
                );

CREATE INDEX IF NOT EXISTS sandbox_losses_time ON sandbox_losses(lost_at);

CREATE INDEX sandbox_migrations_destination_node_id ON sandbox_migrations(destination_node_id) WHERE phase!='complete';
CREATE INDEX sandbox_migrations_destination_job_id ON sandbox_migrations(destination_job_id) WHERE phase!='complete';
CREATE INDEX sandbox_migrations_destination_node_url ON sandbox_migrations(destination_node_url) WHERE phase!='complete';

CREATE TABLE worker_capacity_revisions(identity text PRIMARY KEY,revision bigint NOT NULL);
