-- Live relay authority. Bodies are immutable and never rewritten by lease updates.
CREATE TABLE relay_schema_version (singleton boolean PRIMARY KEY CHECK(singleton), version integer NOT NULL);
INSERT INTO relay_schema_version VALUES (true, 1);
CREATE TABLE relay_rollouts (
 deployment_id text NOT NULL, rollout_id text NOT NULL,
 registration_token text NOT NULL, metadata jsonb NOT NULL,
 registered_at double precision NOT NULL, enabled boolean NOT NULL DEFAULT true,
 PRIMARY KEY(deployment_id, rollout_id)
);
CREATE TABLE relay_quota (
 deployment_id text PRIMARY KEY, reserved_bytes bigint NOT NULL DEFAULT 0 CHECK(reserved_bytes >= 0)
);
CREATE TABLE relay_requests (
 deployment_id text NOT NULL, request_id text NOT NULL, rollout_id text NOT NULL,
 registration_token text NOT NULL, endpoint text NOT NULL, method text NOT NULL,
 created_at double precision NOT NULL, expires_at double precision NOT NULL,
 payload_bytes bigint NOT NULL, reserved_bytes bigint NOT NULL,
 state text NOT NULL CHECK(state IN ('pending','leased','completed')),
 idempotency_key text, request_digest text NOT NULL, reattachable boolean NOT NULL,
 sandbox_id text, sandbox_generation bigint,
 lease_id text, lease_expires_at double precision, leased_by text,
 delivered_at double precision, first_delivered_at double precision,
 delivery_count integer NOT NULL DEFAULT 0,
 completed_at double precision, completed_bytes bigint NOT NULL DEFAULT 0,
 delivery_pending boolean NOT NULL DEFAULT false,
 accepted_notified_at double precision, parked_transport_epoch text, wake_notified_at double precision,
 wake_transport_epoch text, delivery_released_at double precision,
 PRIMARY KEY(deployment_id, request_id),
 FOREIGN KEY(deployment_id, rollout_id) REFERENCES relay_rollouts
);
CREATE UNIQUE INDEX relay_retry_identity ON relay_requests(deployment_id, rollout_id, registration_token, idempotency_key)
 WHERE reattachable AND idempotency_key IS NOT NULL;
CREATE INDEX relay_pending ON relay_requests(deployment_id, rollout_id, registration_token, created_at) WHERE state='pending';
CREATE INDEX relay_leases ON relay_requests(deployment_id, lease_expires_at) WHERE state='leased';
CREATE INDEX relay_expiry ON relay_requests(deployment_id, expires_at) WHERE state!='completed';
CREATE INDEX relay_retention ON relay_requests(deployment_id, completed_at) WHERE state='completed' AND NOT delivery_pending;
CREATE INDEX relay_incarnation ON relay_requests(deployment_id, sandbox_id, sandbox_generation);
CREATE INDEX IF NOT EXISTS relay_compaction ON relay_requests(deployment_id, request_id) WHERE state='completed' AND reserved_bytes>completed_bytes+65536;
CREATE INDEX IF NOT EXISTS relay_outstanding_callers ON relay_requests(deployment_id, sandbox_id, sandbox_generation) WHERE state!='completed' OR delivery_pending;
CREATE TABLE relay_payloads (
 deployment_id text NOT NULL, request_id text NOT NULL, body bytea NOT NULL,
 encoding text NOT NULL CHECK(encoding IN ('json','base64')), headers jsonb NOT NULL,
 PRIMARY KEY(deployment_id, request_id),
 FOREIGN KEY(deployment_id, request_id) REFERENCES relay_requests ON DELETE CASCADE
);
CREATE TABLE relay_results (
 deployment_id text NOT NULL, request_id text NOT NULL, body bytea NOT NULL,
 encoding text NOT NULL CHECK(encoding IN ('json','base64')), status integer NOT NULL,
 headers jsonb NOT NULL, digest text NOT NULL,
 PRIMARY KEY(deployment_id, request_id),
 FOREIGN KEY(deployment_id, request_id) REFERENCES relay_requests ON DELETE CASCADE
);
CREATE TABLE relay_lifecycle (
 deployment_id text NOT NULL, request_id text NOT NULL, action text NOT NULL CHECK(action IN ('park','wake')),
 done boolean NOT NULL DEFAULT false, claim_token uuid, claim_until timestamptz,
 next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(), attempts integer NOT NULL DEFAULT 0,
 last_error text, PRIMARY KEY(deployment_id, request_id, action),
 FOREIGN KEY(deployment_id, request_id) REFERENCES relay_requests ON DELETE CASCADE
);
CREATE INDEX relay_due ON relay_lifecycle(deployment_id, next_attempt_at) WHERE NOT done;
CREATE TABLE relay_workers (
 deployment_id text NOT NULL, rollout_id text NOT NULL, registration_token text NOT NULL,
 worker_id text NOT NULL, last_seen_at double precision NOT NULL, metadata jsonb NOT NULL,
 PRIMARY KEY(deployment_id, rollout_id, worker_id)
);
CREATE TABLE relay_imports (
 deployment_id text PRIMARY KEY, source_digest text NOT NULL, active boolean NOT NULL DEFAULT false,
 imported_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE relay_runtime_config (
 deployment_id text PRIMARY KEY, park_enabled boolean NOT NULL, wake_enabled boolean NOT NULL
);
