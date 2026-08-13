# API Reference

## Authentication

The public SDK API accepts a deployment-generated sandbox API key in either
of these headers:

```text
X-UCloud-Sandbox-Token: <sandbox-api-key>
Authorization: Bearer <sandbox-api-key>
```

The Python SDK uses `X-UCloud-Sandbox-Token`, which also works through UCloud
public links. The sandbox API key authorizes sandbox create/list/delete, exec,
jobs, file transfer, snapshots, image operations, and prepared-capacity
operations. It cannot read nodes, demand, metrics, or registry status and
cannot invoke explicit park, wake, detach, or migration operations.

The gateway control token is a separate operator credential with access to the
complete control-plane API. Heartbeat and node-control tokens are separate
again and are never valid public SDK credentials. `/healthz` is intentionally
unauthenticated and contains no deployment secrets.

## Heartbeat API

`POST /v1/nodes/heartbeat` accepts:

```json
{
  "node_id": "ubuntu-8263",
  "job_id": "12345311",
  "updated_at": "2026-06-28T09:43:15+00:00",
  "active_sandboxes": 1,
  "draining": false,
  "node_url": "http://sandbox-node-12345317:8090",
  "capabilities": ["sandbox", "image-cache"],
  "total_resources": {
    "vcpu": 16,
    "memory_mb": 32768,
    "disk_mb": 500000
  },
  "used_resources": {
    "vcpu": 2.5,
    "memory_mb": 4096,
    "disk_mb": 20000
  },
  "labels": {
    "pool": "default"
  }
}
```

`GET /v1/nodes` returns the stored heartbeat list.

`GET /v1/metrics` returns a dashboard-oriented snapshot derived from
heartbeats, route state, and the rolling metrics event log. The default response
uses a bounded recent event window and a short-lived cached registry summary so
dashboard polling stays cheap. Use `GET /v1/metrics?full=true` for the larger
event window and a fresh registry scan, or
`GET /v1/metrics?refresh_registry=true` when only the registry summary must be
refreshed.

```json
{
  "nodes": {
    "fresh": 1,
    "sandbox": 1,
    "sandbox_ready": 1,
    "sandbox_draining": 0,
    "sandbox_admission_closed": 0,
    "builder": 0,
    "samples": 24
  },
  "resources": {
    "sandbox": {
      "total": {"vcpu": 16, "memory_mb": 32768, "disk_mb": 204800},
      "used": {"vcpu": 1, "memory_mb": 512, "disk_mb": 1024},
      "load": {"vcpu": 0.0625, "memory": 0.015625, "disk": 0.005},
      "actual_usage": {
        "cpu_vcpu": 0.8,
        "cpu_percent_avg": 5.0,
        "memory_used_mb": 3072,
        "memory_percent": 9.375
      }
    }
  },
  "sandboxes": {
    "active_routes": 1,
    "states": {"running": 1},
    "pending": 0
  },
  "capacity": {
    "prepared": 1,
    "prepared_sandboxes": 16,
    "prepared_resources": {"vcpu": 16, "memory_mb": 32768, "disk_mb": 163840}
  },
  "images": {"pending_builds": 0},
  "builders": {"prepared": 1, "prepared_builders": 1},
  "programs": {
    "requests": 2,
    "states": {"model_wait": 1, "ready_to_wake": 1, "waking": 0, "acting": 0},
    "oldest_ready_to_wake_seconds": 3,
    "response_to_wake_p95_ms": 240
  },
  "autoscaler": {
    "actions": [],
    "reasons": ["capacity is sufficient"],
    "program_wake_plan": {
      "queued": 1,
      "placed": 1,
      "unplaced_count": 0,
      "placements": [{"request_id": "request-1", "node_id": "node-1"}],
      "placements_truncated": 0,
      "unplaced_truncated": 0
    },
    "effective_policy": {
      "program_aware_autoscaling_enabled": false,
      "target_cpu_utilization": 0.75,
      "target_memory_utilization": 0.8
    }
  },
  "scale_up": {"samples": 1, "last_ms": 391000, "p95_ms": 391000},
  "traces": {
    "span_count": 42,
    "recent": [
      {
        "trace_id": "sandbox-create-demo-1-abc123",
        "name": "gateway.sandbox_create",
        "status": "ok",
        "duration_ms": 812,
        "span_count": 5
      }
    ]
  },
  "vm_lifecycle": {
    "items": [
      {
        "job_id": "12347064",
        "role": "sandbox",
        "state": "RUNNING",
        "submit_to_running_ms": 27145,
        "ucloud_created_to_running_ms": 26900,
        "running_to_first_init_attempt_ms": 11800,
        "last_successful_package_stage_ms": 2400,
        "last_successful_remote_init_ms": 63000,
        "first_init_attempt_to_first_heartbeat_ms": 65514,
        "running_to_first_heartbeat_ms": 101714,
        "first_heartbeat_to_first_sandbox_ms": 11601,
        "last_successful_init_duration_ms": 66000
      }
    ]
  }
}
```

The compact response keeps at most 100 wake placements and 100 unplaced wake
samples from the latest autoscaler cycle and reports omitted counts. This
prevents a model-return burst from making every dashboard poll proportional to
the entire wake queue. `effective_policy` contains only non-secret operational
knobs and is observational; `/v1/metrics` does not provide a policy mutation
path.

`telemetry` reports the bounded OTLP exporter queue and accepted, exported,
dropped, and failed span counts. Trace payloads are not stored in this response
or in the metrics SQLite database. Sandbox, build, park/wake, relay, storage,
provider, and VM-bootstrap traces are exported to the configured OTLP backend;
see [Performance telemetry](telemetry.md). Node-agent responses retain bounded
`timings` fields because they are also useful to clients during a single
operation.

The lifecycle boundaries have deliberately narrow meanings:

- `ucloud_created_to_running_ms` is the provider-side VM wait visible from
  UCloud timestamps.
- `running_to_first_init_attempt_ms` ends when the controller first starts an
  SSH bootstrap attempt. It includes SSH announcement and autoscaler polling
  delay because UCloud does not expose a separate SSH-ready timestamp.
- `last_successful_package_stage_ms` and
  `last_successful_remote_init_ms` split package transfer from the remote init
  script.
- `first_init_attempt_to_first_heartbeat_ms` measures from bootstrap start to
  node registration/readiness. The heartbeat can arrive just before the SSH
  init command exits, so this boundary does not assume a strictly sequential
  init-then-registration process. `first_heartbeat_to_first_sandbox_ms` then
  measures time to the first sandbox placement on that node.

Two cold-path intervals are not yet exact. SDK context preparation and the
client-to-gateway upload happen before the current image-build trace begins.
Also, a build rejected while no builder is ready is retried as a new HTTP
request, so the service exposes current pending-build age but cannot correlate
that age with the eventual successful build as a single queue-wait sample.
Pending build demand is cleared as soon as a builder accepts the asynchronous
build; the builder heartbeat's active-build count then owns liveness. This
prevents completed or already-running builds from causing replacement capacity
after an unrelated node termination.

## Node Agent API

The VM-side node agent exposes:

- `GET /healthz`
- `GET /v1/heartbeat`
- `GET /v1/images`
- `POST /v1/images/pull`
- `GET /v1/sandboxes`
- `POST /v1/sandboxes`
- `DELETE /v1/sandboxes/<sandbox-id>`
- `PUT /v1/sandboxes/<sandbox-id>/files?path=<absolute-container-path>`
- `GET /v1/sandboxes/<sandbox-id>/files?path=<absolute-container-path>`
- `GET /v1/sandboxes/<sandbox-id>/ssh`
- `POST /v1/sandboxes/<sandbox-id>/exec`
- `GET /v1/exec/<session-id>`
- `GET /v1/exec/<session-id>/events`
- `POST /v1/exec/<session-id>/stdin`
- `POST /v1/exec/<session-id>/close-stdin`
- `POST /v1/sandboxes/<sandbox-id>/snapshot` (requires `--enable-image-builds`)

`GET /healthz` is public and returns the service identity and package version.
The `service` value identifies the `control-plane`, `node-agent`,
`builder-agent`, or `model-relay` process serving the endpoint:

```json
{
  "ok": true,
  "service": "control-plane",
  "version": "<package-version>"
}
```

The gateway/control plane additionally exposes:

- `PUT /v1/image-contexts/sha256:<digest>`
- `POST /v1/images/build` when started with
  `serve-control-plane --enable-image-builds`
- `GET /v1/capacity/prepare`
- `POST /v1/capacity/prepare`
- `DELETE /v1/capacity/prepare/<prepare-id>`
- `GET /v1/builders/prepare`
- `POST /v1/builders/prepare`
- `DELETE /v1/builders/prepare/<prepare-id>`
- `POST /v1/sandboxes/<sandbox-id>/migration`
- `DELETE /v1/sandboxes/<sandbox-id>/migration?migration_id=<id>`

Build clients upload a deterministic `tar.gz` context to
`PUT /v1/image-contexts/sha256:<digest>` with `Content-Type:
application/gzip` and `Content-Length`, then submit the small build JSON with
`context_archive_digest`, `context_archive_size`, and
`context_archive_format: "tar.gz"`. The gateway verifies and stores the blob,
so it survives a no-builder retry, and streams it to the selected builder only
when absent there. Stores are bounded and content-addressed; temporary extracted
directories are removed after the tracked build.

Managed `POST /v1/images/build` requests provide `id`, the context reference,
and optional Dockerfile/build arguments, but omit `tag`. The gateway generates
the worker-private registry tag and forces `push=true`; clients later create a
sandbox using the same image id. An explicit `tag` is still accepted for
external or advanced registry flows. Private registry DNS names and ports are
deployment details, not client configuration.

`POST /v1/capacity/prepare` accepts `count` from 1 through 100, resource fields,
`ttl_seconds`, and optional `image` and `parkable`. Set `parkable: true` when the future
sandboxes will be parkable; the gateway then expands the caller's writable
`disk_mb` into the same hard checkpoint reservation used during sandbox
creation, so each sandbox can claim its prepared unit exactly. Each prepared
unit remains until a matching sandbox allocation atomically claims it or its
TTL expires; provider acceptance alone does not consume it. If `image` is
supplied, the gateway also creates a
transient image warmup work item with the same prepare id and TTL. Warmup runs
in the background as sandbox nodes heartbeat, and completes once cached node
capacity can fit the requested sandbox count. The response includes
`image_warmup` when such work is registered and `image_prewarm` with scheduling
summary fields.

`POST /v1/images/pull` accepts `image`, optional `id`, `count`, resource
fields, and `sandbox_nodes_only` (default `true`). It pulls the image to up to
`count` ready image-cache nodes and returns per-node cache hits, pulls, and
failures.

## Sandbox creation

`POST /v1/sandboxes` accepts:

```json
{
  "id": "demo-1",
  "image": "busybox",
  "profile": "container",
  "command": ["sh", "-lc", "echo ok"],
  "env": {
    "REQUEST_ID": "req-1"
  },
  "memory_mb": 128,
  "cpus": 1,
  "disk_mb": 1024,
  "filesystem": {
    "enforce_disk_quota": false,
    "workspace_path": "/workspace"
  },
  "network": "bridge",
  "ttl_seconds": 600,
  "ssh": {
    "enabled": true,
    "user": "sandbox",
    "host_port": 22000,
    "container_port": 22,
    "authorized_keys": ["ssh-ed25519 AAAA... user@example"]
  },
  "labels": {
    "tenant": "example"
  }
}
```


## Sandbox creation, profiles, and listing

At least one resource field (`cpus`, `memory_mb`, or `disk_mb`) is required for
ordinary sandbox creation.

Sandbox creation is idempotent for a supplied `id` and matching normalized spec.
If a client times out while the node is still creating the Docker container, a
retry with the same `id` and spec either returns the existing sandbox with status
`200` or returns a retryable `503` while the original create is still unresolved.
The sandbox `id` is the idempotency key; there is no separate idempotency header
or field. Reusing the same `id` with a different image, resource request,
command, environment, security profile, filesystem, or labels is a conflict.

The default `profile` is `"container"`, which keeps the hardened gVisor
container defaults. For benchmark images that assume a more VM-like Linux host,
use the explicit `"linux_host"` profile:

```json
{
  "id": "host-like-1",
  "image": "ubuntu:24.04",
  "profile": "linux_host",
  "memory_mb": 1024,
  "cpus": 1,
  "disk_mb": 4096,
  "network": "bridge",
  "linux_host": {
    "enable_cron": true,
    "enable_sshd": false,
    "keep_alive": true,
    "writable_paths": ["/tests", "/logs/verifier", "/task", "/oracle"]
  }
}
```

`linux_host` starts the container through a shell bootstrap that prepares common
host-like writable paths, installs a small `service` command shim when the
image does not provide one, optionally starts cron/sshd when those binaries
exist in the image, and keeps the container alive when no command is supplied.
If no explicit `security` object is supplied, this profile uses root-oriented
defaults rather than the hardened non-root defaults. It still runs under gVisor;
it is not equivalent to a real VM or full `systemd` boot.

`GET /v1/sandboxes` is a cheap cached read of the gateway routing table. It
returns records with stable top-level identity fields as well as the full nested
spec captured at create/reconcile time:

```json
{
  "sandboxes": [
    {
      "id": "demo-1",
      "sandbox_id": "demo-1",
      "name": "ucloud-sandbox-demo-1",
      "image": "busybox",
      "labels": {"tenant": "example"},
      "spec": {"id": "demo-1", "image": "busybox"},
      "state": "running"
    }
  ]
}
```

The response includes `"cached": true` at the top level. Cached records expose
`cached_state`, `route_only`, route timestamps, and node freshness metadata so
clients can distinguish a fresh running route from a stale route that has not
been reconciled. Use `GET /v1/sandboxes?refresh=true` only when a caller
intentionally wants the gateway to fan out to sandbox nodes, reconcile node
state, and return `"cached": false`.

When the gateway receives a non-JSON error response from an upstream node, such
as an HTML `503 Job is unavailable` page, it returns structured JSON with the
original status, `retryable`, upstream content type, and a short body preview.

## Park, wake, and migration

`POST /v1/sandboxes/<sandbox-id>/park` and
`POST /v1/sandboxes/<sandbox-id>/wake` carry the sandbox generation and a stable
operation id. The default foreground park releases compute and seals local
storage without putting registry publication on its latency-critical path. Wake
uses that attached route when the current node has active capacity.

`POST /v1/sandboxes/<sandbox-id>/detach` accepts an empty JSON object. It is the
durability boundary used by node scale-down: the gateway synchronously publishes
an unpublished park, validates and persists its exact `storage-native-v1`
descriptor and Registry reference, then evicts worker-local ownership. The
operation is idempotent. Ambiguous eviction returns a retryable `503` and leaves
the route `detaching`; it never reports freed storage without a successful retry
or a fresh complete heartbeat proving the incarnation absent. Wake of a fully
detached park selects a fitting worker, imports the durable descriptor, and
activates it there.

`POST /v1/sandboxes/<sandbox-id>/migration` moves a parked sandbox between
nodes that both advertise `storage-native-v1` and
`sandbox-migrate-storage-native-v1`. The optional JSON fields are
`migration_id` for idempotent retries and `destination_node_id` to require a
specific ready destination. The response returns the migration journal and the
current sandbox route. A retryable `503` retains the journal phase for the next
identical request.

`DELETE /v1/sandboxes/<sandbox-id>/migration?migration_id=<id>` aborts only
before the atomic route switch. Once routing has committed, retry `POST` until
source finalization completes.

SSH-enabled sandboxes must use `"network": "bridge"`. The node agent binds SSH
to localhost on the VM by default; external access should go through the
gateway/tunnel layer rather than exposing container SSH ports publicly.

Exec commands are session-based. The node records ordered
stdout/stderr/status/exit events, accepts stdin writes, and supports bounded
long-poll reads. See [routing-gateway.md](routing-gateway.md).
