# API Reference

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
  "cpu_overcommit": 2,
  "memory_overcommit": 1.2,
  "disk_overcommit": 1.0,
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
  "nodes": {"fresh": 1, "sandbox": 1, "builder": 0, "samples": 24},
  "resources": {
    "sandbox": {
      "effective": {"vcpu": 16, "memory_mb": 32768, "disk_mb": 204800},
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
  "sandboxes": {"active_routes": 1, "pending": 0},
  "capacity": {
    "prepared": 1,
    "prepared_sandboxes": 16,
    "prepared_resources": {"vcpu": 16, "memory_mb": 32768, "disk_mb": 163840}
  },
  "images": {"pending_builds": 0},
  "builders": {"prepared": 1, "prepared_builders": 1},
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

Trace spans are written to the same metrics JSONL stream as autoscaler and
heartbeat events. Sandbox create traces cover gateway image resolution,
existing-route checks, node selection, image availability or pull, and node
create proxying. Image build traces cover builder selection, pending-build
enqueueing, builder proxying, and node-reported build timings. Node-agent
responses include `timings` fields for sandbox creation and image builds; image
build records also include Docker build/push phase durations. In particular,
the builder response separates request-body read, context materialization,
build wait, Docker build, and registry push. Sandbox responses separate the
gateway proxy from node-side request handling and Docker container creation.

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
The `service` value is `control-plane`, `node-agent`, `async-node-agent`, or
`model-relay` depending on which process serves the endpoint:

```json
{
  "ok": true,
  "service": "control-plane",
  "version": "<package-version>"
}
```

The gateway/control plane additionally exposes:

- `POST /v1/images/build` when started with
  `serve-control-plane --enable-image-builds`
- `GET /v1/capacity/prepare`
- `POST /v1/capacity/prepare`
- `DELETE /v1/capacity/prepare/<prepare-id>`
- `GET /v1/builders/prepare`
- `POST /v1/builders/prepare`
- `DELETE /v1/builders/prepare/<prepare-id>`

`POST /v1/capacity/prepare` accepts `count`, resource fields, `ttl_seconds`,
and optional `image`. The capacity signal is consumed by the autoscaler after
one reconciliation cycle. If `image` is supplied, the gateway also creates a
transient image warmup work item with the same prepare id and TTL. Warmup runs
in the background as sandbox nodes heartbeat, and completes once cached node
capacity can fit the requested sandbox count. The response includes
`image_warmup` when such work is registered and `image_prewarm` with scheduling
summary fields.

`POST /v1/images/pull` accepts `image`, optional `id`, `count`, resource
fields, and `sandbox_nodes_only` (default `true`). It pulls the image to up to
`count` ready image-cache nodes and returns per-node cache hits, pulls, and
failures.

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
  "network": "none",
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

At least one resource field (`cpus`, `memory_mb`, or `disk_mb`) is required.

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
host-like writable paths, installs a small `service` compatibility shim when the
image does not provide one, optionally starts cron/sshd when those binaries
exist in the image, and keeps the container alive when no command is supplied.
If no explicit `security` object is supplied, this profile uses root-compatible
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

SSH-enabled sandboxes must use `"network": "bridge"`. The node agent binds SSH
to localhost on the VM by default; external access should go through the
gateway/tunnel layer rather than exposing container SSH ports publicly.

Exec commands are session-based and async-capable. The compatibility node-agent
HTTP API records ordered stdout/stderr/status/exit events and accepts stdin
writes. The high-performance path uses the aiohttp async node agent with
WebSocket binary streaming and bounded per-session queues. See
[routing-gateway.md](routing-gateway.md).
