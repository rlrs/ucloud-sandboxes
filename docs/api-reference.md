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
heartbeats, route state, and the rolling metrics event log:

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
build records also include Docker build/push phase durations.

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
and optional `image`. The image is not a reservation; the gateway uses it to
opportunistically prewarm already-ready sandbox nodes.

`POST /v1/images/pull` accepts `image`, optional `id`, `count`, resource
fields, and `sandbox_nodes_only` (default `true`). It pulls the image to up to
`count` ready image-cache nodes and returns per-node cache hits, pulls, and
failures.

`POST /v1/sandboxes` accepts:

```json
{
  "id": "demo-1",
  "image": "busybox",
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
retry with the same `id` and spec returns the existing sandbox with status `200`
instead of running a second container. Reusing the same `id` with a different
image, resource request, command, environment, security profile, filesystem, or
labels is a conflict.

`GET /v1/sandboxes` returns records with stable top-level identity fields as
well as the full nested spec:

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
