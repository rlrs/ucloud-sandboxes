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
- `POST /v1/sandboxes/<sandbox-id>/forks` (requires `fork-local-v1`)
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

- `PUT /v1/image-contexts/sha256:<digest>`
- `POST /v1/images/build` when started with
  `serve-control-plane --enable-image-builds`
- `GET /v1/capacity/prepare`
- `POST /v1/capacity/prepare`
- `DELETE /v1/capacity/prepare/<prepare-id>`
- `GET /v1/builders/prepare`
- `POST /v1/builders/prepare`
- `DELETE /v1/builders/prepare/<prepare-id>`

Build clients upload a deterministic `tar.gz` context to
`PUT /v1/image-contexts/sha256:<digest>` with `Content-Type:
application/gzip` and `Content-Length`, then submit the small build JSON with
`context_archive_digest`, `context_archive_size`, and
`context_archive_format: "tar.gz"`. The gateway verifies and stores the blob,
so it survives a no-builder retry, and streams it to the selected builder only
when absent there. Stores are bounded and content-addressed; temporary extracted
directories are removed after the tracked build. The legacy
`context_archive_base64` build field remains accepted for older SDKs.

`POST /v1/capacity/prepare` accepts `count`, resource fields, `ttl_seconds`,
and optional `image`. Each prepared unit remains until a matching sandbox
allocation atomically claims it or its TTL expires; provider acceptance alone
does not consume it. If `image` is supplied, the gateway also creates a
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
  "forkable": true,
  "fork_protocol": {
    "version": "agent-v1",
    "prepare_command": ["/usr/local/bin/ucloud-fork-agent", "prepare"],
    "ready_command": ["/usr/local/bin/ucloud-fork-agent", "ready"],
    "timeout_seconds": 30
  },
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

## Live sandbox fork

`POST /v1/sandboxes/<source-id>/forks` creates a distinct sandbox by restoring
the source's live gVisor process and memory state. The gateway always pins the
operation to the source node and reserves the child's resources there before
checkpointing. A minimal request supplies the new ID and optional restore-time
environment, labels, TTL, CPU, or memory overrides:

```json
{
  "sandbox": {
    "id": "agent-child-1",
    "env": {
      "AGENT_BRANCH": "child-1"
    },
    "ttl_seconds": 900
  }
}
```

The source must have explicit `memory_mb` and `disk_mb` limits,
`"forkable": true`, and a versioned `fork_protocol`; the derived child retains
that protocol. Image,
command/entrypoint, working directory, user,
capabilities, mount layout, network mode, filesystem configuration, and disk
size cannot change because runsc validates those fields against the checkpoint.
The resumable agent must be in the container's initial process tree. Processes
started through the `/exec` API are deliberately not part of the resumed child,
and an active exec session causes the fork to return a conflict rather than
silently losing the session.

The node appends `<checkpoint-id> <64-hex-nonce> <role>` to each hook command.
Before saving it invokes the source `prepare_command` with role `prepare`. That
hook must synchronously tell the initial process tree to stop accepting work,
drain in-flight side effects, and open `/proc/gvisor/checkpoint`. After saving,
the source `ready_command` acknowledges role `resume`. After restore, the child
`ready_command` must query PID 1 and only acknowledge role `restore` after PID 1
read its new identity from `/proc/gvisor/spec_environ`, discarded inherited
connections and credentials, and rekeyed. Hooks have a configurable 1-60 second
deadline (30 seconds by default). The child remains `restoring` and its
checkpoint remains replayable until acknowledgment.
The prepare hook must print `UCLOUD_FORK_PREPARED=<nonce>`; ready hooks must
print `UCLOUD_FORK_READY=<nonce>:<role>`. If a known failure happens before a
checkpoint is taken, the node invokes the ready hook with role `cancel` so PID 1
can discard the pending request and resume. Other output is ignored.

On success the response contains the normal destination `sandbox` record and a
fork result:

```json
{
  "intent_persisted": true,
  "sandbox": {
    "id": "agent-child-1",
    "state": "running",
    "creation_kind": "restore",
    "source_sandbox_id": "agent-parent",
    "source_generation": 3,
    "checkpoint_id": "fork-..."
  },
  "fork": {
    "checkpoint_id": "fork-...",
    "restored": true,
    "commands": []
  }
}
```

For fast same-instant fan-out, send up to 64 child overlays in `sandboxes` on
the same endpoint:

```json
{
  "sandboxes": [
    {"id": "agent-child-1", "env": {"AGENT_BRANCH": "child-1"}},
    {"id": "agent-child-2", "env": {"AGENT_BRANCH": "child-2"}}
  ]
}
```

The gateway atomically reserves every child on the source node before calling
the node. The node durably records every restore intent, takes one checkpoint,
and restores every child from that immutable artifact with up to eight restores
in flight. Results remain in request order. After a restore failure, no new
queued child starts; already-running children finish into their durable intents
for exact replay. A successful batch
returns parallel `sandboxes` and `forks` arrays in request order; each fork
entry includes its `sandbox_id`, and every entry has the same `checkpoint_id`.
The top-level `intent_persisted` applies to the whole atomic intent set. Exact
replay returns `200`; a new batch returns `201`.
The gateway's 55-minute request deadline is derived from the maximum 64-child
fan-out, eight restore workers, bounded checkpoint/restore commands, parallel
retry inspection/readiness, cleanup overhead, and an explicit proxy and
serialization margin.

The public control-plane request accepts only the overlay fields shown above.
It rejects caller-supplied `_ucloud_*` fields and mixed `sandbox`, `target`, and
`sandboxes` shapes. The VM node endpoint is gateway-internal: it requires the
complete normalized child specs plus `_ucloud_source` and one fenced
`_ucloud_operation` per child. The Python gateway clients likewise target the
public control plane for fork calls; they do not construct node fencing data.

On an error, `intent_persisted: true` means every requested intent is durable,
`false` means none is durable, and an absent value means the aggregate is
ambiguous. Batch errors additionally return `intents` entries with per-sandbox
`true`, `false`, or `null` state so the gateway can release only children proven
not to exist. Clients should replay the identical request for durable or
ambiguous children. Keep the source sandbox running until the fork response has
been acknowledged; if it disappears earlier, the node still reports any exact
child intents it can prove, but it cannot take a new checkpoint. The gateway
rejects an expanded internal request before reserving capacity when duplicating
the source spec across the batch would exceed the node JSON-body limit.

Forking is local-only in `fork-local-v1`. The checkpoint is uncompressed,
sealed as an immutable operation artifact, and reflink-staged on the node's XFS
Docker volume before Docker/containerd create the child with the dedicated
`runsc-restore` OCI runtime. Its root-owned wrapper substitutes raw
`runsc restore` for the child's ordinary start. Nodes advertise the capability
only after writable-layer and
tmpfs quota probes pass and a runtime-fingerprinted live probe restores an
initial-workload in-memory sentinel into a distinct container, verifies restore-time
identity through `/proc/gvisor/spec_environ`, adopts its distinct Docker bridge
address inside the restored sandbox, proves an established socket is
disconnected, and confirms the source can be checkpointed again.

External TCP and Unix-domain connections are disconnected at checkpoint; they
are not duplicated into both branches. The restore-time spec environment
contains the child's fresh `UCLOUD_SANDBOX_ID`, `UCLOUD_SANDBOX_FORK_PARENT`,
checkpoint identity, and readiness nonce. Values already held in process
memory, including credentials, remain inherited; the mandatory ready handshake
is where the agent replaces and clears them.

Thread groups originating from `docker exec` are source-only. This includes
background descendants that outlive a completed exec request: gVisor retains
them in the resumed source and excludes them from the restored child.

## Sandbox creation, profiles, and listing

At least one resource field (`cpus`, `memory_mb`, or `disk_mb`) is required for
ordinary sandbox creation. Fork overlays inherit the source resource limits.

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
