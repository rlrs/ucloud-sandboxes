# Sandbox security stance

Date: 2026-06-29

The sandbox runtime is a Docker container launched with gVisor `runsc`. This is
defense-in-depth over regular Linux containers: sandbox workloads see gVisor's
userspace kernel instead of issuing most system calls directly to the host
kernel.

This is not equivalent to a full VM. gVisor intentionally trades some Linux
compatibility for a smaller host attack surface. Workloads that depend on kernel
subsystems, cgroups inside the sandbox, block device filesystems, low-level
networking, or exact `/proc` behavior may observe differences from a normal
machine.

## Current defaults

New sandboxes use the secure profile unless the request explicitly overrides
`security`:

```json
{
  "filesystem": {
    "enforce_disk_quota": false,
    "workspace_path": "/workspace",
    "tmpfs_mb": 64,
    "run_tmpfs_mb": 16
  },
  "security": {
    "user": "1000:1000",
    "cap_drop": ["ALL"],
    "cap_add": [],
    "no_new_privileges": true,
    "pids_limit": 256,
    "read_only_rootfs": false,
    "init": true
  }
}
```

The generated Docker command includes:

- `--runtime runsc`
- `--network none` by default
- `--user 1000:1000`
- `--security-opt no-new-privileges`
- `--cap-drop ALL`
- `--pids-limit 256`
- `--init`
- `--tmpfs /tmp:rw,nosuid,nodev,size=64m`
- `--tmpfs /run:rw,nosuid,nodev,size=16m`
- optional `--memory`
- optional `--cpus`
- optional `--storage-opt size=...` when `disk_mb` is set

`disk_mb` maps to Docker `--storage-opt size=...` for the container writable
layer. This is only a hard limit on nodes initialized with Docker `overlay2` on
an XFS data root mounted with project quotas, and only after the
`storage-opt-quota-enforced` conformance probe passes. On non-conforming nodes,
the Docker runtime refuses `disk_mb`, the node does not advertise `disk-quota`,
and the policy layer treats its free disk as zero.

The code also has a guarded tmpfs workspace mode. That mode is memory-backed, so
it is suitable for small ephemeral writable workspaces, not a general persistent
disk quota. The runtime refuses to use it unless the node runtime is explicitly
configured as having validated tmpfs workspace support. This is intentional
fail-closed behavior.

## Compatibility escape hatch

Some images, especially SSH daemon images, may still need root or selected
capabilities. That should be explicit in the sandbox request:

```json
{
  "security": {
    "user": "root",
    "cap_drop": [],
    "no_new_privileges": false,
    "pids_limit": 256,
    "init": true
  }
}
```

This is less isolated and should be used sparingly. Prefer building images that
run application code as a non-root user and expose SSH through a non-root-safe
configuration where possible.

## Linux Host Compatibility Profile

For benchmark tasks that expect a VM-like Linux host rather than a minimal task
container, the sandbox API supports an explicit `"linux_host"` profile. This
profile is still a Docker + gVisor container, but it changes startup behavior to
look more like a small Linux machine:

- root-compatible defaults when `security` is omitted
- larger `/tmp` and `/run` tmpfs mounts
- a shell PID-1 bootstrap under `/bin/sh`
- creation of common writable paths such as `/tests`, `/logs/verifier`,
  `/task`, `/oracle`, `/var/spool/cron`, `/run/sshd`, and `/workspace`
- a small `service` shim if the image has no `service` command
- optional cron startup when `linux_host.enable_cron` is true and cron exists in
  the image
- optional sshd startup when `linux_host.enable_sshd` is true or sandbox SSH is
  enabled and `sshd` exists in the image
- keep-alive behavior when the caller does not supply a command

This improves compatibility with TMax/Harbor-style setup scripts that touch
root-owned paths, use cron conventions, or expect `service` to exist. It does
not provide real `systemd`, kernel modules, nested container runtimes,
Singularity hooks, privileged mounts, or a true VM boot sequence. Images still
need to contain the packages they use at runtime, including `/bin/sh`, cron, or
OpenSSH server when those features are requested.

## Live VM observations

On UCloud VM job `12345813`, initialized on 2026-06-29:

- Initial Docker `29.6.1` setup used storage driver `overlayfs`, containerd
  snapshotter driver type
- `runsc release-20260622.0`, default platform `systrap`
- Docker daemon root was originally moved to `/work/ucloud-sandboxes/docker-xfs`,
  a sparse XFS image under `/work` mounted with `pquota`. Current node init uses
  local VM disk under `/var/lib/ucloud-sandboxes/docker-xfs` for Docker layer
  I/O, while the registry remains on persistent project storage.
- `--runtime runsc` containers report `4.19.0-gvisor`
- `--network none` blocks outbound network access
- `--memory 128m` is visible through `/proc/meminfo`
- `--cpus 0.5` does not make `nproc` report one CPU; CPU realism is limited
- root inside gVisor cannot mount a tmpfs with the default Docker capability set
- Docker `--storage-opt size=16m` did not stop a 32 MB write under either
  `runc` or `runsc`, so this is a Docker storage configuration issue rather
  than a gVisor-specific result
- After switching Docker to `overlay2` on the XFS project-quota data root and
  disabling the containerd snapshotter path, Docker reported `Backing
  Filesystem: xfs` and `--storage-opt size=16m` rejected a 32 MB write under
  `runsc` with `ENOSPC`
- Docker `--tmpfs /tmp:size=16m` rejected a 32 MB write with `ENOSPC` under
  both `runc` and `runsc`

The Docker storage finding means writable-layer disk quotas should use the
validated Docker `overlay2`/XFS project-quota configuration. Tmpfs can enforce
bounded ephemeral workspace size, but it consumes memory and is not a general
disk replacement. The runtime mounts `/tmp` and `/run` as explicit bounded
tmpfs mounts because the default image `/tmp` path may otherwise bypass the
Docker writable-layer quota.

## Conformance probe

Run this on each initialized VM node before trusting it for hostile workloads:

```bash
uv run ucloud-sandboxes runtime-conformance --sudo --execute --output json
```

The probe checks:

- gVisor kernel is visible in `uname`
- `network=none` blocks outbound traffic
- memory limit is visible
- mount attempts fail
- numeric non-root execution works
- Docker `--storage-opt size=16m` rejects a bounded 32 MB write
- Docker `--tmpfs /tmp:size=16m` rejects a bounded 32 MB write
- when initialized for live fork, a checkpoint restores initial-workload memory into a
  distinct runsc container whose in-sandbox network identity adopts its fresh
  Docker bridge address while the source remains runnable

VM init writes this probe result to
`/work/ucloud-sandboxes/state/runtime-conformance.json`. The node-agent and
periodic heartbeat derive `runtime-conformance` and `disk-quota` capabilities
from that file. The scheduler only credits node disk capacity when `disk-quota`
is present, and the node runtime rejects `disk_mb` when Docker storage quota
support has not been validated.

## Live-fork security contract

`fork-local-v1` is opt-in per sandbox with `forkable=true`, explicit memory and
writable-storage limits, and `fork_protocol.version=agent-v1`; it is advertised only after the
live checkpoint, writable-layer quota, and tmpfs quota probes all succeed. The
live result is bound to the current Docker server, runsc path/version, and socket
policy fingerprint, so changing that runtime configuration disables fork until
conformance is rerun. The resumable workload must be the initial
container process tree. gVisor intentionally kills every exec-origin thread
group during restore—including descendants detached after the `docker exec`
caller exits—because their external callers cannot be reconstructed. The node
therefore rejects a fork while a tracked exec session is active, and the live
probe verifies that a detached OriginExec descendant remains in the resumed
source but is absent from the restored child.

Checkpoint artifacts contain application memory and must be treated like live
credentials. They are stored mode `0700` beneath Docker's local XFS data root,
sealed with source container/image/spec identity, and removed after a completed
restore. A root-owned helper is the only component that stages them into Docker
metadata. A separate root-owned OCI wrapper accepts only the matching staged
marker and converts that child's ordinary start into raw `runsc restore`; it
does not accept a caller-supplied filesystem path. Both read root-owned
fixed-path configuration, reject symlinks and
path traversal, requires full Docker IDs and SHA-256 identities, and confines
copy/delete actions to the checkpoint and target-container directories. The
helper derives a conservative byte reservation from the mandatory memory,
writable-root/workspace, `/tmp`, and `/run` limits, includes every pending
reservation in admission, and refuses to start capture when the Docker
filesystem cannot cover the total. Seal also
rejects a checkpoint larger than its declared bound. Its locked garbage
collector removes only exact helper-generated temporary/trash names and never
age-deletes pending or sealed artifacts. The node-agent remains unprivileged;
its sudo rule names only this helper. Membership
in the Docker group is already root-equivalent and remains the larger host
trust boundary.

Before either node-agent starts serving, it compares the helper's validated
inventory with durable restore records. It removes only unreferenced sealed
artifacts, staged reflinks, and generation-scoped application directories. An
unreferenced *pending* artifact is treated as possible dockerd/runsc activity;
startup fails closed before performing any destructive reconciliation.

The generated `runsc` capture runtime and `runsc-restore` child runtime
explicitly set
`--allow-live-tcp-migration=false`, `--net-disconnect-ok=true`, and
`--allow-connected-on-save=false`, so external TCP and Unix-domain sockets are
disconnected at checkpoint. A fork
does not duplicate one authenticated connection across branches. However, all
ordinary process memory is cloned, including tokens, request state, random
generator state, and identity cached by the application. A fork-aware agent is
required to quiesce requests before save, observe `resume` versus `restore` via
`/proc/gvisor/checkpoint`, read fresh child identity from
`/proc/gvisor/spec_environ`, reconnect, obtain new credentials, and overwrite
inherited credentials before acknowledging the per-fork nonce. The node invokes
prepare/ready hooks with a configurable 1-60 second deadline that communicate
with PID 1 and does not mark the child running until the restore acknowledgment succeeds. A hook timeout or
failure leaves a durable `restoring` intent for exact replay.

`agent-v1` is a nonce-fenced, monotonic PID-1 state machine, not a best-effort
shell callback. For one nonce, `cancel` is terminal and must dominate a late
`prepare`; after acknowledging cancel, the workload must never enter the
quiesced state for that nonce. The node treats a local `docker exec` timeout as
ambiguous because terminating the client does not prove the daemon-side exec
stopped. It therefore retains the restore intent and artifact instead of
deleting state that a late hook could still reference.

## Node control authentication

Production deployments use a third, private credential for control-plane to
node-agent calls. It is not the public gateway token and not the heartbeat-post
token. When configured, sync and async node agents require
`Authorization: Bearer <node-control-token>` on every route except `/healthz`.
The gateway removes client `Authorization`, `Proxy-Authorization`, and
`X-UCloud-Sandbox-Token` headers before proxying and installs only this private
credential. Periodic local heartbeat reads, image pulls/warmups, and autoscaler
drain requests use the same protected channel. Empty configured token files are
fatal at service startup.
Both `NodeGatewayClient` and `AsyncNodeGatewayClient` accept the same optional
node-control token for their direct internal operations; authenticated node
requests reject redirects so the credential is not forwarded to another
origin. Their fork convenience methods deliberately target the public control
plane, which is the only component allowed to allocate route generations and
construct node-operation fences.

The current generated deployment uses one deployment-wide node-control token.
It prevents a bridge-network sandbox that learns the node address from invoking
privileged node APIs, but compromise of one node exposes the credential for all
nodes in that deployment. Per-node credentials or workload identity remain a
future isolation boundary.

## Remaining hardening

- Keep node agents private-network-only and enable the node-control credential;
  public access must go through the authenticated gateway.
- Treat Docker image builds as a separate trust boundary. `docker build` is not
  launched under gVisor by this codebase.
- Make conformance freshness part of node readiness, not just init-time state.
- Add egress policy before enabling `network=bridge` for general workloads.
- Add stronger image policy: allowed registries, digest pinning, and scanner or
  provenance checks.
- Make TTL cleanup run periodically in the node agent, not just opportunistically
  on API operations.
