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

VM init writes this probe result to
`/work/ucloud-sandboxes/state/runtime-conformance.json`. The node-agent and
periodic heartbeat derive `runtime-conformance` and `disk-quota` capabilities
from that file. The scheduler only credits node disk capacity when `disk-quota`
is present, and the node runtime rejects `disk_mb` when Docker storage quota
support has not been validated.

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
node-control token for direct internal client use; authenticated node requests
reject redirects so the credential is not forwarded to another origin.

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
