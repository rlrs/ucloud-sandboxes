# Architecture

- UCloud VM jobs are pool nodes.
- The control plane and VM nodes should be attached to the same UCloud private
  network. VM jobs get a stable private-network hostname, and node heartbeats
  advertise the node-agent URL based on that hostname.
- A node agent on each VM reports resources, active sandbox count, capabilities,
  and drain state.
- The autoscaler reconciles pending sandbox resource demand against UCloud VM
  job state and node heartbeats.
- A later gateway/router should keep client routing stable and forward traffic
  to sandboxes through registered VM nodes.
- Mutating UCloud operations are gated behind explicit `--execute` flags.

See [vm-init.md](vm-init.md) for the current live-API findings and
recommended post-boot VM init strategy.

## Image Build And Registry Flow

Image builds should not run on sandbox nodes. The intended model is:

- control plane/gateway: scheduler, routing, and optionally Docker builds on a
  sufficiently large machine; can also host the private registry service
- builder nodes: autoscaled, builder-only VMs for Docker builds and registry
  push; they advertise physical capacity and do not use sandbox overcommit
- sandbox nodes: run already-built images and pull/cache registry tags
- registry: durable image cache for common building blocks and custom images,
  typically the control-plane-managed registry backed by a UCloud mount

The gateway handles `POST /v1/images/build` locally when started with
`--enable-image-builds`; otherwise it routes builds to ready builder-only nodes
advertising `image-build`. If no builder is ready, it records pending image-build
demand signal so the autoscaler can create a builder VM. The executing
autoscaler consumes that signal after reacting; the image-build caller should
retry the build request once a builder is ready. Runners that know builds are
coming can also call `POST /v1/builders/prepare` to prewarm one or more builder
VMs before the build requests arrive. Built tags should use `"push": true` and a
registry tag; sandbox nodes do not receive builder-local Docker images and
instead pull registry tags before creating containers. Sandbox placement only
considers nodes advertising the `sandbox` capability, and builder nodes scale
back to zero when pending image-build demand is gone, prepared builder signals
have been consumed, and the builder idle grace has elapsed.

For a control-plane-managed registry, run `ucloud-sandbox-registry.service` on
the gateway VM, back `UCLOUD_REGISTRY_DATA_DIR` with persistent storage, and
initialize builder and sandbox VMs with
`--init-docker-insecure-registry ucloud-sandbox-registry:5000` and
`--init-host-alias ucloud-sandbox-registry=<gateway-private-ip>` when using
private HTTP. Use `ucloud-sandboxes registry-prune` plus the installed GC timer
to keep registry storage bounded.

## Resource Placement

Sandbox placement is resource-based. Each sandbox request can ask for its own
`cpus`, `memory_mb`, and `disk_mb`. Nodes report physical resources plus
overcommit multipliers in their heartbeat, so the control plane can pack small
and large sandboxes differently. The live sandbox pool currently uses
`--init-cpu-overcommit 2` and `--init-memory-overcommit 1.2`, with disk left at
`1.0`. CPU overcommit is the main packing lever; memory overcommit should stay
modest until pressure and failure behavior are measured under mixed workloads.

## Disk Quotas

By default `disk_mb` maps to Docker `--storage-opt size=...` for the container
writable layer. VM init creates a sparse XFS image under `/work`, mounts it with
project quotas, configures Docker to use `overlay2` on that data root, and
disables the containerd snapshotter path that did not honor this quota. A node
only advertises `disk-quota` after `ucloud-sandboxes runtime-conformance
--execute` reports `storage-opt-quota-enforced: ok`; the scheduler credits disk
capacity only for nodes with that capability, and the node runtime rejects
`disk_mb` when quota support has not been validated. If
`filesystem.enforce_disk_quota` is true, the runtime uses a read-only root plus
a bounded tmpfs workspace at `filesystem.workspace_path`; that stricter mode is
only enabled when the `tmpfs-quota-enforced` probe passed. Sandboxes also get
explicit bounded `/tmp` and `/run` tmpfs mounts so common temporary writes do
not bypass writable-layer quota as an unbounded runtime default.
