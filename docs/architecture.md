# Architecture

The crash-safe generation, operation-id, inventory, and drain invariants are
specified in [Distributed sandbox state protocol](distributed-state-protocol.md).

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
and large sandboxes differently. The live sandbox pool is configured for
`--init-cpu-overcommit 3` and `--init-memory-overcommit 1.5`, with disk left at
`1.0`. These multipliers affect placement only. Each container keeps its
requested CPU and memory cgroup limits, and standard workers currently have no
swap, so simultaneous resident use above physical RAM can trigger host OOM
kills.

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

## Local live fork

Forkable sandboxes use gVisor checkpoint/restore rather than the existing
Docker-image `snapshot` operation. The first production scope is node-local:

Every forkable source has explicit memory and writable-storage limits. Before
quiescing it, the node reserves enough local Docker storage for resident
memory, the bounded writable root/workspace, and both tmpfs mounts. A source is
placed only on a node advertising `fork-local-v1`; direct node admission
enforces the same rule.

1. The gateway validates the source generation and reserves destination
   generations on the same node.
2. The node persists a `creation_kind=restore` intent before runtime work, so a
   crash replay cannot accidentally start the child from its image entrypoint.
3. A bounded workload `prepare` hook quiesces the initial process tree and
   confirms it is waiting on `/proc/gvisor/checkpoint`.
4. Docker checkpoints the source through its `io.containerd.runc.v2`/`runsc`
   compatibility path with `--leave-running`. Current runsc checkpoints are
   uncompressed by default.
5. A root-owned, narrowly scoped helper records Docker completion, then seals
   the artifact under
   `DockerRootDir/ucloud-checkpoints`. It only accepts validated IDs and can
   reflink-copy a sealed checkpoint into the new Docker container's private
   checkpoint directory.
6. Docker/containerd perform an ordinary start of each already-created
   destination using the root-owned `runsc-restore` OCI runtime wrapper. The
   wrapper durably binds the child ID to its helper-staged image during OCI
   `create`, then substitutes raw `runsc restore --image-path=...` for OCI
   `start`. The restored process tree therefore runs under the destination
   container ID and Docker's fresh cgroup/network identity without using
   Docker's unsupported cross-container checkpoint restore API.
7. The node keeps every child in `restoring` until its bounded workload hook
   acknowledges the persisted nonce after identity rotation and reconnection.

The helper and wrapper split privilege narrowly. The helper validates, seals,
accounts, and reflink-stages checkpoint trees. The wrapper accepts only a
root-owned helper marker and an OCI annotation emitted by the node agent; it
cannot choose arbitrary paths. Docker/containerd still own container metadata,
rootfs, cgroup, network, exec, log, and delete lifecycle. XFS reflinks avoid a
second physical copy of the memory image during staging. A fan-out request
captures the source once and stages the same immutable instant into every
child. Raw runsc restores are synchronous,
but the node runs up to eight independent child restores concurrently and
returns results in request order. It stops scheduling queued children after a
failure while allowing already-running restores to settle into their durable
intents. Each child ultimately owns its restored memory.

On an exact retry, Docker identity/running inspection uses the same bounded
worker pool under one wall-clock setup allowance. Nonce readiness checks are
also parallel and bounded. After the durable `running` commit, checkpoint
unstaging and artifact release are best-effort under one shared cleanup
deadline, so cleanup cannot hold the response open indefinitely.

Fork operations take an exclusive per-sandbox lifecycle lease. Exec and file
operations hold shared leases, so checkpoint cannot race an attached exec,
delete, or file mutation. The destination intent and immutable checkpoint ID
are generation-fenced and replayable. A child observed running with the exact
generation labels must still pass the workload readiness hook after a
node-agent crash; a stopped or partial child is removed and recreated from the
same sealed checkpoint.

Node startup performs a mark-and-sweep against the durable sandbox store. It
reclaims only sealed/staged/application state proven unreferenced. Pending
checkpoint state without a matching restore intent is never guessed safe and
prevents the node from serving until an operator establishes that no runtime
writer remains.

The PID-1 protocol is monotonic per nonce: a cancel acknowledgment permanently
fences any late prepare callback. Host-side hook or Docker timeouts are
ambiguous, so the node leaves the durable intent quarantined and never assumes
that killing a local CLI also stopped work owned by dockerd.

The capability is fail-closed. VM init enables Docker's experimental checkpoint
capture API, installs the privileged helper and root-owned restore wrapper, and
runs the `gvisor-live-fork-v1` probe.
Only a node that restores initial-workload memory into a distinct container,
exposes the child's new spec identity and in-sandbox adoption of its Docker
bridge address, tears down a live socket, excludes a detached OriginExec
descendant from the child, and can
checkpoint the resumed source again advertises `fork-local-v1`.
Cross-node restore, shared copy-on-write process memory, and background page
loading are separate future runtime features.
