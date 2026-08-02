# Architecture

The crash-safe generation, operation-id, inventory, and drain invariants are
specified in [Distributed sandbox state protocol](distributed-state-protocol.md).
Current implementation priorities are tracked in
[Production roadmap](roadmap.md).

## Runtime ownership

The target sandbox-node runtime is one privileged direct-runsc Warden per node.
It owns every sandbox task lifecycle on that node. Docker/containerd remain
image infrastructure and never own a sandbox task.

The direct runtime replaces the legacy Docker-owned task runtime. They are not
simultaneously active:

- a node deployment has one immutable runtime identity;
- the node agent refuses a state store created by another runtime identity;
- scheduling and routing never mix runtime identities within a node;
- qualification compares dedicated, drained legacy and direct nodes;
- rollback closes admission and drains a whole direct node on its matching
  binary; it never resumes a direct-runtime artifact through the legacy path;
- after production qualification and complete legacy drain, the legacy task
  runtime and its helpers are removed.

The required production-flow and removal gates are tracked in
[Direct runtime production qualification](direct-runtime-production-qualification.md).

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
- sandbox nodes: run already-built images and pull/cache gateway-resolved
  immutable registry references
- registry: durable image cache for common building blocks and custom images,
  typically the control-plane-managed registry backed by a UCloud mount

The gateway handles `POST /v1/images/build` locally when started with
`--enable-image-builds`; otherwise it routes builds to ready builder-only nodes
advertising `image-build`. If no builder is ready, it records pending image-build
demand signal so the autoscaler can create a builder VM. The executing
autoscaler consumes that signal after reacting; the image-build caller should
retry the build request once a builder is ready. Runners that know builds are
coming can also call `POST /v1/builders/prepare` to prewarm one or more builder
VMs before the build requests arrive. Managed build clients submit a stable
image id. The gateway allocates the internal registry tag, forces a durable
push, records the manifest digest, and resolves the image id to a worker-private
pull reference. Sandbox nodes do not receive builder-local Docker images.
Sandbox placement only considers nodes advertising the `sandbox` capability,
and builder nodes scale back to zero when pending image-build demand is gone,
prepared builder signals have been consumed, and the builder idle grace has
elapsed.

For a control-plane-managed registry, run `ucloud-sandbox-registry.service` on
the gateway VM, back `UCLOUD_REGISTRY_DATA_DIR` with persistent storage, and
initialize builder and sandbox VMs with the gateway's restart-stable private DNS
name as the insecure-registry endpoint when using private HTTP. The registry
hostname and port are deployment configuration and are never part of the
managed client contract. Use `ucloud-sandboxes registry-prune` plus the
installed GC timer to keep registry storage bounded.

## Resource Placement

Sandbox placement is resource-based. Each sandbox request can ask for its own
`cpus`, `memory_mb`, and `disk_mb`. The storage-native direct-runtime pool uses
exact `1.0` CPU, memory, and disk capacity factors. Parked sandboxes retain
their hard disk placement but release active CPU and memory; those resources
are checked again on wake and may cause migration. Node heartbeats report
actual CPU, memory, PSI, storage capacity, and storage-operation queueing.
Static requested resources remain hard admission constraints, while the
autoscaler's live feedback policy uses those observations to retain latency
headroom. The qualified homogeneous worker shape has 32 vCPU, 96 GiB RAM, and
a 2-TB attached disk split into bounded Docker image storage, swap, disposable
storage cache, and headroom before sandbox hard capacity is advertised.

## Disk Quotas

The storage-native direct runtime uses one fixed-size COW-backed filesystem per
sandbox incarnation. Its hard reservation covers the client-visible writable
capacity and required runtime state. Published immutable parents are durable
remote authority; bounded local cache bytes are disposable and are not
admission capacity. Logical reservation and physical/durable hard capacity
must agree before runsc creation.

The following describes the legacy removal target. `disk_mb` maps to Docker
`--storage-opt size=...` for the container
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

## Legacy local live fork

This section documents the Docker-owned implementation that will be removed.
Fork is explicitly deferred from the initial direct-runtime release and is not
a migration blocker. If reintroduced, it will use native Warden artifact
ownership rather than this Docker checkpoint/restore path.

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
   `create`, then substitutes raw
   `runsc restore --background --image-path=...` for OCI `start`. Because the
   checkpoint is uncompressed, runsc can return after restoring kernel
   metadata, load application and file pages in the background, and prioritize
   pages faulted by the resumed workload. The restored process tree therefore
   runs under the destination
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
child. Raw runsc restores return after background page loading starts, and the
node runs up to eight independent child restores concurrently and
returns results in request order. It stops scheduling queued children after a
failure while allowing already-running restores to settle into their durable
intents. Each child ultimately owns its restored memory.

Background restore changes time to first instruction, not total checkpoint
bytes: stock runsc still scans and saves the checkpointed memory image. A
successful OCI start means that asynchronous page loading has started, not
that every page has been read. The staged POSIX files may be unlinked after
start because runsc retains open descriptors; their blocks remain charged by
the filesystem until loading completes. A late page-load failure can terminate
the restored sandbox and is handled as an ordinary post-start runtime failure.

On an exact retry, Docker identity/running inspection uses the same bounded
worker pool under one wall-clock setup allowance. Nonce readiness checks are
also parallel and bounded. After the durable `running` commit, checkpoint
unstaging and artifact release are best-effort under one shared cleanup
deadline, so cleanup cannot hold the response open indefinitely.

The experimental local-hibernation format is separate from live fork. A
`parkable` sandbox is single-owner, cannot also be forkable or expose direct
SSH, and is admitted against requested writable disk plus its complete
regular-file memory backing and allocator slack. Its strict manifest,
authority journal, crash transitions, and fail-closed capability contract are
defined in [hibernation-artifact-v1.md](hibernation-artifact-v1.md).

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
Cross-node restore and shared copy-on-write process memory are separate future
runtime features.
