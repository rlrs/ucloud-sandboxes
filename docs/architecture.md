# Architecture

The crash-safe generation, operation-id, inventory, and drain invariants are
specified in [Distributed sandbox state protocol](distributed-state-protocol.md).

## Runtime ownership

The sandbox-node runtime is one privileged direct-runsc Warden per node.
It owns every sandbox task lifecycle on that node. Docker/containerd remain
image infrastructure and never own a sandbox task.

Each node deployment has one immutable runtime identity. Admission, routing,
heartbeats, and storage authority all refer to that same Warden-owned runtime.

- UCloud VM jobs are pool nodes.
- The control plane and VM nodes should be attached to the same UCloud private
  network. VM jobs get a stable private-network hostname, and node heartbeats
  advertise the node-agent URL based on that hostname.
- A node agent on each VM reports resources, active sandbox count, capabilities,
  and drain state.
- The autoscaler reconciles pending sandbox resource demand against UCloud VM
  job state and node heartbeats.
- The gateway keeps client routing stable and forwards traffic through the
  node that owns the route's exact sandbox generation.
- Mutating UCloud operations are gated behind explicit `--execute` flags.

See [vm-init.md](vm-init.md) for the verified post-boot VM init contract.

## Image Build And Registry Flow

Image builds should not run on sandbox nodes. The intended model is:

- control plane/gateway: scheduler and routing; it also hosts the private
  registry service in the all-in-one deployment
- builder nodes: autoscaled, builder-only VMs for Docker builds and registry
  push; they advertise physical capacity and do not use sandbox admission
- sandbox nodes: run already-built images, pull/cache gateway-resolved immutable
  registry references, reserve disk exactly, and admit CPU/RAM dynamically from
  individual shapes plus live pressure
- registry: durable image cache for common building blocks and custom images,
  typically the control-plane-managed registry backed by a UCloud mount

The gateway routes `POST /v1/images/build` to ready builder-only nodes
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
are checked again on wake. Node heartbeats report
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

## Park and wake

The direct Warden runtime owns sandbox lifecycle and storage-native volumes. A
parkable sandbox has one authoritative generation and one storage manifest.
Park releases compute while preserving its immutable rootfs identity and
writable volume. Wake restores that generation on its owning node when the node
has active capacity. Otherwise the gateway migrates the parked generation to a
ready storage-native node and wakes it there.

Migration is available only when both nodes advertise `storage-native-v1` and
`sandbox-migrate-storage-native-v1`, and the parked route contains its verified
snapshot manifest. The gateway journals destination reservation, source
publication, destination import, atomic route switch, activation, and source
finalization. Retrying the same migration continues that journal; it does not
create another sandbox generation.

Lifecycle operations use generation and operation identifiers throughout.
Heartbeats may confirm an assigned incarnation but cannot create a new
generation or move a route. Storage-native capacity, registry digests, the
migration journal, and route ownership therefore remain explicit durable
authorities rather than observations inferred from runtime processes.
