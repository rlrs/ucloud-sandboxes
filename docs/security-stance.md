# Sandbox security stance

Sandbox nodes run one privileged Warden that invokes the deployment-pinned
`runsc` binary directly. Docker and containerd remain image infrastructure;
they do not own sandbox processes, writable layers, or lifecycle state.

gVisor interposes a userspace kernel between workloads and the host kernel. It
is a strong isolation layer, but it is not a VM. Workloads that depend on host
kernel modules, nested container runtimes, privileged mounts, cgroups, or exact
`/proc` behavior are outside the sandbox contract.

## Runtime authority

The Warden is the only sandbox lifecycle writer on a node. Every mutation
requires the exact sandbox generation and a stable operation id. The node
rejects stale generations, conflicting retries, and mutation while admission is
closed.

Sandbox root filesystems are OCI images materialized through the node image
cache. Writable volumes are storage-native devices with hard physical
reservation. Admission never relies on a soft Docker writable-layer estimate.
The storage service, Warden registry, and control-plane route must agree on the
same owner and generation before a sandbox is advertised as running.

The default network mode is `none`. `sandbox` networking creates a dedicated
namespace and permits only configured TCP egress destinations. Node-control
and storage sockets are never mounted into a workload.

## Bootstrap trust

Nodes boot from one role-specific package bundle staged by the control plane.
The bundle SHA-256 digest is mandatory. VM init verifies:

- the exact Ubuntu release, architecture, kernel release, and node role;
- every Docker, gVisor, support-package, and kernel-module digest;
- the preassembled Python agent runtime;
- on sandbox nodes, the pinned patched `runsc`, managed PID 1, and
  storage-native backend provenance.

Any missing, corrupt, or mismatched artifact aborts bootstrap. Nodes never add
package repositories or download a substitute runtime during boot.

## Authentication boundaries

Generated deployments use four distinct mandatory gateway and node
credentials:

- sandbox API key: least-privileged public SDK access;
- gateway control token: operator, autoscaler, and internal relay access;
- heartbeat token: node heartbeat publication;
- node-control token: privileged control-plane-to-node calls.

Every node route except `/healthz` requires the node-control bearer token. The
gateway strips caller authorization headers before forwarding and attaches only
the private node credential. Heartbeat reads, image warmup, drain, and sandbox
operations use the same protected channel. Empty token files are a
startup error.

The sandbox API key can create and operate SDK sandboxes, upload build
contexts, build and pull images, and publish prepared-capacity demand. It
cannot read operator telemetry or node inventory and cannot explicitly park,
wake, detach, or migrate a sandbox. The gateway control token remains accepted
through the UCloud-safe header for operator tooling, but must never be
distributed as an SDK key. Public deployments terminate trusted TLS before the
gateway so neither credential crosses the Internet in plaintext.

The current node-control token is deployment-wide. Private networking and host
firewalling remain part of the trust boundary; per-node identity would be the
next hardening step if mutually untrusted node operators enter scope.

## Workload policy

Sandbox requests carry explicit CPU, memory, disk, user, capability, PID, and
network policy. The service validates these fields before constructing the OCI
bundle. The normal profile should use an unprivileged numeric user, drop all
capabilities, enable `no_new_privileges`, and bound PIDs and temporary files.
Requests that deliberately grant root or capabilities receive exactly that
weaker policy. Host-oriented bootstrap behavior requires the explicit
`linux_host` profile.

Secrets must be injected at execution time and treated as workload-visible.
They must not be written into images, node labels, route metadata, logs, or
storage-native snapshot manifests.

## Operational invariants

- A node advertises readiness only after its exact bundle and services pass
  startup checks.
- Disk capacity is credited only from storage-native physical authority.
- Park, wake, and delete are generation-fenced and crash-replayable.
- Migration requires the canonical storage-native schema and capability on
  both nodes; manifest and snapshot digests fence every ownership transition.
- A node in drain mode closes admission before reporting drain progress.
- `/healthz` reveals service health and version only.
- Sandbox API, gateway control, heartbeat, and node-control credentials must be
  rotated independently.
