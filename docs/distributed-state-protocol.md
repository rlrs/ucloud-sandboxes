# Distributed sandbox state protocol

This document defines the required contract for durable sandbox operations.
Every component persists and enforces these properties; there is no reduced
or generation-zero protocol.

## Identity and fencing

A sandbox incarnation is identified by:

- `deployment_id`: prevents cross-deployment admission.
- `sandbox_id`: stable user-facing name.
- `generation`: positive, monotonically increasing integer allocated by the control
  plane. A generation is never reused, including after deletion.
- `operation_id`: stable random identifier for one create, delete, or cancel
  intent. Every retry of that intent reuses the identifier.
- `spec_hash`: canonical hash of the desired sandbox spec, excluding the
  operation envelope and other transport metadata.

The node retains a tombstone containing the greatest accepted generation after
deletion. A delayed request at or below that generation cannot recreate or
delete a later incarnation.

## Durable operation order

The control plane persists an intent before making a network call. Its durable
states are `intent`, `dispatching`, `observed`, `uncertain`, `canceling`, and
`tombstoned`. A timeout moves an operation to `uncertain`; elapsed wall-clock
time alone never changes its generation, target node, or outcome.

The node serializes operations for a sandbox and applies these rules atomically
with its store:

1. The same generation, operation ID, and spec hash is an idempotent replay.
2. The same generation with a different operation ID or spec hash is a
   conflict.
3. A lower generation is stale and has no side effect.
4. A higher generation cannot replace a live lower generation. The lower
   incarnation must first be deleted or canceled and fenced.
5. Delete succeeds idempotently when the matching tombstone already exists.
6. Delete never acts on a live generation newer than the request.

Runtime objects carry generation, operation ID, and spec-hash labels. On
restart the node reconciles its durable store with those labels before serving
mutations. This closes the crash window between the runtime side effect and the
store write.

## Inventory and uncertainty

Each node heartbeat includes:

- `node_epoch`, which changes when the host boots;
- boot-scoped `activity_epoch`, monotonic across node-agent restarts and
  advanced for durable or transient sandbox mutations;
- `inventory_complete`, distinguishing an empty inventory from unavailable
  inventory;
- an inventory entry for every live sandbox with its generation, operation ID,
  spec hash, state, and resources;
- separate live usage, create reservations, build reservations, and physical
  disk telemetry.

Synchronous park and wake acknowledgements carry the worker's `node_epoch` and
a post-transition `activity_epoch`. The gateway accepts that proof only for the
exact routed worker and atomically commits the stable state with the newer
revision. Ordinary activity against a parked route first performs this same
fenced wake, so a heartbeat sampled before the transition cannot later restore
the old lifecycle state. Direct workers advertise this contract as
`hibernate-local-v2`; the scheduler does not place new parkable sandboxes on
older workers. Routes already owned by a v1 worker remain fail-closed until
that worker is drained and replaced.

Inventory may confirm a matching operation, but absence alone does not cancel
an in-flight request: the request might arrive after the snapshot. The control
plane retries the same operation ID on the same node. It may place a new
generation elsewhere only after one of these fences:

- the old node durably acknowledges cancel/delete for the generation;
- the active provider supplies a validated, journaled proof that the old
  instance cannot recover (for example UCloud guest suspension or an expired
  unreachable-guest lease); or
- a node-specific recovery protocol proves the operation rejected and records
  a tombstone/high-water mark.

Provider state that is merely unschedulable is not destructive proof. In
particular, powered-off Hetzner servers retain their routes and local state.
When a worker owner is proven lost, one route classifier decides the outcome:
published portable parks become detached and remain wakeable, delete-intent
routes terminate without replacement demand, and all other routes become
replacement demand.

## Draining and scale-down

Draining is a handshake, not a heartbeat boolean. The control plane creates a
durable random `drain_token`; the node persists it, rejects new create/build
admission, and reports the token with its activity epoch. The VM may be stopped
only when a fresh, gateway-stamped heartbeat reports the same token, complete
inventory, and zero live work and reservations. Any new activity invalidates
the zero-work observation and requires another fresh acknowledgement.

## Autoscaler/provider operations

Only the process holding the deployment-scoped POSIX autoscaler lock may start
provider operations. The kernel releases ownership when that local process
exits; multi-host controllers are outside this protocol. Every create or stop
has a journaled operation ID. VM creates carry the operation ID in a UCloud job
label. After a crash, recovery uses a complete paginated job inventory to
correlate the label; it never blindly resubmits an unresolved create. Duplicate
provider jobs with the same operation label are quarantined and reconciled
explicitly.

The process lock cannot cancel an already in-flight provider request.
Consequently, the operation journal and provider labels remain required even
with one local mutating controller.
