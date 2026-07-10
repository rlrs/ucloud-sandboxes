# Distributed sandbox state protocol

This document defines the compatibility target for durable sandbox operations.
It is intentionally stricter than the legacy `sandbox_id`-only API. New fields
are additive during rollout, but a component must not claim the safety
properties below until it persists and enforces them.

## Identity and fencing

A sandbox incarnation is identified by:

- `deployment_id`: prevents cross-deployment admission.
- `sandbox_id`: stable user-facing name.
- `generation`: monotonically increasing integer allocated by the control
  plane. A generation is never reused, including after deletion.
- `operation_id`: stable random identifier for one create, delete, or cancel
  intent. Every retry of that intent reuses the identifier.
- `spec_hash`: canonical hash of the desired sandbox spec, excluding the
  operation envelope and other transport metadata.

The node retains a tombstone containing the greatest accepted generation after
deletion. A delayed request at or below that generation cannot recreate or
delete a later incarnation.

Legacy requests have generation zero. They may create a new legacy record, but
must never mutate a record or tombstone whose generation is greater than zero.

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
- persisted `activity_epoch`, incremented for every sandbox mutation;
- `inventory_complete`, distinguishing an empty inventory from unavailable
  inventory;
- an inventory entry for every live sandbox with its generation, operation ID,
  spec hash, state, and resources;
- separate live usage, create reservations, build reservations, and physical
  disk telemetry.

Inventory may confirm a matching operation, but absence alone does not cancel
an in-flight request: the request might arrive after the snapshot. The control
plane retries the same operation ID on the same node. It may place a new
generation elsewhere only after one of these fences:

- the old node durably acknowledges cancel/delete for the generation;
- UCloud reports the old VM job final, so its runtime cannot execute; or
- a node-specific recovery protocol proves the operation rejected and records
  a tombstone/high-water mark.

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

## Rollout order

1. Deploy readers for the additive heartbeat, route, and store fields.
2. Deploy node persistence, runtime-label recovery, and idempotent mutation
   enforcement.
3. Enable control-plane generation/operation envelopes and reconciliation.
4. Enable the drain handshake and provider-operation recovery.
5. Remove generation-zero mutation compatibility only after all nodes advertise
   the generation protocol capability.
