# Sandbox service remediation roadmap

This roadmap deliberately targets the supported deployment: one control-plane
host, one relay process, and local POSIX locks/SQLite. It does not claim
multi-host correctness. The detailed findings and current status are in
`sandbox-service-audit.md`; the wire/state invariants are in
`distributed-state-protocol.md`.

## Design rule

Keep one durable fact for each concern:

- routing SQLite owns sandbox assignment and request incarnation;
- node state owns planned/running/deleting lifecycle and admission;
- Docker labels are independent recovery evidence, not another scheduler;
- the provider journal records only ambiguous UCloud mutations;
- registry references protect active routes/builds; finite leases are only for
  transient pulls;
- relay state is bounded and in memory because a process restart also loses the
  caller connection.

Do not add renewable local leases, full in-memory mirrors of SQLite, or a second
backend unless the supported topology changes.

## Implemented correctness baseline

The current work establishes:

1. A route is persisted before node dispatch with a generation, operation ID,
   and canonical spec hash.
2. A node persists `planned` before Docker create and `deleting` before Docker
   delete. Replays use the same incarnation and runtime labels.
3. Node aggregate capacity counts planned and running work under the admission
   lock. Drain closes create/build admission before zero-work acknowledgement.
4. Complete heartbeat inventory is authenticated, receipt-time stamped, and
   rejected wholesale when identity, resources, or destructive flags are
   malformed.
5. One local autoscaler process holds a kernel lock. Provider creates are
   journaled before submission and correlated by immutable UCloud labels after
   uncertain results.
6. Registry prune revalidates aliases, usage, persistent route/build references,
   and transient leases under the maintenance fence.
7. Relay registrations have incarnation tokens; request/lease expiry,
   cancellation, admission, completed tombstones, and worker diagnostics are
   bounded in the single in-memory state machine.
8. Node-control, heartbeat, public gateway, and relay credentials are distinct
   in generated deployments.

## Remaining repository work

These changes are useful without changing topology, but should be driven by
observed need and delivered separately:

### P1 — Controller structure

Split the large autoscaler cycle into typed phases:

```text
observe -> recover -> plan -> execute -> report
```

Keep bootstrap work outside the provider-mutation critical section. Replace the
large result dictionary and `raw*` keys with typed observation, plan, and result
objects.

### P2 — Ordered drain commands

Replace token reincarnation with a per-job monotonic drain generation and
command revision. The node persists the highest tuple and rejects delayed older
drain/undrain messages. Do this only with an explicit rolling-upgrade protocol.

### P3 — Runtime-object policy

Choose an operator policy for containers found in Docker but absent from node
state: adopt a verified legacy object, quarantine it and close admission, or
surface it for manual repair. Do not silently delete or count it as free.

### P4 — Measured performance

Prioritize only after profiling:

- stream gateway file/proxy bodies when concurrent large transfers drive RSS;
- pool node/UCloud/registry connections when connection setup is material;
- move JSON stores to transactional/incremental storage when rewrite latency is
  material;
- add indexed provider change feeds only if UCloud exposes them.

The existing caps, bounded request threads, socket timeouts, history limits,
pagination guards, and targeted SQLite reads remain the inexpensive baseline.

### P5 — Compatibility retirement

Advertise protocol capabilities, observe legacy generation-zero traffic, and
remove compatibility branches only after a measured zero-legacy window.
Destructive operations must fail closed during mixed-version rollout.

## Architecture programs requiring explicit decisions

These are not local refactors:

- **Multi-host control:** use a server database/consensus-backed coordinator
  with database-time leases and serializable/CAS mutations. Do not extend POSIX
  locks across a network filesystem.
- **Durable desired state:** introduce an accepted request ID, outbox/inbox, and
  caller retry/reattachment semantics. Pending autoscaling hints are not a work
  queue.
- **Relay HA:** define caller idempotency/reattachment, then use a server-backed
  broker. Persisting local orphan requests is not HA.
- **Untrusted builds:** use disposable hardened builders or a remote rootless
  build service with provenance and policy controls.
- **Service decomposition:** split API edge, orchestrator, fleet controller,
  registry controller, relay broker, and observability only when independent
  scaling/failure domains justify it.

## Delivery sequence

Keep commits dependency-ordered and independently testable:

1. local persistence and resource bounds;
2. sandbox incarnation and node lifecycle;
3. routing, heartbeat reconciliation, and node authentication;
4. local autoscaler lock, provider journal, and drain workflow;
5. registry references and fenced maintenance;
6. bounded in-memory relay;
7. deployment wiring and documentation.

Each commit should contain its focused regression tests. Avoid a final test-only
commit that obscures which invariant belongs to which implementation.

## Validation gates

Before production rollout, exercise these crash points:

- before and after Docker create/delete;
- before provider submission, after acceptance but before local commit, and
  while accepted creates are not yet visible;
- before/after drain, undrain, and provider stop;
- between registry prune planning, alias/reference revalidation, and deletion;
- during relay replacement, lease expiry, cancellation, and client disconnect.

Also test corrupt/truncated state, disk-full/fsync failure, repeated pagination
cursors, delayed old-incarnation messages, process lock contention, and a soak
at twice forecast concurrency. Repository unit tests are necessary but do not
replace these deployment-specific gates.
