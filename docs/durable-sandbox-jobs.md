# Durable sandbox jobs

Status: **selected production design; implementation not yet complete**

Last reviewed: 2026-08-02

## Decision

A parkable sandbox owns one durable primary job. The sandbox is the isolation,
checkpoint, migration, networking, and resource-accounting unit. The job is
the independently addressable workload lifecycle exposed to clients.

The direct runtime replaces the injected `docker-init` binary with a small
trusted runtime init. That process is already required as PID 1; this design
does not add a relay proxy or harness-specific sidecar. The runtime init:

- starts the arbitrary image command or an SDK-supplied command as the primary
  job;
- assigns and remembers its generation-fenced job ID;
- reaps it and records its exact terminal status;
- owns bounded stdout and stderr spools;
- applies idempotent start, signal, and status operations; and
- remains PID 1 until the sandbox is deleted.

The initial command is therefore job payload, not sandbox-lifecycle authority.
The existing attached exec API remains available for short interactive work,
but an attached exec deliberately retains its lifecycle fence and is never a
supported way to run a parkable harness.

Stock `runsc exec --detach` is not this abstraction. Its implementation starts
a source-host child `runsc exec` process which waits for the sandbox process to
exit. That waiter, its pipes, and its exit-status ownership do not survive a
node-agent restart or move to a migration destination. See the
[gVisor exec implementation](https://github.com/google/gvisor/blob/master/runsc/cmd/exec.go).

## Scope

Version 1 supports exactly one primary job per sandbox generation. This is the
right contract for the current agent model, where an agent is permanently
associated with its sandbox and one arbitrary harness owns the rollout. It is
not presented as a multi-tenant batch scheduler.

Short exec sessions may run alongside the primary job. They are explicitly
non-durable and block park or migration for their attached lifetime. Multiple
durable jobs, background services, and fork are later extensions of the same
job protocol, not prerequisites for correct harness parking.

## Ownership model

```text
gateway job journal                 sandbox checkpoint
-------------------                 ------------------
desired operation + job ID          runtime init (PID 1)
latest acknowledged event     <-->  primary job process tree
terminal result                      job ledger + bounded logs
relay registration                   open files, sockets, memory
route + sandbox generation           supervisor signing key
```

There are two durable records because they answer different questions:

- The gateway journal owns client intent, idempotency, routing, the latest
  acknowledged event, and terminal results which must remain readable without
  waking or even retaining a sandbox node.
- The runtime-init ledger owns guest process truth. It is stored in the
  sandbox's quota-accounted writable storage and moves with the same
  storage-native snapshot as the checkpointed process tree.

Neither record may silently overwrite the other. Reconciliation compares the
sandbox generation, job ID, job-spec digest, monotonic event sequence, and
ledger digest. A mismatch quarantines the sandbox instead of guessing which
process to run.

The job identity is:

```text
(sandbox_id, sandbox_generation, job_id, job_spec_sha256)
```

Every mutating operation additionally carries a caller operation ID. Reusing
an operation ID with another spec is a conflict. Retrying the same operation
returns the already-admitted result.

## Runtime protocol

### Start

1. The gateway commits a generation-fenced `start_requested` row before
   contacting a node.
2. The node takes the sandbox lifecycle fence, restoring it if necessary.
3. A bounded `runsc exec` invokes the injected `ucloud-jobctl` client. It sends
   the operation to runtime init over a private Unix socket.
4. Runtime init validates the identity and appends the admitted operation to
   its journal before launching the child. It redirects stdout and stderr to
   bounded, offset-addressed spool files, starts the process, appends the
   `running` event, and acknowledges the highest durable event sequence.
5. The short `runsc exec` exits. The node releases the lifecycle fence while
   the primary job continues as a child of runtime init.
6. The gateway commits the acknowledgment. A lost response is reconciled by
   resending the same operation ID; it must never launch a second process.

The host-side `runsc` process owns only a bounded control exchange. It never
owns the primary job's lifetime, output pipes, or exit status.

### Status and logs

`GET` operations never wake a parked sandbox.

- While running, the node may perform a bounded status exchange and upload new
  signed events and log chunks to the control plane.
- While parking, the node must flush through a recorded event sequence before
  publishing the parked generation.
- While parked, moving, unreachable, or unassigned, the gateway serves the
  latest committed job state and durable log chunks.
- A stream is a reconnectable view identified by `(job_id, stream, offset)`.
  Disconnecting a viewer never changes job or sandbox state.

Output is bounded independently per stream. Exceeding the configured retained
bytes records a durable truncation event and continues draining the child, so
a noisy harness cannot deadlock itself or consume unbounded hard disk. Large
named artifacts use the existing artifact/storage path rather than the log
journal.

### Park

1. The relay durably accepts a model request and asks the gateway to park the
   exact sandbox generation.
2. The node takes the exclusive lifecycle fence. New starts, signals, file
   mutations, and attached exec sessions cannot cross it.
3. It imports all runtime-init events and log chunks through sequence `N`.
4. The Warden checkpoints the complete process tree and storage, including PID
   1, the primary job, its sockets, journal, logs, and private job key.
5. After gVisor has stopped the live backend, the node reads the frozen job
   ledger, verifies its event chain, and records its digest and sequence in the
   hibernation publication.
6. The gateway atomically commits the parked route, hibernation generation,
   job digest, and observed job state. Only then may active CPU and memory be
   released for scheduling.

The client-facing job state remains `running` with a sandbox execution state
of `suspended`; parking is not process termination. A separate derived state
may display `suspended`, but it must not be mistaken for a job event emitted by
the frozen guest.

### Restore and migration

The existing paused restore handoff is retained. Before the destination can
become routable it must prove:

- the expected sandbox and hibernation generations;
- the expected job identity, ledger digest, and acknowledged event sequence;
- a live runtime init with the checkpointed primary job or its already
  recorded terminal state; and
- the destination route generation and relay transport epoch.

The route commit moves sandbox and job authority together. The source cannot
accept another job operation after migration preparation begins. Log and
status readers reconnect through the gateway; the relay reconnects or replays
by relay request ID. No client is routed to the source host merely because it
created the job there.

After same-node restore, the original relay TCP connection may continue. After
cross-node migration it may fail and retry with the same relay request ID. Job
identity and relay request identity are deliberately separate.

### Completion and cancellation

Runtime init records `exited`, `signaled`, or `failed` exactly once, including
the exit code or signal and the final log offsets. The node uploads that event;
the gateway terminal row is monotonic and remains readable after sandbox
deletion.

Version 1 cancellation has two unambiguous cases:

- A running sandbox receives an idempotent signal operation through runtime
  init and reports the resulting terminal event.
- A parked primary job is canceled by deleting its sandbox generation without
  restoring it. The gateway records `canceled_while_suspended`, seals already
  uploaded logs, and destroys the checkpoint. This is safe because version 1
  has exactly one durable job per sandbox.

Selective cancellation of one among several parked durable jobs is out of
scope. Adding it would require a restore barrier that lets runtime init apply
pending control intents before any workload task resumes; it must not be
implemented as a race immediately after `runsc resume`.

## API contract

The public shape is job-oriented even though version 1 permits one job:

```text
POST   /v1/sandboxes/{sandbox_id}/jobs
GET    /v1/sandboxes/{sandbox_id}/jobs/{job_id}
GET    /v1/sandboxes/{sandbox_id}/jobs/{job_id}/events?after={sequence}
GET    /v1/sandboxes/{sandbox_id}/jobs/{job_id}/logs/{stream}?offset={offset}
POST   /v1/sandboxes/{sandbox_id}/jobs/{job_id}/signal
DELETE /v1/sandboxes/{sandbox_id}/jobs/{job_id}
```

Start accepts an explicit idempotency key or a client-generated job ID plus
`argv`, `env`, `cwd`, and numeric user. It returns after durable admission, not
after process completion. A `JobHandle` in the SDK exposes `status`, `wait`,
offset-based log iteration, `signal`, and `cancel`. None of those methods owns
an attached node connection.

Sandbox creation gains an explicit managed-job mode. In that mode the runtime
init stays alive and an image command, when requested, becomes the primary job.
We do not infer this behavior from a command such as `sleep infinity`, and we
do not overload `parkable` to mean that a harness has already been launched.

## Trust boundary

Runtime init is infrastructure, not workload code:

- it is installed read-only from a content-addressed release artifact;
- it starts as PID 1 and launches the requested workload user with the exact
  OCI capability and `no_new_privileges` policy;
- it marks itself non-dumpable, closes inherited secrets, and exposes a
  permission-restricted control socket;
- it signs or MACs the append-only event chain with a per-sandbox key retained
  in checkpointed init memory; and
- the node verifies the public identity recorded at sandbox creation before
  accepting job state.

A hostile workload may kill or corrupt its own work where ordinary process
permissions permit it, but it cannot forge a later terminal sequence accepted
by the control plane. Loss of runtime init terminates the sandbox and is
reported as infrastructure failure; it is never interpreted as successful
job completion.

Parkable managed-job mode is fail-closed until the runtime-init conformance
probe passes. Images which require a fully privileged root process need a
separate security qualification; silently weakening the init boundary is not
acceptable.

## Relay and program-aware scheduling

Relay registration binds all of the following:

```text
rollout_id
sandbox_id + sandbox_generation
job_id + job_spec_sha256
relay registration incarnation
```

The durable relay request ID remains the identity for model-call replay. A
relay request can drive `acting -> model_wait -> ready_to_wake -> waking`, but
it never changes job identity. The gateway may count parked CPU and memory as
released only after the parked generation and job sequence are committed.

Program-aware autoscaling remains shadow-only until production traffic proves
real transitions from a managed job:

```text
running/acting
  -> relay request durably accepted
  -> model_wait
  -> sandbox parked and active resources released
  -> response committed / ready_to_wake
  -> restored locally or migrated
  -> job continues / acting
```

Merely setting `parkable=true`, launching a host-attached exec, or registering
a relay tunnel must never satisfy this gate.

## Implementation order

1. **Runtime init and local conformance.** Build a static release artifact,
   replace `docker-init` only in explicit managed-job mode, implement the
   idempotent journal/control protocol, and test arbitrary argv, exit, signal,
   bounded logs, and hostile malformed commands.
2. **Node and gateway journal.** Add generation-fenced job rows and APIs,
   bounded short control exchanges, event/log replication, monotonic terminal
   commits, and reconciliation after node-agent and gateway restarts.
3. **Checkpoint binding.** Add job digest/sequence to hibernation and portable
   migration manifests. Test crashes before and after admission, checkpoint,
   publication, destination activation, route commit, and source finalization.
4. **SDK and Verifiers.** Add `JobHandle`, change the integration to launch the
   arbitrary harness through the job API, and run a real relay-driven
   `model_wait -> parked -> ready_to_wake -> running -> completed` flow.
5. **Production qualification.** Canary same-node parking, cross-node
   migration, node-agent restart, relay retry, log reattachment, cancellation,
   and hard output limits. Measure job-admission, park, wake, migration, status,
   and log-tail latency.
6. **Scheduler activation.** Enable program-aware actions only after the above
   flow emits non-zero live program metrics and actual CPU/memory use plus the
   active-sandbox count demonstrably fall while the primary job is suspended.

## Rejected shortcuts

- Using a long attached exec and releasing its lock early: park can invalidate
  the host pipes and process waiter while callers still believe they own them.
- Using `runsc exec --detach` as a durable handle: the source host still owns a
  child waiter and output/exit lifecycle.
- Treating the harness as the sandbox initial process without a durable job
  identity: this conflates sandbox, rollout, retries, logs, cancellation, and
  reuse.
- Inferring idleness from API inactivity: guest CPU and subprocess work are
  not visible to that clock.
- Polling a parked sandbox for process state: status inspection must not spend
  wake latency or active capacity.
- Resuming all workload tasks and then racing to apply a pending cancellation:
  this is not a valid suspension barrier.
