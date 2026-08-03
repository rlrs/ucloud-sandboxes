# Durable sandbox jobs

Status: **implemented and real-node qualified; production rollout pending**

Last reviewed: 2026-08-03

## Decision

A parkable sandbox owns one durable primary job. The sandbox is the isolation,
checkpoint, migration, networking, and resource-accounting unit. The job is
the independently addressable workload lifecycle exposed to clients.

In explicit `managed_process` mode, the direct runtime uses a small trusted
runtime init as PID 1 instead of the image command. This does not add a relay
proxy or harness-specific sidecar. The runtime init:

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
latest terminal result               job ledger + bounded logs
relay registration                   open files, sockets, memory
route + sandbox generation           checkpoint-bound ledger digest
```

There are two durable records because they answer different questions:

- The gateway journal owns client intent, idempotency, routing, the latest
  acknowledged event, and terminal results which must remain readable without
  waking or even retaining a sandbox node.
- The runtime-init ledger owns guest process truth. It is stored in the
  sandbox's quota-accounted writable storage and moves with the same
  storage-native snapshot as the checkpointed process tree.

Neither record may silently overwrite the other. Reconciliation compares the
sandbox generation, job ID, job-spec digest, monotonic sequence, and ledger
digest. A mismatch fails restore instead of guessing which process to run.

The job identity is:

```text
(sandbox_id, sandbox_generation, job_id, job_spec_sha256)
```

Start idempotency is carried by the caller-selected job ID and canonical job
spec digest. Reusing a job ID with another spec is a conflict; retrying the
same identity returns the already-admitted result.

## Runtime protocol

### Start

1. The caller chooses a generation-fenced job ID before contacting a node.
2. The node takes the sandbox lifecycle fence, restoring it if necessary.
3. A bounded `runsc exec` invokes `ucloud-sandbox-init ctl`. It sends
   the operation to runtime init over a private Unix socket.
4. Runtime init validates the identity and durably records admission before
   launching the child. It redirects stdout and stderr to bounded,
   offset-addressed spool files, starts the process, and advances its durable
   sequence.
5. The short `runsc exec` exits. The node releases the lifecycle fence while
   the primary job continues as a child of runtime init.
6. The gateway commits the acknowledgment. A lost response is reconciled by
   resending the same job ID and spec; it never launches a second process.

The host-side `runsc` process owns only a bounded control exchange. It never
owns the primary job's lifetime, output pipes, or exit status.

### Status and logs

Job-status `GET` operations never wake a parked sandbox.

- While running, the node performs a bounded status exchange and commits the
  latest record to the control plane.
- While parked or moving, the gateway serves the latest committed job state.
- Version 1 log reads are offset-addressed but node-backed; requesting logs
  while parked wakes the sandbox. Replicating bounded log chunks into gateway
  storage remains a later optimization and is not required by harness wait.
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
3. The Warden hashes the runtime-init ledger into the hibernation manifest.
4. It checkpoints the complete process tree and storage, including PID 1, the
   primary job, its sockets, ledger, and logs.
5. Restore and migration reconstruct the manifest with that digest and verify
   the mounted ledger before any workload task resumes.
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
- the expected checkpoint-bound job-ledger digest;
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

Version 1 currently exposes one cancellation path:

- A running sandbox receives an idempotent signal operation through runtime
  init and reports the resulting terminal event.

Deleting a parked sandbox destroys its checkpoint and therefore cancels its
primary job, but a separately queryable `canceled_while_suspended` terminal
job record is not implemented yet. Signaling a parked job intentionally wakes
it first.

Selective cancellation of one among several parked durable jobs is out of
scope. Adding it would require a restore barrier that lets runtime init apply
pending control intents before any workload task resumes; it must not be
implemented as a race immediately after `runsc resume`.

## API contract

The public shape is job-oriented even though version 1 permits one job:

```text
POST   /v1/sandboxes/{sandbox_id}/jobs
GET    /v1/sandboxes/{sandbox_id}/jobs/{job_id}
GET    /v1/sandboxes/{sandbox_id}/jobs/{job_id}/logs/{stream}?offset={offset}
POST   /v1/sandboxes/{sandbox_id}/jobs/{job_id}/signal
```

Start accepts a caller- or SDK-generated job ID plus `argv`, `env`, `cwd`, and
bounded log limits. The node derives the numeric workload identity from the
OCI config rather than trusting the caller. It returns after durable admission,
not process completion. Sync and async SDK `JobHandle`s expose `refresh`,
`wait`, offset-based logs, and `signal`. None owns an attached node connection.

Sandbox creation gains an explicit managed-job mode. In that mode the runtime
init stays alive and the SDK-supplied command becomes the primary job.
We do not infer this behavior from a command such as `sleep infinity`, and we
do not overload `parkable` to mean that a harness has already been launched.

## Trust boundary

Runtime init is infrastructure, not workload code:

- it is installed read-only from a content-addressed release artifact;
- it starts as PID 1 and launches the requested workload user with the exact
  OCI capability and `no_new_privileges` policy;
- it receives launch environment through private pipes rather than argv or a
  persisted host record and exposes a root-only control socket; and
- the Warden binds the rootfs ledger bytes to hibernation metadata before
  publication and validates them before restore.

A hostile workload may kill or corrupt its own work where ordinary process
permissions permit it. Loss of runtime init terminates the sandbox and is
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

## Delivery state

1. **Complete: runtime init and local conformance.** Build a static release artifact,
   replace `docker-init` only in explicit managed-job mode, implement the
   idempotent journal/control protocol, and test arbitrary argv, exit, signal,
   bounded logs, and hostile malformed commands.
2. **Complete: node and gateway state.** Add generation-fenced job rows and APIs,
   bounded short control exchanges, monotonic terminal commits, and cached
   parked status.
3. **Complete: checkpoint binding.** Add the job-ledger digest to hibernation and portable
   migration manifests. Test crashes before and after admission, checkpoint,
   publication, destination activation, route commit, and source finalization.
4. **Complete: SDK and Verifiers.** `JobHandle` launches the arbitrary harness
   through the job API. An isolated DFM deployment ran the pinned Verifiers v1
   `NullHarness` through real same-node park/wake and cross-node migration with
   exactly one model call and trace turn.
5. **Complete: isolated real-node qualification.** A clean zero-node run moved
   the managed harness from UCloud job `12362748` to `12362749`, verified the
   checkpoint-bound job-ledger digest, reattached the relay after one expected
   transport reset, and completed. The storage-native migration protocol took
   1.009 seconds after destination readiness; the enclosing 54.61 seconds was
   dominated by VM provisioning. Raw results are in
   [`benchmarks/managed-harness-verifiers-dfm-2026-08-03.json`](benchmarks/managed-harness-verifiers-dfm-2026-08-03.json).
   A second clean run after adding the final destination-mounted-ledger check
   moved job `12362753` to `12362754` and completed the same SDK/Verifiers flow.
   The destination verified the ledger before `runsc restore`; its protocol
   took 1.043 seconds after readiness and the enclosing migration took 54.57
   seconds.
6. **Next: production canary and fault matrix.** Exercise node-agent restart,
   log reattachment, cancellation, and hard output limits under production
   traffic, then measure job-admission, park, wake, status, and log-tail latency.
7. **Scheduler activation.** Enable program-aware actions only after the above
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
