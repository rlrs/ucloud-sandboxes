# Program-aware sandbox scheduling

Date: 2026-07-31

Status: **Stages A-B implemented locally; Stage C action-gated and off by
default; queued wake activation not implemented**

This plan applies the useful scheduling concepts from ThunderAgent to the
storage-native sandbox runtime. It does not couple the sandbox service to a
specific model backend and does not make probabilistic demand part of hard
resource admission.

ThunderAgent's relevant abstraction is a program table spanning multiple model
and tool requests, with phase, resource footprint, and placement. It uses
program-level admission under memory pressure and resumes work through a global
waiting queue rather than permanent session-to-node pinning. Its paper also
describes tool assets such as disk and network ports as part of the program's
heterogeneous resource state:

- [Together AI overview](https://www.together.ai/blog/thunderagent)
- [ThunderAgent paper](https://arxiv.org/abs/2602.13692)
- [ThunderAgent repository](https://github.com/ThunderAgent-org/ThunderAgent)

We borrow the program identity, explicit phase transitions, global resume
queue, pressure-aware destination choice, and asynchronous preparation ideas.
We do not borrow inference admission control or KV-cache ownership: the relay
does not own the inference backend. We also do not pause an executing tool for
packing. Our immovable identity is the agent-to-sandbox association; the
parked sandbox's node placement is deliberately movable, with exact hard disk
ownership and migration fencing that ThunderAgent's GPU scheduler does not
need.

## Objective

Treat one agent rollout as a first-class scheduling object spanning model
generation and tool execution. A rollout is permanently associated with one
durable sandbox identity, but that sandbox is not permanently associated with
one node.

The existing identifiers form the ownership chain:

```text
rollout_id
  -> relay request_id
  -> sandbox_id + sandbox_generation
  -> current route/node or portable parked snapshot
```

This gives the scheduler exact lifecycle boundaries:

- a relay request accepted for a sandbox starts a model-wait interval;
- successful parking makes the sandbox inactive and portable;
- a committed model response makes the sandbox ready to act;
- successful wake makes it active on its selected node;
- rollout termination releases the relay registration and sandbox.

Arbitrary harnesses may overlap requests or executions. The durable model
therefore stores per-request transitions and derives rollout state from
counters; it does not assume a strictly alternating two-state harness.

## Invariants

1. Requested disk is never overallocated. No score, probability, or live
   metric can bypass exact disk fit.
2. A running or waking sandbox must fit its requested CPU and memory on one
   node.
3. Active tool execution is never preempted merely to improve packing.
   Reassignment happens at the already-safe parked boundary.
4. A model response is committed durably before wake scheduling. Retry identity
   remains the relay request ID/idempotency key.
5. Program metrics and forecasts are advisory. Losing them can reduce
   efficiency but cannot lose, duplicate, or redirect a sandbox or model
   response.
6. Queue order cannot invalidate sandbox generation, route incarnation,
   migration fencing, or relay registration fencing.
7. Every delayed wake has bounded aging. Small or cache-local sandboxes cannot
   starve older or larger work.
8. The existing immediate wake path remains the rollback path until queued wake
   replay and crash-boundary tests pass.

## Durable program state

The gateway routing database stores a bounded current-state projection keyed by
relay request ID:

- rollout ID, request ID, sandbox ID, and sandbox generation;
- lifecycle state: `model_wait`, `ready_to_wake`, `waking`, `acting`, or
  `terminal`;
- requested resources and current route incarnation;
- accepted, parked, response-ready, wake-started, and wake-completed times;
- queue priority inputs and last error.

Relay-to-gateway lifecycle notifications carry rollout and request identity.
Repeated notifications are idempotent. A notification for an old sandbox
generation cannot mutate the current program or route.

Terminal records receive bounded retention. Non-terminal records remain until
completion, explicit rollout cleanup, or conservative reconciliation proves
that their registration and sandbox no longer exist.

## Scheduling model

### Immediate and leading demand

The autoscaler uses three distinct inputs:

1. **Hard immediate demand:** ordinary pending/prepared sandbox resources and
   `ready_to_wake` sandboxes that currently have no active placement.
2. **Soft leading demand:** resources of sandboxes waiting for model responses,
   discounted by a configurable aggregate return weight.
3. **Reactive pressure:** measured node CPU, memory, full-memory PSI, and
   storage-operation pressure.

Soft demand may retain or create at most configured headroom; it is never
credited as disk capacity and never causes a placement that fails hard fit.
The first implementation uses an explicit fixed weight and cap. Empirical
model-latency hazard buckets may replace it only after production traces show
that they improve the cost/latency frontier.

### Global wake queue

Once activated, a committed model response enters a durable global queue rather
than forcing an immediate node-local wake. The planner considers every queued
sandbox and every ready node.

Candidate nodes first pass:

- deployment/version/capability compatibility;
- exact disk fit including existing route reservations;
- exact active CPU and memory fit;
- drain and admission state;
- migration and restore concurrency limits.

Among valid candidates, a small deterministic score prefers:

1. the current node when it has active capacity;
2. cached image and snapshot layers;
3. lower live CPU, memory, PSI, and storage pressure;
4. tighter residual packing when the node is healthy;
5. a node whose use does not prevent another queued hard shape from fitting.

Queue priority combines response-ready age, explicit priority, retry count, and
measured restore cost. Aging dominates every locality or short-job preference
after a bounded interval.

### Autoscaling

The feedback loop remains intentionally small:

```text
required =
    hard pending/prepared demand
  + hard ready-to-wake demand
  + bounded weighted model-wait demand

create when:
    required does not fit projected ready/provisioning supply
    OR sustained live pressure needs one headroom node

stop only when:
    hard demand still fits
    AND queued wakes still fit
    AND weighted model-wait demand is below the low watermark
    AND pressure/provisioning cooldowns have expired
    AND parked disk ownership can be evacuated exactly
```

High and low watermarks provide hysteresis. VM provisioning p95 remains the
scale-down grace floor. Wake queue age and return-to-tool latency become primary
SLO signals.

## Metrics and dashboard

The indexed metrics database exposes bounded current projections and events for:

- rollouts and requests by derived phase;
- model-wait duration;
- ready-to-wake queue resources and oldest age;
- wake placement decision and rejected-node reasons;
- response-ready to wake-start and wake-complete latency;
- local wakes versus migrations;
- soft-demand contribution and cap;
- queue starvation/fairness;
- model returns that arrived without predicted headroom;
- idle node-hours retained by program-aware demand.

The dashboard must show the exact program inputs used by each autoscaler cycle.
Program state is read with indexed current-state queries, not reconstructed by
scanning the event history.

## Rollout stages

### Stage A: durable observation

- Persist idempotent per-request phase transitions.
- Expose program-state metrics and dashboard counters.
- Keep immediate wake behavior unchanged.

Implementation: complete locally. Relay lifecycle identity is persisted in
SQLite, transitions are monotonic and generation-fenced, sandbox deletion
terminalizes retained records, and the dashboard exposes bounded program
summaries.

Acceptance:

- relay restart, gateway restart, duplicate result, transport reset, migration,
  and generation-fence tests preserve one request/sandbox association;
- observation loss cannot affect product requests;
- current-state queries stay bounded at 10x target scale.

### Stage B: shadow wake planner

- Compute queue order and destination for every response-ready sandbox.
- Record the planned decision and compare it with the actual immediate wake.
- Do not delay or redirect the production wake.

Implementation: complete locally. Every relay return records a one-request
O(nodes) shadow choice and the actual wake owner. The periodic autoscaler
computes the full aging-first queue against all ready nodes. This split avoids
putting an O(requests × nodes) simulation on the relay request path.

Acceptance:

- every shadow choice passes the same hard-fit predicates as production;
- no sandbox waits indefinitely in simulation;
- migration rate, predicted queue latency, and packing improve over current
  placement on production traces.

### Stage C: leading-demand autoscaling

- Add bounded weighted model-wait demand and hard ready-wake demand.
- Default the action off while displaying counterfactual creates/stops.
- Tune weight, cap, and watermarks from production traces.

Implementation: observation and counterfactual calculation are complete
locally. Action is controlled by
`program_aware_autoscaling_enabled` and defaults off. Production trace tuning
and canary acceptance remain.

Acceptance:

- lower return-to-tool p95 or fewer capacity misses at an acceptable increase
  in idle node-hours;
- false headroom creates are visible and bounded;
- disabling the feature produces the original autoscaler decision.

### Stage D: queued wake activation

- Activate durable queue admission for a canary fraction of rollouts.
- Reconcile queued work independently of the original HTTP connection.
- Deliver/replay the committed response only after fenced wake completion.

Acceptance:

- complete crash matrix around enqueue, placement reservation, migration route
  commit, wake, response delivery, and acknowledgement;
- bounded fairness under heterogeneous resource shapes and wake storms;
- rollback to immediate wake requires no sandbox or relay-state migration.

## Deliberately deferred

- Pausing active tool executions for packing.
- A general constraint solver.
- Per-rollout learned completion-time prediction.
- Cross-service ownership of the model inference backend.
- Fork/clone scheduling policy.

If the inference backend supports a program identifier, `rollout_id` may also
be forwarded as that identifier to improve KV-cache scheduling. That is an
independent integration and is not required for sandbox scheduling.

## Local 10x lower-bound benchmark

[`benchmarks/program-scheduler-shadow-2026-07-31.json`](benchmarks/program-scheduler-shadow-2026-07-31.json)
records 10,000 program requests and 32 candidates. Median local timings were
105 ms to read all current states, 19 ms to reduce scale signals, 33 ms to
build the bounded dashboard summary, 0.6 ms for the per-arrival shadow choice,
and 559 ms for the full 10,000-request global plan.

The global result is intentionally periodic. It is not acceptable on the relay
critical path. These are local CPU/storage lower bounds; the production
persistent mount and heterogeneous wake-storm qualification remain rollout
gates.
