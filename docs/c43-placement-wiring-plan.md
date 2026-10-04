# C4.3 placement wiring and C3.2 group create: implementation plan

Status: plan, 2026-10-03. Scoped against today's create path. The design is
in [rl-scale-architecture-plan.md](rl-scale-architecture-plan.md) ("C4.3",
"C3.2") and [rl-state-primitives.md §4](rl-state-primitives.md). The library
`placement_choice.py` exists but is not wired.

## Findings

1. **Production runs at most 32 creates at once, across the whole fleet.**
   - With PostgreSQL routing, every public create enters the durable queue
     (`cli.py:1099`, `queue_placement=not placement_role`).
   - One placement process claims at most `create_concurrency=32` commands
     (`placement_queue.py:561`) and replays each to itself over loopback HTTP
     (`:584-603`).
   - Six API processes and a per-node target of 32 sit behind this one
     executor. That queueing happens at the gateway, apart from the per-node
     queueing in the 512-rollout baseline.
2. **C4.3 alone may not move the 512 burst.** The baseline found creates
   roughly serialized per node: about 180 × 0.38 s, or 70 s. Phase 0 measures
   which queue dominates before any wiring.
3. **Wakes still need some of the pieces.** Wake batches
   (`control_plane.py:384-386`), migrations (`:987`) and `wake_placement.py`
   use the capacity revisions, the advisory turns and the scheduling lock. So
   the create phases remove only the create path's use of them. The pieces
   themselves go in a later wake phase (C4.3b).
4. **`node_startup_busy` is a definite reject, but the gateway treats it as
   ambiguous.** The node raises it before provisioning
   (`direct_service.py:2490`). `_node_create_rejection_reason`
   (`control_plane.py:6223-6233`) does not list it, so a busy node keeps the
   route instead of the gateway choosing another. It is a one-line fix, and
   safe on today's path.
5. **A node holds a create for up to 30 s rather than rejecting it.** Its
   startup slots and memory-demand waits use `admission_wait_seconds`.
   - "Try the next of k" therefore needs a short node-side wait.
   - The wait goes in a header (`X-UCloud-Admission-Wait`), because
     `_ucloud_operation` must have exactly four keys.
   - Old nodes ignore the header and wait as before.
6. **Nothing reaps a sandbox that has no route.** Inventory reconcile skips
   such entries (`routing.py:2318-2321`). So the first phases still write the
   route intent before dispatch, but cheaply. The literal "write the route
   once, after acceptance" is phase 3b, with a reaper and a delete fence.

## Phase 0 result (2026-10-04)

The 2026-10-03 baseline's burst was rerun on 0.9.6: 512 rollouts, 195 images,
three CCX63, with wave 1 on the chunk store. Each command's `created_at` and
`completed_at` were read from `gateway_commands`.

**Gateway descriptor limit (fixed).** First, two runs on 0.9.4 lost 45 and
131 creates to EMFILE: env-io ran at systemd's default of 1024 descriptors
and peaked at 1217. 0.9.6 raised it to 65536.

**After the fix:**

| Run | Ready | Ready p50 / p95 / max (s) | Commands done on attempt 1 | Rate |
| --- | --- | ---: | ---: | --- |
| A3, from zero, 8 turns | 511/512 | 159 / 225 / 244 | 148 | nothing for 96 s (provisioning), then about 3.5/s |
| B3, warm fleet, 1 turn | 511/512 | 63 / 130 / 139 | 353 | about 3.5/s, flat for all 139 s |

The one failure in each run is the catalog gap, as in the baseline.

**Where B3 waited:**
- **Inside a worker's create.** `manager_create` p50 3.0 s, p95 12 s, max
  21 s. Nearly all of it is `image_resolve`, the component attach: p50 2.7 s,
  p95 11.6 s. Every other phase stays under 0.2 s. The 0.8.4 baseline's
  worker create was 0.38 s.
- **In requeues.** 158 creates needed 2 to 26 attempts. Busy nodes turned them
  away, and the queue retried them with backoff.

**Verdict.** At this scale the burst is limited by node-side attach and
admission, not by the gateway's 32-command executor.
- The executor never had fewer than 32 commands waiting, but a command's time
  went into the node.
- The levers come first:
  - EROFS attach is serial (`attach_concurrency` 1). Each wave M2 moves to RAFS
    gets 8 nydusd slots.
  - Node admission rejects rather than queueing.
  - Per-node create concurrency (C5.2).
- C4.3 is still needed for creates/s at the API tier and for C4.5, but it is
  not what limits a 512-rollout burst today.

**Confound.** Wave 2's converters were reading the gateway registry at
100 MB/s throughout, which slows EROFS attach reads. Rerun B3 with no
conversions running before tuning against these numbers.

## Phase 0b (2026-10-04, evening): the queue's own bounds removed

0.9.14 logs why the placement worker defers commands. Each change below was
measured alone on the same 512-rollout pair (from zero, then warm), with the
store node's reads in Go (0.9.13):

| Gateway | Warm ready p50/p95/max (s) | Retried creates | Worker create p50/p95 (s) |
| --- | --- | ---: | --- |
| 0.9.13 | 45 / 83 / 89 | 182 (up to 34 tries) | 2.0 / 4.8 |
| 0.9.15: a create awaits its environment attach (was 503 after 2 s) | 45 / 85 / 90 | 3 | 1.8 / 4.6 |
| 0.9.16: creates in flight = per-node slots x max nodes (was 32) | 23 / 55 / 61 | 105 | 3.0 / 8.8 |
| 0.9.17: awaited attaches not capped at 32 per process | 33 / 72 / 84 | 69 (2 tries) | 3.3 / 10.5 |

- **The retries were a polling loop.** `image_warmup_pending` was nearly
  every deferral: the gateway's separate per-node "pull" is the attach on
  environment workers, and creates polled it every 2 s through the queue.
- **Then the fleet-wide 32 bound** capped completions near 6/s (32 in flight
  over ~5.5 s each).
- **Now the burst is node-bound.** `image_resolve` (the attach) is 2.9/8.1 s
  p50/p95 under burst load, against 0.5 s when creates trickle in. Run-to-run
  noise is about ±10 s at p50.
- **Next levers:** node attach throughput (RAFS attach slots, and the
  remaining EROFS images, which attach serially until wave 3 switches);
  C3.2 group create, which attaches once per group; C4.3 phase 1-2 for
  creates/s at the API tier (≥ 300/s). The fixes above are stopgaps on
  today's path, not the target shape.

**Then (2026-10-04, later):** traces of a warm burst (a loopback OTLP sink at
100% sampling) put 64% of create time in image preparation: a node pull-slot
wait of 11.8 s p50 and an attach of 7.9 s p50. PostgreSQL was idle (no lock
waits) and the placement process averaged ~45% of a core. On the workers,
env-io sat at 120-160% CPU: every nydusd attach built the Python reader's
per-chunk state under one GIL. 0.9.18 loads only what nydusd reads:

| Workers | Warm ready p50/p95/max (s) | Worker create p50/p95 (s) | Attach p50/p95 (s) |
| --- | --- | --- | --- |
| 0.9.6 agent, gateway 0.9.17 | 26 / 56 / 65 | 1.7 / 8.1 | 1.3 / 7.6 |
| 0.9.18 agent | 19 / 36 / 44 | 0.7 / 1.4 | 0.3 / 1.1 |

The burst drains at ~14-15 creates/s with 96 in flight. C4.3 phases 1-2 are
next for creates/s; C3.2 for attaches per group.

## Today's create (PostgreSQL)

Each step is marked by its fate: **D** deleted by the create phases, **W**
deletable only once wakes move (C4.3b), **K** kept.

1. Gateway limiter, then spec parse. **K**
2. `PlacementQueue.submit`: the command row plus a `queued_create` pending
   row. The client waits on LISTEN/NOTIFY. **K** only for creates that must
   wait; **D** on the fast path.
3. The placement service claims the command (`SKIP LOCKED`) and POSTs it to
   its own loopback listener. **D** for creates, **W** for wakes.
4. `command_execution`: a SERIALIZABLE transaction around the command row.
   **D** for creates.
5. Image resolution, import, `environment_root` dispatch, and the
   existing-route read with its retry. **K**. The layer-cache read is **D**:
   only placement scoring uses it.
6. `Placement.select_and_reserve`. **D**
   - a whole-fleet `SELECT * FROM sandboxes` under REPEATABLE READ, plus
     every heartbeat;
   - the lexicographic rank;
   - an advisory lock per worker;
   - a REPEATABLE READ transaction with `routes_for_node`, the fit recheck,
     and `allocate_sandbox_create_with_pending`, which writes the route and
     the capacity fence.
7. Route image reference under the host-wide `registry-leases` flock. **K**
   until phase R.
8. `_ensure_image_for_create`, single-flight. **K**
9. A pressure recheck with another `routes_for_node` transaction. **D**
10. Node `POST /v1/sandboxes`. **K**
11. `confirm_sandbox_observation`, SERIALIZABLE. **D**: it becomes one fenced
    UPDATE.
12. On a reject: delete the route, record demand, run another whole-fleet
    select, and recurse. **D**
13. Queue completion and NOTIFY. **D** on the fast path.

## Phases

The switch is `gateway_create_placement: "ranked" | "power_of_k"`, default
`ranked`. Each phase is one PR.

| Phase | What | Package lines |
| --- | --- | ---: |
| 0 | **Measure.** From traces of a 512 run: queue claim latency, worker turn waits, scan and transaction time, and the node's startup-admission and active-capacity phases. If node phases dominate, C3.2 and node create concurrency matter more for the burst. | 0 |
| 1 | **Power-of-k behind the switch.** See below. | ≈ +380 |
| 2 | **Creates leave the loopback.** Every API process claims queued creates and runs them in process, under the command fence. The budget becomes 6 × 32. The placement service claims wakes only. | ≈ +65 |
| 3 | **Flip the default after a production canary**, then delete the ranked create path. See below. | ≈ −960 |
| 3b | **Optional: the literal write-once.** See below. | ≈ +90 |
| R | **Image liveness from routes.** Retention derives image liveness from route rows plus a grace window, then drops per-route leases and the `registry-leases` flock. A GC-safety change with its own review. | ≈ −70 |
| C4.3b | **Wakes on node-gated admission.** Delete the capacity fence, the advisory turns, `run_placement`, the scheduling locks, `command_execution`, the loopback branch and the placement service. | ≈ −450 |

**Phase 1 in detail:**
- **Chooser.** A process-wide `InflightOverlay` and `PowerOfKChooser`.
  - Candidates come from shared heartbeats, plus active-migration
    reservations. There is no fleet scan.
  - Required capabilities come from `_sandbox_required_capabilities`, which
    covers M2's root dispatch.
- **Routing.**
  - `reserve_create_intent`: today's allocation without the capacity read,
    under READ COMMITTED.
  - `retarget_create_intent`: moves the intent after a definite reject.
  - `confirm_create`: one fenced UPDATE.
- **The create path.** A new `gateway/create.py` runs up to 2 rounds over
  the k candidates.
  - It sends a short admission wait to each candidate except the last, which
    gets the full wait.
  - Outcomes:
    - accept: confirm;
    - definite reject: release and try the next candidate;
    - ambiguous: keep the route.
- **Node.** Parses the admission-wait header.
- **Tests.** LocalFleet with two gateways over shared state, parametrized
  over both modes, plus an `environment_root` scenario.

**Phase 3 deletes:**
- the ranked block of `control_plane.py`;
- `select`, `rank`, `select_and_reserve`, `alternate_available` and
  `InflightCreatePlacements` from `gateway/placement.py`;
- `RegistryLayerMetadataCache`;
- `allocate_sandbox_create_with_pending`.

**Phase 3b** adds:
- a generation allocated in one statement, and one INSERT on acceptance;
- an hwm bump on a DELETE of a create that has no route yet;
- an orphan reaper in heartbeat ingest.

**C3.2 (P4) on top**, ≈ +510 (≈ +420 without the node batch):
- **Placement.**
  - `plan_group` wraps `PowerOfKChooser`.
  - `pack` uses B = min(32, 2 × per-node create concurrency).
  - `spread` is added.
  - `choose` gains `include_job_ids`, so a node holding the image joins the
    sample.
- **Routing.** A `sandbox_groups` table (additive DDL), and multi-row intent
  and confirm, one transaction per node.
- **Gateway.** `gateway/groups.py`: `POST`, `GET` and `DELETE
  /v1/sandboxes:batch`.
  - Member IDs are `<group>-<i:04d>`.
  - The image and root are resolved once per group.
  - One image ensure per (node, image).
  - Rejected members are re-planned for at most 4 rounds.
  - Leftover members become pending demand.
- **Node.** `POST /v1/sandboxes:batch` behind the
  `sandbox-batch-create-v1` capability. A node without it gets per-member
  POSTs.
- **Deferred.** "Setup on create" moves to P5.

**Budget.** Phases 1–2 need a temporary raise of about 445 lines, repaid by
phase 3, which leaves the package about 515 lines lower. P4 then fits with
almost no headroom.

## Risks

- **Fencing.** Phases 1–3 keep today's generation, operation-id and
  spec-hash fences.
- **Phase 3b opens three gaps:**
  - orphans after a crash, closed by the reaper;
  - a delete racing a create that has no route yet, closed by the hwm bump;
  - retry churn.
- **Wakes.** The create writes keep `_fence_route` until wakes move, so wake
  snapshots still see creates.
- **Autoscaler.** Inline creates show up as demand only after every round
  fails. Warm bursts therefore scale up later, while starts from zero still
  queue visibly.
- **Pause tier.** Queued wakes reserve memory before creates, so short waits
  bounce. The full wait on the last candidate stops all k failing together.
- **M2.** Capability filtering happens before sampling, so k is uniform over
  capable nodes. Heartbeats do not show which nodes already hold a root (that
  needs C2.8), so placement gives such nodes no preference.
