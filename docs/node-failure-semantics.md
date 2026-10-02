# Node failure semantics

This is the canonical description of how the gateway, the autoscaler and the
worker treat a worker that goes silent, is suspended or powered off, reboots,
is deleted by its provider, or is drained. It describes the code with decisions
D1-D7 of the 2026-10-02 loss audit applied (reviewed, unmerged on that date).
[Scaling policy](scaling-policy.md) owns the capacity rules and the
[distributed state protocol](distributed-state-protocol.md) the fences.

## Rules

1. **Silence is never loss.** A late heartbeat, a failed probe, or a provider
   suspension or power-off makes a worker unschedulable and its traffic
   retryable. It never declares a sandbox lost or terminates an occupied VM.
2. **A new boot loses processes, not the node.** A fresh authenticated
   heartbeat with a new `node_epoch` (the guest's kernel `boot_id`) proves the
   old boot's processes are gone. Running, paused and half-captured
   incarnations are lost; complete local parks survive on disk and are kept.
3. **Loss has one answer:** 410 `node_lost` (exec sessions 410
   `exec_worker_lost`), never a bare 404.
4. **Termination needs a proof:** a drain acknowledgement of an empty worker,
   or an empty lease-expired worker that never heartbeated or failed a direct
   transport probe in the same cycle. No provider status authorizes it.

## Failure classes

### 1. Late heartbeat, worker alive

- **Detection.** The gateway stamps its receipt time on each push (every 20 s);
  a worker is fresh for `gateway_heartbeat_ttl_seconds` (120 s). Archetype: the
  2026-09-20 virtiofs allocation failure in the heartbeat process.
- **Clients.** Before an exec, file, park, wake, DELETE, create-replay or
  exec-session request answers 503, the gateway pulls `GET /v1/heartbeat` from
  the stored node URL (2 s timeout). Requests for one worker boot share one
  pull; the next waits 2 s after it ends, doubling to 32 s while unanswered. A
  sample counts only if node, job, deployment, agent version and URL match,
  and is ingested like a push: it reconciles routes, and a new epoch is
  class 4. A successful pull lets the request proceed, so every request type
  agrees. New placement does not pull; it skips the stale worker.
- **Controller.** UCloud quarantines it (`heartbeat_continuity_unverified`,
  admission closed) and probes it in the same 5 s cycle (3 s timeout); a
  matching boot and route inventory recovers it at once. Hetzner has no
  quarantine: the node counts as unreachable and may be replaced one-for-one
  inside `max_nodes`.
- **Worker.** Nothing. An agent restart within one boot adopts live sandboxes
  and settles interrupted parks and wakes.
- **Termination.** Occupied: never. Empty: the unreachable-empty proof, which an
  alive worker fails because it answers the probe (its same-boot answer is
  stored as its heartbeat).

### 2. Unreachable on push, pull and probe

- **Detection.** Class 1 plus a failed pull and a failed direct probe. Only a
  transport error fails; an HTTP, auth or schema answer proves contact.
- **Clients.** exec, files, park, DELETE: 503 `sandbox_worker_unreachable`,
  `retryable: true`, `Retry-After: 1`. DELETE first records a durable
  `delete_operation_id`; later traffic gets 409 `sandbox_delete_pending`. Wake
  of a local park: 503 `wake_destination_unavailable`. A detached published
  park migrates and wakes elsewhere; an attached one cannot, because migration
  needs the source to answer `/migration/prepare`. Status reports `unknown`. No
  route is deleted and no loss is recorded.
- **Controller.** Each executing cycle replays recorded deletes, least
  recently attempted first (`pending_delete_attempts` in the autoscaler state,
  stamped before the calls), 16 per cycle, eight at a time, 30 s each; the
  operation id makes any replay safe. UCloud: quarantined and `unavailable`,
  so it keeps its billing slot, counts as unreachable, is skipped by that
  replay, cold offload and soft drain, and is never stop-eligible. Hetzner:
  unreachable, replaceable one-for-one; an empty one may meet the
  unreachable-empty proof.
- **Worker.** If the partition heals within the boot, nothing. UCloud recovery
  needs a probe from the quarantined boot with complete inventory, no in-flight
  creates, and every assigned route present with the same generation, create
  operation and spec hash. Quarantined ingest retires no routes, so a verified
  same-boot probe first retires the routes its complete inventory omits by the
  ordinary reconcile rules (in-flight and newer-activity routes stay
  protected); one vanished sandbox no longer pins quarantine. Never across a
  boot change.
- **Termination.** Occupied: never automatically, on either provider; an
  operator decides. Empty: the unreachable-empty proof.

### 3. Provider suspension or power-off

- **Detection.** UCloud: a current post-start `SUSPENDED` is `unavailable`. A
  RUNNING job whose history holds a timed post-start suspension carries
  `interrupted_at` and is quarantined (`provider_readiness_unverified`) only if
  that is newer than the label
  `ucloud-sandboxes/controller-continuity-verified-through`, which recovery
  writes; a historical suspension quarantines once, not every cycle. An untimed
  one stays `unavailable`: no watermark can cover it. Hetzner: `off` and
  `stopping` are `unavailable` (the disk survives); `starting`, `migrating`,
  `rebuilding` and unknown statuses are `provisioning`.
- **Clients.** Creates: 503 `no_ready_node` if no other node fits. Existing
  work keeps serving while the heartbeat is fresh. While its boot is unchanged
  (`ucloud-sandboxes/controller-quarantine-epoch` equals `node_epoch`), a
  quarantined worker still wakes its own local parks. A powered-off worker is
  silent: class 2 answers.
- **Controller.** UCloud probes every cycle and recovers on `RUNNING` plus
  continuity, recording the interruption it covered. An off Hetzner server
  stays in `max_nodes` and the unreachable count (replaced one-for-one), shows
  as `unavailable` in cycle output and `vm_observed` events, and is never
  stopped automatically.
- **Worker and termination.** A readiness blip needs nothing; a real reboot
  (every UCloud case allowed to finish so far) is class 4. Nothing terminates:
  an off Hetzner server stays billed until an operator powers it on (class 4)
  or deletes it (class 5).

### 4. Reboot: a new authenticated `node_epoch`

- **Detection.** The first pushed or pulled heartbeat of the new boot. The
  control store retires the old epoch (later heartbeats from it are refused)
  and, in the same write, appends the receipt time to the label
  `ucloud-sandboxes/controller-epoch-retirements` (last four kept). The
  gateway emits `node_epoch_retired` with `downtime_seconds`.
- **Gateway.** `retire_node_epochs` settles every old-boot route of the job in
  one routing transaction, idempotently on every heartbeat:

  | Old-boot route | Outcome |
  | --- | --- |
  | Attached; the new boot's complete inventory reports its exact incarnation (generation, operation id, spec hash) `parked` | re-adopted into the new epoch, wakes there; its exec sessions are lost |
  | Carries a recorded client DELETE | kept as a pending delete until the worker confirms |
  | Published (portable) park | detached; wakes on any storage-native node |
  | Anything else: running, paused, capture without COMPLETE | deleted with a loss row, reason `rebooted` |

  At startup the worker marks a RUNNING journal with a dead sentry
  recovery-required, quarantines an ERROR volume instead of mounting it, and
  reports `parked` only with a valid COMPLETE manifest: an interrupted restore
  rolls back to parked, an interrupted capture without COMPLETE is not.
  Ordinary inventory never adopts or retires another boot's route. Off the
  heartbeat thread, the reboot reaper then sends
  generation-fenced worker DELETEs (one pass per job at a time, at most 32 per
  pass, 60 s each) for the pending client deletes and for each new-boot
  inventory entry whose incarnation has a `rebooted` loss row; a confirmed
  client delete removes its route. The work is derived from durable state on
  each heartbeat, so a crash or failed call waits for the next one. This frees
  the dead entries' reservations and ids.
- **Clients.** Lost incarnations: 410 `node_lost`, `reason: rebooted`,
  `retryable: false`, with `sandbox_generation` and `lost_at`; exec sessions
  410 `exec_worker_lost`; DELETE 200 `{"ok": true, "deleted": false}`.
  Re-adopted parks wake normally. After a delivered client delete GET is 404.
  Creating an id the worker still registers is 409
  `sandbox_registration_conflict`, and a detached park never migrates to its
  former owner or to a worker whose inventory still holds its id.
- **Controller.** The node rejoins the pool. On UCloud the power-off shows as a
  suspension and quarantines it; once ingest has retired the old boot, the
  quarantine is re-anchored on the new one, whose routes then prove continuity,
  and the next cycle recovers it. Two epoch retirements for one job within 24 h
  mark a failing host: it stops counting as capacity, takes the soft-drain slot
  regardless of demand (with `drain_on_park_enabled`, the default) and stops
  through the ordinary drain handshake once idle.
- **Termination.** No direct path: the new guest is an ordinary node, stopped
  by the drain proof when idle; a repeatedly rebooting one is emptied first.

### 5. Provider deletion or termination

- **Detection.** UCloud final states, a re-retrieve when a job vanishes from the
  per-state query, and a full census every 300 s. Hetzner `deleting`, absence
  from the listing, or 404 on terminate. Our own accepted stops.
- **Clients.** Until a cycle observes the final state the worker is merely
  silent (class 2 answers). That cycle prunes its heartbeat and deletes the
  routes of final, definitely terminated or destructively lost jobs as 410
  `node_lost` (`reason: node_lost`: cause not recorded), exec sessions 410
  `exec_worker_lost`. Published parks detach and wake elsewhere. An executing
  cycle also deletes, as `node_lost`, routes whose job is no longer an active
  provider job and whose node is not fresh, after the 360 s stale-route grace.
- **Controller.** Retires the job's drain intents, prunes orphaned heartbeats
  and replans. **Worker.** Gone, with every unpublished checkpoint.
  **Termination.** Already done; nothing is left to stop.

### 6. Drain and scale-down

- **Detection.** A fresh idle node past `max(scale_down_idle_seconds, 2 x
  provisioning p95)`, an incompatible agent, or the repeated-reboot rule.
- **Clients.** No new placement; existing traffic keeps working, and parks
  moved by detach (2 per cycle) or soft drain (4 per cycle) wake elsewhere.
- **Controller, worker, termination.** A durable drain intent, then `POST
  /v1/drain` with its token; the worker closes admission, publishes parks on
  request and acknowledges when empty. Demand cancels a drain; a quarantined
  node cannot be drained to termination. Only the drain proof stops it.

## Termination proofs

The stop journal records one before any provider call:

- **Drain proof** (`drainToken`, `drainReady`): a fresh, gateway-stamped,
  complete, empty inventory acknowledging the same token and activity epoch,
  with zero used and reserved resources.
- **Unreachable-empty proof** (`unreachableStaleReady`): phase `running`;
  receipt older than `unreachable_stop_after_seconds`; no gateway routes; a
  last heartbeat with complete empty inventory and zero used and reserved
  resources (`lastHeartbeatSafeToStop`); and either no heartbeat ever
  (`lastHeartbeatPresent: false`) or a transport failure of this cycle's direct
  probe (`directProbeFailed: true`). A quarantined UCloud node is
  `unavailable`, not `running`, so on UCloud it retires never-heartbeated VMs
  only.
- **Provider destructive loss:** `destructive_instance_losses` is empty for both
  in-tree adapters; older UCloud destructive stop records are invalidated
  before replay.

## Timers

| Timer | Default | Hetzner prod | Effect |
| --- | --- | --- | --- |
| `heartbeat_interval_seconds` | 20 s | 20 s | push cadence |
| `gateway_heartbeat_ttl_seconds` (also `policy.heartbeat_ttl_seconds`) | 120 s | 120 s | stale: pull, then retryable 503; UCloud quarantine |
| Gateway pull (constants) | 2 s timeout; 2 s apart, doubling to 32 s | same | stands in for a late push |
| `autoscaler_interval_seconds` | 5 s | 5 s | probe, recovery and replay cadence |
| Direct probe (constant) | 3 s, 8 in parallel | same | quarantine recovery; unreachable-empty proof |
| `unreachable_stop_after_seconds` | 1800 s | 900 s | earliest unreachable-empty stop |
| Stale-route grace | max(3 x TTL, TTL + 60) = 360 s | 360 s | `node_lost` for routes of vanished jobs |
| Repeated reboot (constants) | 2 retirements in 24 h | same | soft-drain and retire |
| Reboot reaper (constants) | 32 per pass, 60 s each | same | frees old-boot registrations |
| Pending-delete replay | 16 per cycle, 8 parallel, 30 s each | same | delivers recorded deletes |
| `scale_down_idle_seconds`, builders | 600 s, 900 s | 300 s, 300 s | idle grace before drain |
| `max_stop_per_cycle` | 1 | 1 | stop budget |
| Storage-native detaches; `drain_on_park_moves_per_cycle` | 2; 4 per cycle | 2; 4 | park moves |
| UCloud full census | 300 s | n/a | final-state backstop |
| Loss diagnosis retention | 7 days | 7 days | how long 410 answers last |

Hetzner prod (`scripts/hetzner_prod/make_config.py`) also sets `max_nodes=3`
(env `MAX_NODES`) and builder `max_nodes=8`. No timer turns silence or a
provider status into loss, or terminates an occupied VM.

## Measurement

Events go to `metric_events` in the shared `metrics.sqlite`:
`vm_observed` (autoscaler, on any change of provider state, note, readiness,
freshness or `interrupted_at`; carries `phase` and the last known `node_epoch`),
`node_epoch_retired` (`retired_node_epoch`, `node_epoch`, `downtime_seconds`,
`retiring`), `sandbox_reboot_reap` (`status`, `confirmed`, `client_delete`)
and `node_heartbeat_pull` (`outcome`: `refreshed`, `epoch_changed`,
`unreachable`, `identity_mismatch`, `rejected` or `error`;
`receipt_age_seconds`). An interruption followed by the same epoch is a
readiness blip; one followed by `node_epoch_retired` is a reboot.

## Evidence

UCloud, 2026-09-07 to 09-26 (sources under `docs/reviews/` and
`docs/benchmarks/`):

| When (UTC) | Worker | What happened | New boot | Duration | Our action, outcome |
| --- | --- | --- | --- | --- | --- |
| 09-07 22:23:57 | 12383398 (dev) | Powered off in a 128-sandbox hot wake; no OOM, panic or shutdown record | yes | 34.5 s to RUNNING; API then crash-looped on ERROR volumes | None by a controller; disk kept 89 parked, 39 recovery-required, 663,552 MiB reserved; manual cleanup |
| 09-08 06:12:58 | 12383398 | Powered off in a c16 wake; abrupt stop | yes | 43.1 s | None; 61 parked, 67 recovery-required; manual cleanup |
| 09-08 06:47:28, 06:59:42 | 12383398 | Powered off in two c24 wakes | yes, both | 40.3 s, 43.8 s; agent about 3 min | None; services self-recovered |
| 09-18 07:36:30 | 12395318 (prod) | Powered off with active sandboxes | unknown | stopped after 2 s | Destructive stop; sandboxes lost |
| 09-18 15:15:34 | 12396030 (prod) | Suspended in a 256-way cold-start burst, unresponsive 11 s before | unknown | stopped after 1.25 s | 146 routes `node_lost` |
| 09-19 13:10:09 | 12396482 (prod) | Powered off with 103 sandboxes | unknown | stopped after 1.3 s | 103 routes lost; led to 410 `node_lost` |
| 09-20 before 13:55 | 3 prod workers | Power-offs; requests waited about 60 s before | unknown | not recorded | Stopped |
| 09-20 14:57-15:07 | 12397020/21/37/38 | Four power-offs in 10 min; a survivor's heartbeat process hit an order-4 allocation failure (virtiofs) | unknown | stopped after 2-5 s | Stopped; heartbeat hotfix; 09-21 review led to 0.5.67 (no destructive authority) |
| 09-21 to 09-22 11:41 | 12397503 | Silent but alive (heartbeat handler exception), no provider event | no | up to about 24 h | Quarantined, kept; recovered with 0.5.76 |
| 09-22 09:17 | gateway 12379311 | Suspended for storage quota, then powered off | yes | about 2 h 7 min | Operator freed quota; disk and PostgreSQL kept, private IP changed |
| 09-23 13:04-18:35 | 17 workers | 24 staleness episodes under pressure; one gateway-caused, simultaneous | no | 30-76 s (lower bounds) | None; all survived |
| 09-21 to 09-26 | all workers (0.5.67+) | No suspension, no recorded quarantine, no second epoch | - | - | Production moved to Hetzner 09-26/27 |

All 4 UCloud suspensions allowed to play out were real reboots with disk and
registrations intact (rule 2). The 9+ production losses were stopped 1-5 s
after the report, so their outcome is unknown. Every silent-but-alive episode
survived (rule 1). A July/August claim that resume "clears the ephemeral guest"
kept no raw evidence.

Hetzner: no unplanned reboot, power-off or server loss is recorded. Planned
reboots kept disk and volumes (08-12 qualification; 09-28 gateway resize, 43 s,
new kernel). The 09-30 incident was contention only, one epoch per worker.
Worker-hours are not recorded, so no rate bound exists.

## Open unknowns

- The cause of the UCloud power-offs (host or guest; the signal sender and
  virt-launcher or QEMU exit records were never obtained), whether the stopped
  production workers would have come back, and why losses stopped after 09-20
  15:07 (heartbeat hotfix, 0.5.66 memory tuning, lower load or chance).
- Whether UCloud ever reports `SUSPENDED` while the guest keeps its boot; never
  observed, so the same-boot recovery path guards a hypothetical case.
- Whether surviving checkpoints and volumes restore after a real reboot (the
  harness keeps devices and mounts across its reboot), and post-reboot drift:
  `/tmp` lost, swap appeared, SSH port, IP or kernel changed.
- The real controller behaviour on a UCloud reboot (only synthetic tests), the
  cause of 1-5.6 s guest clock gaps under pressure, how often clients meet the
  silent-worker DELETE path, and the Hetzner failure rate.
- Known gaps in these rules: an occupied worker that never returns waits for an
  operator; nothing alerts on an `unavailable` node; an untimed UCloud
  suspension re-quarantines every cycle; Hetzner `migrating` makes a running
  worker unschedulable; a reap failing longer than the 7-day loss retention is
  forgotten.

## Observe-only UCloud experiment

Goal: on the next UCloud deployment, separate E0 gateway-side staleness, E1
unreachable on the same boot, E2 suspension with the guest intact, E3 reboot
with disk intact, E4 reboot without disk, and E5 a VM that never returns,
without any destructive decision.

**Setup.**

1. Deploy a disposable deployment id running these rules, synthetic load only,
   with logged quota headroom of at least N x 2,000 GB (a quota suspension, as
   on 09-22, would confound the result).
2. `systemctl stop ucloud-sandbox-autoscaler.service`, then `systemctl mask`
   it; `max_stop_per_cycle=0` is not a kill switch. The gateway stays up: its
   pull, epoch retirement and reaper are under observation.
3. Provision 6 workers of the production shape (32 vCPU, 96 GiB, 2,000 GB): 4
   under the loads that preceded losses (`scripts/benchmark_sandbox_density.py`
   128-sandbox hot wake at c16/c24; `scripts/live_relay_load_benchmark.py`
   pressure256 and natural512), 2 idle. Each sandbox runs a continuity marker
   (nonce, counter, memory hash); keep some locally parked.
4. Run at least 24 h (about 96 loaded and 48 idle worker-hours); extend to 72 h
   if no suspension is seen. Reference rates: 4 reboots in about 9.5 h on the
   dev VM, at least 9 losses in about 3 days in production.

**Samplers**, on the gateway, appending JSONL with realtime and monotonic time.
`$CONFIG` is the deployment JSON, `$DATA_ROOT` its `data_root`, and
`$NODE_TOKEN` the content of its node control token file.

1. Every 30 s, the controller counterfactual: `uv run ucloud-sandboxes
   autoscaler --once --config "$CONFIG" --output json` (no `--execute`, so no
   provider, routing or quarantine writes). Keep `nodes[].phase`,
   `quarantined_job_ids`, `unreachableNodeProbes` and `stopJobIds`.
2. Every 10 s per job, including the gateway's: `retrieve_job` through
   `UCloudClient(SessionStore(...))` with `ucloud_settings(config)`, logging
   `status.state` and each `updates[]` `state`, `status` and `timestamp`.
   Back off on 403.
3. Every 2 s: `uv run ucloud-sandboxes heartbeats --config "$CONFIG" --output
   json` (epoch, receipt, inventory, labels).
4. Every 5 s per worker, no faster (`/v1/heartbeat` runs expiry cleanup on the
   node): DNS, TCP connect to `:8090` (1 s), `GET /health` (2 s), then
   `curl -m 3 -H "Authorization: Bearer $NODE_TOKEN"
   http://<node>:8090/v1/heartbeat`.
   Classify DNS, refused, connect or read timeout, disconnect, HTTP status.
5. Every 1 s, a guest beacon from a unit with `WorkingDirectory=/` (`boot_id`,
   clocks, steal, PSI, MemAvailable, swap, dirty pages, new kernel lines) and
   gateway PSI, CPU, event-loop lag and `/v1/nodes` latency; every 15 min,
   `scripts/read_ucloud_observability.sh <gateway-job-id> --window 15m
   --compact`.
6. Every 10 s: `sqlite3 "file:$DATA_ROOT/metrics.sqlite?mode=ro" "SELECT
   sequence, timestamp, kind, data_json FROM metric_events WHERE sequence >
   $LAST AND kind IN ('vm_observed', 'node_epoch_retired',
   'sandbox_reboot_reap', 'node_heartbeat_pull')"`. Pulls refresh the receipt,
   so measure push staleness from `node_heartbeat_pull` `receipt_age_seconds`.
7. On each epoch change or suspension, read-only from the worker:
   `journalctl --list-boots`, `journalctl -k -b -1 -n 500`, `last -x`, pstore,
   `kernel.panic`, `swapon`, `uname -r`, IP and SSH port, whether `/tmp`,
   `/var/tmp` and `/var/lib/ucloud-sandboxes` survived, and marker nonces. The
   reaper deletes lost registrations within seconds, so take their states from
   `sandbox_reboot_reap` and the 410 loss rows. Ask UCloud for the VMI/QEMU
   exit and kill-sender records at that time.

Optional, annotated controls on idle workers: a 90 s nftables drop of
gateway-worker traffic (expect E1), an in-guest `systemctl reboot`, and a UCloud
suspend and unsuspend.

**Episode classes.** E0: many workers stale at once while probes and beacons
are fine. E1: unreachable, same boot, provider RUNNING throughout. E2:
`SUSPENDED` then `RUNNING` with the same epoch and nonces. E3: new epoch, disk
and registry intact. E4: new epoch, disk or registry gone. E5: no
authenticated heartbeat within 30 min, or a final state without our stop.

**Decision rules, fixed in advance.**

- Always: zero counterfactual stops (`stopJobIds`) for E0, E1 or E2, and an E1
  quarantine clears within one cycle of recovery.
- E1: silence stays non-destructive; if p99.9 of loaded silence exceeds 120 s,
  raise `gateway_heartbeat_ttl_seconds` above it.
- Any E2: keep same-boot quarantine recovery and retryable 503s. A later timer
  that turns E2 silence into loss must wait at least T_fail = max(120 s, 1.5 x
  p99 E2 duration), and termination at least T_term = max(30 min, 3 x max E2
  duration).
- No E2 and at least 3 suspensions, all E3: these reboot rules stand, and two
  extensions are justified: answer 410 `node_lost` for running work as soon as
  `SUSPENDED` follows `RUNNING`, and terminate and replace a VM with no
  authenticated heartbeat within T_return = 2 x p99 (suspension to agent
  ready), about 6-10 min from the dev VM's 3 min.
- Any E4: local parks count as lost (re-adoption already needs them in the new
  inventory); prefer replacement if the guest returns reconfigured (kernel,
  swap, modules).
- Any E5: terminate after T_return.
- No suspension at all: keep these rules; report the 95% upper bound 3/W for W
  worker-hours.

**End.** Capture forensics, terminate the pool as the operator, and keep the
sanitized evidence with a SHA-256 manifest under
`docs/benchmarks/ucloud-loss-observation-<date>/`.
