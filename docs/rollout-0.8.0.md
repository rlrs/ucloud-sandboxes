# 0.8.0 rollout plan (Hetzner production)

Status: plan, not executed. Production runs 0.7.0 (gateway 77.42.92.27; workers
scale from zero). The release candidate is branch `rl-scale-plan` at version 0.8.0. The
node-failure fixes are not in it; they ship as 0.8.1 after review. Placement wiring (C4.3)
and later items go out separately in 0.9.0, after their own canary, because
they change PostgreSQL placement behavior.

## What ships, and its default state

| Change | Default in 0.8.0 | Persistent or wire effect |
| --- | --- | --- |
| Signed exec sessions (C4.1 part 1) | On | New session-id shape. Old workers ignore the header and keep durable routes. |
| Node create phase timings (C0.2) | On | Extra fields in create timings. |
| EROFS metadata hints and trace prefetch (C2.2/C2.3) | **On** (`immutable_environments.prefetch_enabled`) | Builders attach hint annotations, which old workers ignore. Workers keep node-local trace files. |
| EROFS layout 2 readers, mtimes and `--MZ` (C2.11/C2.12) | Readers on, writer **off** (`preserve_mtimes: false`) | Workers and gateways accept layout 2. |
| Pause tier (C1.1) | **Off** (`sandbox.direct_pause_tier`) | New metrics fields, defaulting to 0. |
| In-process heartbeat sender (C4.4) | On | VM init retires the old oneshot timer. |
| Create pipeline (C5.2) | On | Three commits per create, a netns pool in `network-slots.json`, and a `direct-registry.sqlite.owner` sidecar. |
| Commit worker and builder halves (C3.1) | Unreachable: no gateway route | — |
| Guest agent (C5.1 step 1) | Not wired | The bundles keep the qualified 0.7.0 init binary; only the unwired `guest_agent.py` uses the new `agent` and `files stat` modes. |
| Program scheduler removed (C4.7) | — | Policy keys removed: the config must be re-rendered. |
| Relay and placement-queue cancellation fixes | On | — |
| Node-failure fixes (reboot = process loss, quarantine, Hetzner off ≠ lost, delete replay) | Not in 0.8.0 | Ship in 0.8.1. They fix 0.7.0 behavior and nothing in 0.8.0 depends on them. |

## Ordering constraints

- **Gateway before workers.** An older gateway rejects heartbeats that carry
  the new `environment_io` or pause-tier fields.
- **Config with binaries.** A `deployment.json` rendered by this release has
  keys that 0.7.0 rejects, such as `preserve_mtimes` and `prefetch_enabled`.
  Render it only for 0.8.0 hosts. The 2026-09-28 render already fails to load
  at HEAD, so re-rendering is mandatory anyway.
- **Workers come from a new golden snapshot.** Hetzner workers boot from a
  snapshot that contains the node bundle. Live workers do not change in place,
  so the fleet is replaced by scaling it to zero.
- **Layout-2 writer only after every worker runs 0.8.0, and builders have
  erofs-utils 1.9+.** That is a later, separate step (below).

## Steps

1. **Preflight, read-only.** The fleet must be idle: no routes, no running
   builds, no relay in-flight work. Confirm the off-host evidence and
   PostgreSQL backups (`scripts/backup_relay_postgres.py`).
2. **Build.** The wheel from `rl-scale-plan`. Repack the live sandbox and
   builder bundles on the gateway with `scripts/repack_node_bundle.py`,
   replacing only the agent wheel and asserting that native files and the
   dependency closure are unchanged. Run
   `scripts/verify_installed_wheel.py`, which must show
   `ucloud_sandboxes/gateway/`, and the full suite: `scripts/run_tests.py`,
   with and without PostgreSQL. Run the Go tests and the local fleet harness.
3. **Derive the config from the live one.** Do not re-render with
   `make_config.py`: the live `/etc/ucloud-sandboxes/deployment.json` carries
   production tuning the script does not know (for example a 32 GB sandbox
   Docker store, snapshot `436561313`, `node_package_root`). Copy it, then:
   - remove `policy.program_aware_autoscaling_enabled` (it was `false`),
     `policy.model_wait_capacity_weight` and `policy.model_wait_max_headroom_nodes`;
   - set `node_package_root` to the 0.8.0 bundle directory and
     `provider.sandbox_image`/`builder_image` to the new snapshot.
   New keys stay at their defaults (prefetch on, layout-2 writer off, pause
   tier off), so nothing else is added. Validate with the 0.8.0 loader.
4. **New worker snapshot.** Follow docs/hetzner.md "New snapshot", including
   the park/wake canary on the snapshot source.
5. **Upgrade the gateway** with `upgrade-gateway.sh 0.8.0`, while the fleet is
   idle. Health check: `/healthz` reports 0.8.0, and the placement, relay and
   autoscaler units are active.
6. **Canary.**
   - Run one worker through the normal autoscaler: create, exec (the session
     id starts with `xr1.`), files, park, wake, delete.
   - Run an EROFS image's first create and check the prefetch counters in the
     heartbeat `environment_io`.
   - Run `scripts/bench_rl_scale.py` with the `cold`, `warm` and `burst`
     scenarios at small counts.
7. **Watch for 24 h.** Heartbeat staleness, node_lost and quarantine events
   (new metrics), create p95, and relay delivery.
8. **Take the W0 baseline** with `scripts/bench_rl_scale.py`, every
   scenario, at the declared load, and record it in `docs/benchmarks/`.
   This baseline covers the control plane at moderate load. The realistic
   500+-rollout start is the deferred W9 `rollout` scenario.

## Rollback

- **Gateway:** reinstall the 0.7.0 wheel with the previous rendered config
  (keep a copy). No PostgreSQL schema changes ship in 0.8.0.
- **Workers:** scale to zero and boot the previous snapshot.
  - Registry rows written by 0.8.0 are readable by 0.7.0: there is no schema
    bump, and creates go `planned` → `rootfs_ready`, which old code handles.
  - A pooled netns slot leased after a rollback leaves only an empty
    `ucloud-pool-<slot>` name behind.
  - Exception: an OS crash inside the unsynced `commit_owned` window,
    followed by a rollback. This is documented in the C5.2 evidence.
- **Do not roll back after the layout-2 writer is turned on.** 0.7.0 workers
  cannot read layout-2 components.

## Later steps (each its own change)

- **Layout-2 writer.**
  1. Builders on erofs-utils 1.9+.
  2. Set `immutable_environments.preserve_mtimes: true`.
  3. Republish Python-heavy images: foundations, then anchors, then task
     images (docs/image-foundations.md).
  4. Remove pre-C2.12 layout-2 `layer-*` tags from qualification registries.
- **Pause tier canary.**
  1. One fresh worker with `sandbox.direct_pause_tier: true` and a `swap_gb`
     from the W0 interference matrix, zswap off.
  2. Gate it with `bench_rl_scale.py` density-at-latency.
  3. Enable fleet-wide.
  4. Delete `warm_park`.
- **0.9.0:** placement wiring (C4.3), the commit gateway route (C3.1), and
  gateway split steps 7–9.

## Execution log (2026-10-02)

Steps 1–6 ran on 2026-10-02; the 24 h watch and the W0 baseline (steps 7–8) have not.

- **Preflight.** Idle fleet: only the gateway, 0 sandboxes, no relay or lifecycle work. The
  live gateway was 0.7.0 plus files hot-copied into `site-packages`; every one matched a
  commit already in `main`, so the 0.8.0 wheel dropped no production fix.
- **Build.** Wheel `7d6bc452…`. Both live bundles were repacked on the gateway
  (`/work/ucloud-sandboxes/release-0.8.0-20261002/package_080.py`): sandbox `89370316…`,
  builder `a86a5ca5…`, native files and agent dependencies byte-identical.
- **Config.** Derived from the live file: the three removed policy keys, `node_package_root`
  and later `provider.sandbox_image`. Builders stay on `436561313`.
- **Gateway.** `gateway_upgrade_080.py apply`: about 9 s of downtime while idle, with the
  backup in `rollback/`. `/healthz` reports 0.8.0.
- **Snapshot `438664008`.** Built from `436561313` on a CPX32 after the gateway upgrade, so
  VM init ran 0.8.0. The CPX32 needs a source-only config with
  `sandbox.direct_disk_headroom_mb` 24576 and `immutable_environments.cache_bytes`
  8 GiB: production values are sized for a CCX63's disk. VM init must run as the `ucloud`
  user, because the trust file is owned by `ucloud`. The source passed the lifecycle canary
  (create 0.67 s, park 0.29 s, wake 0.31 s).
- **Canary through the autoscaler.** A fresh CCX63 booted from `438664008`, with the EROFS
  task image `terminal-resolver-task_02282-adba4e22`. The first create, including the cold
  worker boot, took 61.3 s. On the warm worker: create 0.74 s, first exec 0.07 s, park
  0.30 s, wake 0.46 s, memory and disk state intact, signed (`xr1.`) exec sessions.
- **Prefetch counters** (`heartbeat.runtime_metrics.environment_io`, not in `/v1/nodes`):
  `prefetch_enabled` true, 117 hits, 8.9 MB downloaded, no corruptions or retries.
  `metadata_hint_absent` 10: images published before 0.8.0 builders carry no hints until
  they are republished. `trace_recordings_started` 10 but `traces_recorded` 0 and
  `trace_chunks_recorded` 0, minutes after the canary: trace recording does not see these
  reads in production. To investigate before relying on C2.3.
- **Canary caveats.** Production nodes need `network: bridge` and a keep-alive command.
  Images vary in their user and `$HOME`, so the canary writes to the first writable disk
  directory.

## 0.8.1 (2026-10-02)

Same procedure, from the 0.8.0 release directory. Scripts are in
`/work/ucloud-sandboxes/release-0.8.1-20261002`.

- **Build.** Wheel `32c93c29…` from `9ab452f`. Bundles repacked from 0.8.0: sandbox
  `cc5780c6…`, builder `21346078…`, native files and dependencies unchanged.
- **Gateway.** `gateway_upgrade_081.py apply`; the only config change is
  `node_package_root`. Backup in `rollback/`.
- **Snapshot `438710747`.** Built from `438664008` on a CPX32 with the same source-only
  overrides. Run twice on the source, the canary showed the trace fix on a real kernel:
  the first run saved 5 traces (36 chunks) at delete, and the second found all 5 and
  started replay. Replay fetched nothing, because that node's cache already held the
  chunks. Prepare now leaves no ucloud units or environment config in `/etc`.
- **Canary through the autoscaler.** A fresh CCX63 from `438710747`. The first create,
  including the cold worker boot, took 42.8 s (61.3 s on 0.8.0). Warm: create 0.54 s,
  first exec 0.03 s, park 0.16 s, wake 0.18 s, state intact. Traces were saved on the
  first run and replayed on the second.
- **Operations.** `hz.py` now reads its API key from `build/hetzner-prod/hetzner.env`.
  Every production step ran as one plain `scripts/hetzner_prod/gw '…'`, `gscp` or `hz.py`
  command, so the permission rules in `.claude/settings.local.json` apply.

## 0.8.2 (2026-10-02)

The environment backend's EAGAIN fix (worker side) plus the opt-in upstream mirror
code (off). Scripts are in `/work/ucloud-sandboxes/release-0.8.2-20261002`.

- **Build and gateway.** Wheel `c2cc213c…`. Bundles repacked from 0.8.1: sandbox
  `4c879e83…`, builder `116a9889…`. `gateway_upgrade_082.py apply`.
- **Snapshot `438728121`.** Built from `438710747`. The source passed the canary twice.
- **Incident during the canary.** The first autoscaled canary create got a
  non-retryable 409 `placement_command_rejected` ("placement command incarnation no
  longer exists") after about 6 minutes:
  - placement had chosen the just-deleted snapshot source (job `168386564`), whose
    last heartbeat was still fresh;
  - the create retried 190 times against it;
  - provider-confirmed loss then recorded `node_lost` and deleted the route.

  Two follow-ups:
  1. **Procedure:** drain the source (`POST /v1/drain` on its node agent, with the
     node-control token) before sanitizing it.
  2. **Bug:** a create whose worker is lost before the create starts should be
     re-placed, or fail as retryable, not end with a hard 409.

  The rerun passed on a fresh CCX63: cold create 57.9 s, then exec, park and wake.
- **Burst smoke (first live run of the `rollout` scenario).** 48 creates at once on
  one warm CCX63, `--fleet-state warm-empty`, sleep mode, 2 turns:
  - all 48 succeeded, with no EAGAIN;
  - time to ready p50 9.1 s, p95 15.8 s; first command p50 10.2 s, p95 21.1 s;
  - SWE-smith was slowest, up to 22.9 s;
  - 25 of the 48 were recipe-family fallbacks to their prepared base (no task build).

  The report is in `build/bench-smoke-20261002/rollout-48.json`.

## 0.8.5: admission order and the pause tier

Status: deployed on 2026-10-03, steps 1–6 (execution log below). Step 7 is
deferred: no training runs until more RL-scale milestones land, so canaries are the
production signal. Before it, production ran 0.8.4: gateway
`77.42.92.27`, worker snapshot `438866767`, bundles in
`/work/ucloud-sandboxes/release-0.8.4-20261002`, `swap_gb` 0, pause tier off. The
contents and default states are in `CHANGELOG.md` under 0.8.5. One rollout ships the
code and turns the pause tier on for new workers (swap 64 GiB, zswap off): both need a
fleet replacement anyway, and rolling back only the pause tier is a config revert.

### Evidence

Unregistered CCX63 workers ran the relay pressure harness: 140 managed agents,
1.5 GiB heaps, 20 ± 5 s model waits, rollouts that end with a delete.
- Today's path, plus 0.8.5's admission order: 137 hibernated waits against 160,
  slowest wake 32 s against 57 s.
- Pause tier with swap and zswap off: every wait resumed, wake p95 0.13 s against
  14.2 s on today's path, no hibernation, no lost agent, swap at most 8.2 GB.
- Reports: `docs/benchmarks/admission-priority-2026-10-03/`,
  `pause-reclaim-2026-10-03/` and `memory-pressure-2026-10-03/`.

### Steps

1. **Preflight, read-only.** The fleet must be idle: 0 sandbox nodes, no routes, no
   builds, no relay work. Back up relay PostgreSQL (`scripts/backup_relay_postgres.py`).
2. **Build** the wheel from the release commit and stage
   `/work/ucloud-sandboxes/release-0.8.5-<date>/` with `package_085.py`,
   `gateway_upgrade_085.py`, `set_snapshot_085.py` and `set_pause_tier_085.py`. The
   scripts are in `build/release-0.8.5/` locally.
   - `package_085.py` repacks the 0.8.4 sandbox and builder bundles with the 0.8.5
     wheel. It asserts that native files and the dependency closure are unchanged.
   - Run `scripts/verify_installed_wheel.py` and the full suite.
3. **Gateway first.** Run `gateway_upgrade_085.py check`, then `apply`. Its only config
   change is `node_package_root`. 0.8.5 workers send new `ResidentWaitMetrics`
   fields, which a 0.8.4 gateway drops. `/healthz` must report 0.8.5.
4. **New worker snapshot** from `438866767`, as for 0.8.4: CPX32 source, source-only
   disk overrides, VM init as `ucloud`, the lifecycle canary on the source, then
   drain the source before sanitizing it. The snapshot is taken with the pause tier
   off; it is a VM init setting, not part of the image.
5. **Point new workers at it and turn the pause tier on.** Run `set_snapshot_085.py`,
   then `set_pause_tier_085.py`. The second changes exactly:
   - `sandbox.direct_pause_tier` → true;
   - `sandbox.swap_gb` 0 → 64;
   - `sandbox.direct_pause_tier_zswap` → false.

   It validates the new file with the installed loader before publishing.
   - **Cost:** 64 GiB of swap per CCX63. The sandbox disk budget falls from 609,280
     to 543,744 MB (11%).
   - **What VM init does:** creates the swap file and makes the RAM memory tmpfs
     swappable.
6. **Canary through the autoscaler**, one fresh CCX63:
   - VM init shows `swapon` 64G, zswap N, and a swappable application-memory tmpfs.
   - The 0.8.4 lifecycle canary passes: create, exec, files, park, wake, delete.
   - `bench_rl_scale.py rollout --think-mode relay --tasks 64` passes with:
     - `pauses` and `thaws` counting one per model wait, plus one pair per SDK
       status or log poll made during a pause;
     - relay overhead per call within 1.5× of
       `docs/benchmarks/rl-scale-relay-2026-10-03` at p95 (0.16 s on turns 0–6);
     - `pause_escalations` and `pause_reclaim_errors` at 0;
     - no lost agent.

   If it fails, put back `deployment.before-pause-tier.json`, replace the worker, and
   re-run the canary on 0.8.5 alone. That tells whether the pause tier or the code
   caused it.
7. **Watch the first training run** for:
   - `pause_reclaim_stalls`, `pause_escalations` and thaw time (`thaw_ms_max`);
   - memory PSI and swap use;
   - heartbeat staleness, create p95 and relay delivery.

### Rollback

- **Pause tier only:** put back `deployment.before-pause-tier.json`, restart the
  autoscaler, and replace the workers. 0.8.5 stays.
- **Workers:** also put back `deployment.before-snapshot.json` (snapshot
  `438866767`), then scale to zero.
- **Gateway:** `gateway_upgrade_085.py rollback`.
- No schema changes ship; the new heartbeat fields are additive.

**Still open.** Model waits still go through the gateway's park protocol (item 6,
`docs/benchmarks/rl-scale-relay-2026-10-03`). zswap stays off until the fleet's real
compression ratio is measured. `warm_park.py` can be deleted once the pause tier is the
only path.

### Execution log (2026-10-03)

Steps 1–6 ran on 2026-10-03. Step 7 is deferred until training starts, after further
RL-scale milestones.

- **Preflight.** Idle fleet: 0 sandbox nodes, no routes, builds or relay work. No relay
  PostgreSQL backup: relay PostgreSQL is not a container on the gateway, so
  `scripts/backup_relay_postgres.py` does not apply. 0.8.5 ships no schema change.
- **Build.** Wheel `a0c1dc76…` from `f9e1fa9` (later commits touch docs only). Bundles in
  `/work/ucloud-sandboxes/release-0.8.5-20261003`: sandbox `ee7ea273…`, builder
  `2079c46b…`. Only the agent wheel changed.
- **Gateway.** `gateway_upgrade_085.py apply` at 18:36:54Z. `/healthz` reports 0.8.5;
  the gateway, relay and autoscaler are active.
- **Snapshot `439222185`** (`ucloud-sandboxes-sandbox-0.8.5-ubuntu-26.04-7.0.0-30`),
  built from `438866767` on a CPX32. It used the 0.8.4 source-only overrides, with the
  pause tier off (the live config still had `swap_gb` 0).
  - VM init ran as `ucloud`, and the agent reported 0.8.5.
  - The lifecycle canary passed twice: park 0.20–0.23 s, wake 0.23–0.26 s.
  - The source was drained, sanitized and snapshotted, then deleted.
- **Config.** `set_snapshot_085.py`, then `set_pause_tier_085.py`: `sandbox_image`
  `439222185`, `direct_pause_tier` true, `swap_gb` 64, `direct_pause_tier_zswap` false.
  Backups: `deployment.before-snapshot.json`, `deployment.before-pause-tier.json`.
- **Canary through the autoscaler.** A fresh CCX63 (job `168561613`).
  - VM init: a 64G swap file, zswap N, the application-memory tmpfs without `noswap`,
    and the node agent running with `--pause-tier`.
  - Lifecycle canary: create 61.0 s (cold boot), first exec 0.10 s, park 0.29 s, wake
    0.42 s.
- **Relay rollout:** `bench_rl_scale.py rollout --think-mode relay --tasks 64 --seed 1`,
  the same harness (`c59ab924…`) and selection as the 0.8.4 baseline.
  - Raw report:
    `docs/benchmarks/rl-scale-relay-2026-10-03/raw/relay-64-085.json`.
  - **Rollouts.** 57 of 64 succeeded. The 7 failures are the same images without
    Python 3 as in the baseline (exit 97, a harness limit). No agent was lost.
  - **Pause counters** (heartbeat deltas): `pauses` 641, `thaws` 638,
    `checkpoints_completed` 0, `pause_escalations` 0, `pause_reclaim_errors` 0,
    `pause_reclaims` 0. At peak, 26 of 63 sandboxes were paused and none parked.
  - **The pause gate, read correctly.** Pauses outnumber the 456 model waits because a
    read-only managed-process control exchange (an SDK job status or log poll) during
    a pause thaws for that exchange and pauses again (`direct_service.py`
    `keep_paused`). So there was one pause per wait, plus 185 poll re-pauses. Thaws
    take 17 ms on average (`thaw_ms_total` 10,819), with no prefetch and no reclaim:
    one node at 64 rollouts has no memory pressure.

  | per call, s | 0.8.4 baseline | 0.8.5, pause tier |
  | --- | ---: | ---: |
  | relay overhead, turns 0–6: p50 / p95 / max | 0.053 / 0.16 / 0.50 | 0.073 / **0.229** / 0.47 |
  | answer → agent resumes, turns 0–6: p50 / p95 | 0.018 / 0.025 | 0.038 / 0.046 |
  | relay overhead, turn 7: p50 / p95 / max | 1.31 / 1.56 / 2.54 | 1.40 / 2.57 / 3.76 |

  - **Overhead gate:** p95 0.229 s against the 0.24 s gate (1.5× of 0.16 s). It passes,
    narrowly. The ~20 ms added per call is the thaw on the answer path.
  - **The final-turn hold is longer:** turn 7 p95 is 2.57 s against 1.56 s. The cause
    of that hold was not pinned before 0.8.5 (`rl-scale-relay-2026-10-03`), and one
    run per version cannot tell a regression from noise. Watch it in step 7.
- **Not exercised here:** reclaim to swap and admission under pressure. The
  unregistered 140-rollout runs validated them
  (`docs/benchmarks/admission-priority-2026-10-03`). The first training run is their
  production test.

## 0.8.6: node-local model waits

Status: deployed on 2026-10-03, together with the gateway-only 0.8.7 and 0.8.8 fixes
found by its canary (execution log below). Before it, production ran 0.8.5 (snapshot
`439222185`, bundles in `/work/ucloud-sandboxes/release-0.8.5-20261003`, the pause tier on).
No training runs yet: canaries are the production signal. Contents are in `CHANGELOG.md`
under 0.8.6. The design is in `docs/node-local-model-waits.md`.

### Steps

1. **Preflight, read-only.** The fleet must be idle: 0 sandbox nodes, no routes, no
   relay work.
2. **Build** the 0.8.6 wheel and stage `/work/ucloud-sandboxes/release-0.8.6-<date>/`:
   - `package_086.py` repacks the 0.8.5 bundles with it;
   - `gateway_upgrade_086.py`, `set_snapshot_086.py` and `set_local_waits_086.py`.
3. **Gateway first** (`gateway_upgrade_086.py check`, then `apply`).
   - After installing the wheel, and before any service starts, apply runs
     `python -m ucloud_sandboxes.shared_control migrate` as the services' user. It adds
     `relay_requests.local_wait`, a metadata-only `ADD COLUMN ... DEFAULT false`.
   - 0.8.6's relay writes that column on every enqueue, so it must exist first.
   - **Rollback** restores the 0.8.5 venv and config. The column stays: 0.8.5 neither
     reads nor writes it.
4. **New worker snapshot** from `439222185`, as for 0.8.5: CPX32 source, VM init as
   `ucloud`, the lifecycle canary, drain, sanitize.
5. **Point workers at it, then turn the switch on.**
   - `set_snapshot_086.py`.
   - `set_local_waits_086.py` sets `sandbox.direct_local_model_waits` true and restarts
     the relay and the autoscaler. The relay reads the switch at start; workers read it
     at VM init.
6. **Canary through the autoscaler**, one fresh CCX63:
   - **On the worker:**
     - the node agent runs `--local-model-waits`;
     - `nft list table inet ucloud_local_wait` logs `10.42.0.2:8092` in both
       directions.
   - The lifecycle canary passes.
   - **Relay benchmark on the private plaintext path:**
     `bench_rl_scale.py rollout --think-mode relay --tasks 64 --sandbox-relay-url http://10.42.0.2:8092`.
     Gates:
     - pauses at least one per model wait;
     - no lost agent, other than the images without Python;
     - relay overhead at most the 0.8.5 canary's (p95 0.229 s on turns 0–6);
     - no `relay_lifecycle` park rows for these requests;
     - every wake row closed without a dispatch.
   - **Hibernation fallback.** During a second, long-think run (`--think-seconds 20:25`),
     explicitly park (hibernate) a few of its sandboxes mid-call. Gates:
     - each parked rollout still finishes, its agent's retry reattaching to the stored
       answer;
     - one dispatched wake per hibernation, and no duplicate model sample.

   **If the canary fails,** put back `deployment.before-local-waits.json` and replace
   the worker. The relay and nodes then take today's path, with 0.8.6 code.

### Rollback

- **Switch only:** put back `deployment.before-local-waits.json`, restart the relay
  and the autoscaler, and replace the workers.
- **Workers:** also put back `deployment.before-snapshot.json`.
- **Gateway:** `gateway_upgrade_086.py rollback`.

### Execution log (2026-10-03)

- **Preflight:** idle; gateway 0.8.5.
- **Build:** wheel `690bdf1c…`. Bundles: sandbox `0c456903…`, builder `427f6fda…`, with
  only the agent wheel changed.
- **Gateway:** `gateway_upgrade_086.py apply` at 20:44:40Z. The relay migration ran
  (`migrated: true` on `ucloud_shared_prod`). `/healthz` reports 0.8.6, and the four
  services are active.
- **Snapshot `439244700`** from `439222185`. Its source config had swap and the pause
  tier off. The lifecycle canary passed twice.
- **Config:** `set_snapshot_086.py`, then `set_local_waits_086.py`. The switch is on,
  and the relay and autoscaler were restarted.
- **Autoscaler canary**, CCX63 `10.42.0.3`:
  - `--pause-tier --local-model-waits`, NFLOG on `10.42.0.2:8092` in both directions,
    64G of swap, zswap N.
  - The lifecycle canary passed.
  - **Relay benchmark** (`--sandbox-relay-url http://10.42.0.2:8092`):
    - 57/64 (the same 7 images without Python);
    - overhead on turns 0–6 p50 0.056 s, p95 0.165 s;
    - 688 pauses for 456 waits, no escalations;
    - **but all 456 requests had `local_wait` false,** with dispatched park and wake
      rows. The benchmark's agents use the relay's HTTP tunnel, which 0.8.6 did not
      mark local. **Fixed in 0.8.7, a gateway-only upgrade.**
- **0.8.7, gateway only** (20:58:38Z). The tunnel takes the local path; workers stay
  on the 0.8.6 bundle.
  - **Relay benchmark again** (`rlbench-0236f2e80111`):
    - all 456 requests had `local_wait` true;
    - no park rows, and 456 wake rows closed without a dispatch;
    - answer → agent p50 / p95 22 / 30 ms, against 38 / 54 ms;
    - overhead on turns 0–6 p95 0.141 s;
    - **the final-turn hold is gone**: turn 7 p50 0.038 s, against 1.3 s;
    - 57/64 rollouts, 519 pauses, no escalations.
  - **Hibernation fallback** (`rlbench-96876e590159`, 16 tasks, 20–25 s thinks): four
    sandboxes were parked (hibernated) mid-call.
    - All four rollouts finished all 8 turns: each cut call succeeded on its second
      attempt, about 5.8 s over its think time (the 5 s grace plus the restore).
    - 13/16 rollouts finished; the 3 failures are images without Python.
    - **But 8 wakes were dispatched for 4 hibernations.** The call after each restore
      was not paused (the sandbox was busy after its restore). Its guest read the
      answer and closed before the relay checked, so the relay found its socket
      closed and counted the answer unacknowledged. The wakes were harmless no-ops.
      **Fixed in 0.8.8** by probing a duplicated socket.
- **0.8.8, gateway only** (21:13:19Z). The relay probes acknowledgment on a duplicated
  socket. **Fallback canary from a cold fleet** (`rlbench-6880cfb95a62`): four sandboxes
  were hibernated mid-call.
  - All four rollouts finished all 8 turns, each cut call succeeding on its retry
    after about 5.8 s.
  - **Exactly 4 dispatched wakes, and 100 answers acknowledged without one.**
  - Other calls' overhead p50 / p95 0.037 / 0.122 s.
  - 13/16 rollouts finished; the 3 failures are images without Python.
- **Result:** production runs node-local model waits for agents on the private
  plaintext relay. A wait costs no gateway work and no park row. Waits through the TLS
  ingress keep the relay-driven park.


## 0.9.0: the production chunk store

Status: deployed on 2026-10-03 (execution log below). Before it, production ran
gateway 0.8.8 with workers on the 0.8.6 bundle (snapshot `439244700`).
- **What it adds.** The chunk store's production infrastructure, and nydusd in
  the worker bundle.
- **What it leaves alone.** Every sandbox still mounts its EROFS root, because
  no `image_roots` row is switched.
- **What it enables.** M2's waves can start
  ([chunk-store-m2-plan.md](chunk-store-m2-plan.md) §5).

Contents are in `CHANGELOG.md` under 0.9.0.

### Steps

The kit is in `/work/ucloud-sandboxes/release-0.9.0-<date>/`.

1. **Preflight.** The fleet must be idle.
2. **Build.**
   - `package_090.py` repacks the 0.8.6 bundles with the 0.9.0 wheel and adds
     the pinned nydusd to the sandbox bundle (`nydusd-pinned/`, from
     `runtime/nydusd/build_pinned.sh`).
   - The gateway's installed 0.8.8 lacks the pin constants that
     `repack_node_bundle.py` imports. So it runs with `PYTHONPATH` set to the
     unpacked 0.9.0 wheel; the dependencies are unchanged since 0.8.6.
3. **Gateway:** `gateway_upgrade_090.py check`, then `apply`. This points
   `node_package_root` at the kit.
4. **Config:** `set_chunk_store_090.py`. It is derived from the live config,
   not `make_config.py`, and adds `immutable_environments.chunk_store`:
   - S3 prefix `production/chunks`;
   - index and store node on `10.42.0.200`;
   - a 240 GiB extent cache;
   - the nydusd pin.

   It also makes the index tokens as `ucloud` and restarts the autoscaler.
   - `attach_concurrency` stays 1: EROFS attach is still the Python miss
     path.
   - `dispatch_roots` stays off.
5. **Store node.**
   - Create it: `hz.py server sandboxes-store-1 ccx43 <worker snapshot> 10.42.0.200 public`.
   - Then run `init-vm <id> --role store` as `ucloud`, using `runuser -p`
     with `hetzner.env` sourced: the tokens are owned by `ucloud`.
   - Check `/healthz` on ports 5090 and 5091, and `/v1/metrics`.
6. **Worker snapshot** from `439244700`, as for 0.8.6:
   - a CPX32 source with the source-only config;
   - the lifecycle canary on the source;
   - drain, then sanitize: remove the 0.8.6 init package and run
     `prepare_hetzner_snapshot.sh`.

   Then `set_snapshot_090.py`.
7. **Autoscaled canary.** The worker must show:
   - the 0.9.0 agent;
   - `environment-root-dispatch-v1` and `environment-rafs-v1`;
   - the pinned nydusd and `--chunk-store-url`;
   - no `--attach-concurrency`;
   - the pause tier, swap and local waits unchanged.

   The lifecycle canary must pass.
8. **Root dispatch with an empty table:** `set_dispatch_roots_090.py`, then
   `dispatch_check_090.py <images…>`. Every create must carry its
   annotation's root, then run and delete cleanly. Then run the lifecycle
   canary again.

### Rollback

Each step has its own backup: `deployment.before-dispatch-roots.json`,
`deployment.before-snapshot.json` and `deployment.before-chunk-store.json`.
- **Gateway:** `gateway_upgrade_090.py rollback`.
- **Store node:** it holds only a cache, and nothing reads it until a wave
  switches. Delete it with `hz.py delete-server sandboxes-store-1`.

### Execution log (2026-10-03)

- **Build.**
  - Wheel `b7d13da9…` (commit "0.9.0: the production chunk store's release").
  - Bundles: sandbox `fae7a5bb…`, with nydusd `ba7ac636…`; builder
    `649461e7…`.
  - Native files and dependencies unchanged.
- **Gateway** 0.9.0 at about 22:52Z: healthy, the relay migration a no-op.
- **Config:**
  - `chunk_store` added;
  - tokens made as `ucloud` (`serve-chunk-index` exited 78 as designed);
  - `dispatch_roots` false and `attach_concurrency` 1.
- **Store node `sandboxes-store-1`** (CCX43, server `168580830`,
  `10.42.0.200`):
  - store init in 4.7 s;
  - `ucloud-chunk-store` and `ucloud-chunk-index` active, bound to the
    private address only;
  - 321 GB of disk free for the 240 GiB cache, and 61 GB of RAM.
- **Snapshot `439298797`** from source `168580907`:
  - the source advertised both M2 capabilities;
  - its nydusd sha matched the pin;
  - the lifecycle canary passed: create 0.91 s, park 0.26 s, wake 0.34 s.
  - The source was drained, sanitized, snapshotted and deleted.
- **Autoscaled canary.** A fresh CCX63 `10.42.0.3` (job `168581195`).
  - Agent 0.9.0, with both capabilities.
  - nydusd `ba7ac636…`, `--chunk-store-url http://10.42.0.200:5091`, and
    serial attach.
  - The pause tier, local waits, 64G of swap and the `ucloud_local_wait`
    table are all present.
  - The store node is reachable from the worker.
  - Lifecycle canary passed; create took 57.9 s, including provisioning
    from zero.
- **Root dispatch on** (an empty table, no running sandboxes).
  `dispatch_check_090.py` on 4 images: the terminal-resolver alias, two
  shared task images and a precomputed source.
  - All 4 creates carried `environment_root`, equal to the manifest
    annotation.
  - All 4 ran a command and deleted cleanly.
  - The lifecycle canary then passed: create 0.74 s, park 0.30 s, wake
    0.43 s.
- **Result.**
  - Production has a chunk store, with nothing in it yet.
  - Every worker can read RAFS through nydusd.
  - Every create pins its root.
  - **Next is M2 wave 1:**
    - convert on disposable converters;
    - `chunk-migrate record`;
    - warm the store node;
    - switch.

## 0.9.1 to 0.9.3: what M2 wave 1 found (2026-10-04)

The findings are in [chunk-store-m2-plan.md](chunk-store-m2-plan.md) §5.2.

**0.9.1 and 0.9.2: the store node only.**
- Built with `package_091.py` / `package_092.py`: the 0.9.0 sandbox bundle with
  the new wheel. Bundles `33645ae1…` and `4cae6041…`.
- Deployed by store-role `init-vm` again, as `ucloud`. The extent cache
  survives (39.7 GB kept).
- The converter took 0.9.1's index client through `PYTHONPATH`.
- The gateway and workers stayed on 0.9.0.

**0.9.3: gateway and workers.**
- **Build.** Wheel `0588a5a8…`. Bundles: sandbox `3e898e08…` (nydusd
  unchanged), builder `4148f4bb…`.
- **Gateway:** `gateway_upgrade_093.py` at 01:44:50Z.
- **Snapshot `439310398`** from `439298797`, through the usual source canary
  (create 0.92 s, park 0.27 s, wake 0.36 s), then `set_snapshot_093.py`.
- **Checked on a converter:** an attached nydusd device reports `io_timeout`
  600000 ms and `retry_limit` 8, and mounts and reads 362 MB.

**Wave 1.**
- Reverted at about 01:30Z, while S3 kept failing fills.
- Switched again with 0.9.3's warming switch: 142, then 3 more on rerun,
  145 in total.
- Cold-fleet canary: 40/40 rollouts, and no NBD timeouts or I/O errors.
- The converter `sandboxes-m2-conv-1` was deleted after wave 1.

## 0.9.4 to 0.9.6 (2026-10-04)

**0.9.4: gateway, workers and store node.**
- Snapshot `439378718`. The first snapshot, `439375977`, was **not sanitized**,
  so it is unused and should be deleted: a RAFS check run on the source
  before sanitizing left state that made `prepare_hetzner_snapshot.sh` fail,
  and the snapshot was taken anyway.
- A rebuilt source passed the canary and sanitized cleanly.
- The cold-fleet wave 1 canary passed 40/40.

**0.9.5: store node and converters only.**
- A converter's verification warm is filled first-to-evict.
- `chunk-migrate convert --shard`.
- Store-role init again, with the cache kept (42.5 GB). Wave 2 was stopped and
  restarted around it.

**0.9.6: gateway and workers, snapshot `439434667`.**
- **Why:** env-io's `LimitNOFILE` 65536. Two 512-rollout bursts on 0.9.4
  (`rlbench-6bf72456eacc`, `rlbench-2335c389db71`) lost 45 and 59 creates to
  EMFILE and timeouts. env-io sat at systemd's default of 1024 descriptors.
- **The first source run used a wrong server ID.** Its init failed, and
  sanitize refused before any destructive step. The canary then ran on an
  autoscaled worker, which booted snapshot `439378718` and installed the
  0.9.6 bundle at init. It passed.
- **The second source run** passed both checks: an env-io open-file limit of
  65536, and the canary.
- **512 bursts after 0.9.6:** 511/512 from zero and 511/512 on a warm fleet.
  The one failure is the catalog gap. env-io peaked at 1217 descriptors.

## Production moves to UCloud: 0.9.31 to 0.9.33 (2026-10-05)

**What moved.**
- **Gateway:** job 12412561, `cpu-amd-zen5-8-vcpu`, private address 10.36.101.16.
- **Store node:** job 12412562, with a 2,000 GB disk, at 10.36.103.152.
- **Workers:** autoscaled on `cpu-amd-zen5-64-vcpu`.
- **Stayed at Hetzner:** the chunk store's S3 bucket. The store node refilled its replica from it in under 2 hours (331 GB).

**How the state moved.**
- The Hetzner gateway's state was restored on the new gateway: PostgreSQL 18 from `pg_dump`, plus the state directory, tokens and signing keys.
- The registry went to `/work/data/ucloud-sandboxes-prod/registry`; all copies were compared byte for byte.
- The Hetzner autoscaler state stayed behind: deleted servers and 324 stale drain intents.
- **Registry disk guard off.** `/work/data` is shared and multi-petabyte, so the `registry_disk_*_percent` thresholds are 100 there. Retention is 365 days, keeping 8 per repository.

**Hetzner-only assumptions this exposed.**
- **Store init's digest check lacked `$SUDO`** (0.9.32). Hetzner init logs in as root.
- **Releases after 0.9.13 lacked `ucloud-chunk-serve`** (0.9.32 restores it). They repacked the 0.9.6 bundles, which predate it.
- **The builder bundle lacked erofs-utils** (0.9.32). Hetzner's builder image shipped it.
- **The gateway's `nydus-image` v2.4.5** had been installed by hand. It was copied over with its pinned digest.
- **Stored image references named `10.42.0.2:5000`.** `scripts/rehome_registry_host.py` rewrote about 47,000 of them in the gateway databases and PostgreSQL. The training selection's `prepared_reference` values need the same rewrite; the canaries used a rewritten copy. verifiers-ucloud sends source images and is unaffected.
- **0.9.32 nodes could not initialize.** Its repacked bundles kept the 0.9.6 module load list, which init must match. 0.9.33 adds `erofs`, `nbd`, `nft_log` and `nfnetlink_log` from the pinned kernel build. It also declares nbd's 1024-device pool in modprobe.d, because init now loads nbd before environment bootstrap would.
- **The upgrade check is strict about the venv.** It compares venv bytes, and the installer's `bin/pip` wrote `python3` shebangs, so the first 0.9.32 apply rolled back by itself. A reinstall through `bin/python -m pip` normalized them.

**Canaries.** All ran through the private address: UCloud's public links answered 449 for every new link, an SDU-side issue reported on 2026-10-05.

| Canary | Result |
|---|---|
| 512 warm | 511/512, ready p50/p95/max 9.3/17.5/20 s |
| 512 from zero | 511/512, 93/102/103 s, including VM provisioning |
| 512 group | 512/512, 30/44/46 s |
| Managed-process (32) | exec 308/s at p50 101 ms; archive upload 7,101 files/s |
| Relay round trip | through an SSH tunnel |
| Build on a released base | 44.5 s, on a 0.9.33 builder |

- **The one burst failure** is the known catalog gap: `prime/primeintellect/pycqa-pyflakes:374-7c74ab0` is not pullable.
- **0.9.33 nodes:** sandbox workers show `nbds_max` 1024 and the four modules loaded. Builders have erofs-utils 1.9.1, with `curl` upgraded rather than removed.
- **Store node:** re-initialized from the 0.9.33 bundle, with `ucloud-chunk-serve` at the pin.

**Hetzner afterwards.**
- **Deleted:** the gateway server, the 250 GB registry Volume, the primary IP and 11 superseded worker snapshots.
- **Kept:** the S3 bucket, plus snapshots `439434667` (0.9.6 worker) and `436561313` (builder) as a minimal rollback.

## The full relay rehearsal on UCloud: 0.9.34 to 0.9.37 (2026-10-05)

**Shape.** 1,024 relay-mode rollouts over 128 images (8 each), at most 2 workers
(`cpu-amd-zen5-64-vcpu`, 192 GB). Each rollout creates a parkable managed-process
sandbox, runs its family's first command, then 8 turns with 5–30 s model waits, as
production training does. "Cold" starts from zero workers, "warm" on the two the cold run
left. The 40 failures in every run are images without Python 3, which the harness needs.

| Release | Run | Completed | Ready p50 / p95 / max | Relay overhead p95 / p99 |
|---|---|---|---|---|
| 0.9.34 | cold | 945 | 247 s / – / – | 3.55 s / – |
| 0.9.35 | cold | 984 | 119 / 168 / – s | 10.09 s / – |
| 0.9.35 | warm | 984 | 31 / 82 / 90 s | 10.09 s / – |
| 0.9.36 | cold | 984 | 115 / 226 / 239 s | 1.96 / 4.26 s |
| 0.9.36 | warm (one worker replaced) | 984 | 99 / 206 / 212 s | 0.26 / 1.97 s |
| 0.9.37 | cold | 980 | 115 / 154 / 164 s | 0.78 / 2.79 s |
| 0.9.37 | warm | 981 | 24 / 64 / 74 s | 0.87 / 2.92 s |

Every turn of every completed rollout succeeded. 0.9.37's 7 other failures were the
benchmark client running out of descriptors under `systemd-run`'s default limit of 1,024.

**What each release fixed** (details in the CHANGELOG):
- **0.9.34:** UCloud bootstrap over the private network (the public SSH proxy refused a
  fresh worker for minutes). Agent launches no longer queue for startup slots. Forecast-only
  pressure no longer swaps out paused waits.
- **0.9.35:** node-local model waits suspend growth forecasts. Before this, a 192 GB worker
  stopped admitting launches at about 160 sandboxes with about 160 GB free.
- **0.9.36:** that bookkeeping moved off the pause and thaw paths. Inline, it put 571 of
  7,872 answers on the 10 s gateway wake.
- **0.9.37:** create throughput.

**Create throughput (0.9.37).**
- A create held one of a node's 32 startup slots for about 3.4 s (p50) for well under 1 s
  of work. Two workers therefore made about 19 creates/s.
- `py-spy --gil` on a worker during the burst showed where its time went:

  | GIL consumer | Share of GIL |
  |---|---|
  | Recomputing every sandbox's growth forecast inside every admission | about 20% |
  | `if_nametoindex` probes (as root, a missing name makes the kernel run modprobe twice) | 6% |
  | The disk sampler resolving 1,024 nbd devices every second | 5.6% |

  All three are fixed. The agent then held the GIL about half the time instead of most of it.
- On the gateway, one host-wide registry-lease lock serialized every create: p50 0.56 s,
  p90 4.1 s per create. It is now keyed by owner.
- **0.9.36's warm run had one worker, not two.** The autoscaler soft-drained one of the two
  workers four minutes into the cold step (by memory claims, everything fit on one). It
  stopped once idle, and the next burst waited for a replacement. Soft-drain now waits until
  no create has been placed for the effective `scale_down_idle_seconds`.
- **Still open:** warm ready is p50 24 s against M2's 30 s for all 1,024, at a steady
  ~20 creates/s (609 ready at 30 s). A slot still holds for about 3.8 s (mean). Of that:
  - memory-admission waits: 1.0 s (launches carry their whole bound until their first wait);
  - registry commits: 0.74 s;
  - runsc: 0.77 s;
  - storage prepare: 0.5 s.
