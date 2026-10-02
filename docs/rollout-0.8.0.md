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
