# Production release 0.5.82 — 2026-09-22

Deployed the pinned AgentEnv v0.2.2 upgrade through fresh production workers.
The gateway and relay report 0.5.82, Postgres remains authoritative, and normal
autoscaling is running. No SDK or Verifiers update is required.

Implementation: `e8fa5fc419c4fe076cbf4648e4cb7082f785ce29`.
Version commit: `0bd6cb8`. The artifact manifest's original `commit` identifies
the implementation before the version-only commit; `release_commit` records the
version commit separately. Package installation verified 110 installed files
against the release wheel. Gateway cutover completed at 19:36:24 UTC.

## Storage rollout and preserved state

Fresh workers `12399418`, `12399419`, and `12399420` run 0.5.82. Reading the actual
running storage backend executables through `/proc` verified SHA-256
`76d59a1c10cb495e90380e320de8fe30e6370f8b374aab410494c8e0b92a5748` on all three.
They run kernel 7.0.0-30-generic. Both node bundles passed boot validation on that
kernel. Gateway `12399353` retains its four vCPUs and kernel 7.0.0-31-generic.

Empty old workers `12398499`, `12398500`, and `12398539` were retired only after
fresh drain readiness, empty local inventory, and zero assigned routes. Stop
requests went through the durable provider-operation journal. Independent
provider reads confirmed SUCCESS for all three; full provider inventory then
reconciled their uncertain stop responses.

Old workers `12397503` and `12398541` still hold one retained inventory entry
each. They remain on 0.5.81 with their original writable state preserved.
Both were drained during cutover. After autoscaling resumed, it canceled the
drain on `12397503`; incompatible-version placement filtering still excludes
both workers from new release placement. The drain proof for `12398541` remains
incomplete, and no provider stop was submitted. They were not upgraded in place
or deleted to make the rollout appear clean.

Builders `12399412`, `12399413`, and `12399414` were drained, updated to 0.5.82,
and reopened. The existing autoscaler drain on `12399413` was canceled through
its recorded token after the update. Once resumed, autoscaling successfully
bootstrapped builder `12399422` with the new release bundle. Live builds were
observed on the updated builders.

Rollback must use the qualified sealed-layer migration procedure and fresh
writable state; never open v0.2.2 writable uppers with the old backend. See the
[qualification review](agentenv022-upgrade-2026-09-22.md) for compatibility tests,
rollback constraints, and the unresolved native-write-throughput limitation.

## Public API verification

Both tests used the realistic relay workload: 128 MiB resident memory, 16 MiB
dirty memory, 64 files of 64 KiB, guest tool execution, and memory/file verification.
All 240 cycles completed without test or cleanup errors. The first cycle per
sandbox was warmup and excluded from latency measurements.

| Test | Completed cycles | Commit/wake p95 | Usable execution p95 |
| --- | ---: | ---: | ---: |
| 16 sandboxes, forced parking | 48 | 0.781 s | 1.325 s |
| 64 sandboxes, natural parking | 192 | 0.611 s | 0.977 s |

The forced test observed parking in all 32 measured cycles. Its response-ready
to usable p95 was 7.912 s, including the harness waiting for forced parking.
Its `slo_passed` field used a 10-second functional-smoke threshold, not 0.8 s.

The natural test observed parking in only one of 128 measured cycles, so its
commit/wake result is predominantly the already-running path. Response-ready
to usable p95 was 0.977 s and the configured 0.8-second SLO failed. These tests
establish deployment functionality, not 256/512-way performance qualification
or achievement of the sustained parked-wake target. Natural-test placements
covered all three fresh workers.

## Final checks and operational correction

Gateway, relay, registry, and autoscaler services are active. Public gateway
and relay health checks return 0.5.82. The autoscaler completed scheduling cycles
and holds the controller lock. Live application traffic is running on the new
workers; additional synthetic testing stopped once that traffic appeared.

A root-run deployment provider refresh replaced the UCloud session file with
root ownership. The first autoscaler restart could not read it. Restoring the
file to `ucloud:ucloud` with mode 0600 fixed the error; after the explicit restart,
checks reported active/running with zero further restarts. Future operational
provider calls should run as the service user to avoid repeating this issue.

Sanitized evidence is in
[`docs/benchmarks/release-0.5.82-2026-09-22`](../benchmarks/release-0.5.82-2026-09-22).
The benchmark files retain configurations and aggregate results; per-cycle
records remain in `/work/ucloud-sandboxes/release/0.5.82` on the gateway. The
fleet records are time-stamped snapshots, not promises of a fixed fleet size.
