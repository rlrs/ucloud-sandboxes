# Relay and inventory optimization, 2026-09-28

Deployed on the production CCX23 gateway at **18:47:27 UTC**. The installed
wheel is `b96bdce2cdda134dd26d28fc95949cc6e9d62a7e142dc41a10d0066f1baf5ac3`.
This is an incremental reduction in work, not a new 1,000-sandbox capacity
qualification. The gateway remains four CPUs / 16 GiB.

## Changes and recovery properties

Relay commits now signal only the relevant local wake or park queue. Unbound
observer requests no longer signal lifecycle work. The notification listener
ignores this process's own publisher echoes while the actual backend connection
is still open. Wire notifications retain the legacy global hint for peers.
Dispatch completion rescans only when capacity blocked additional work.
Fixed 250 ms reconciliation remains for lost notifications, retries and expired
claims; leases, ownership fencing and transaction boundaries are unchanged.
Process-local operation counters expose useful claims, empty claims and query
activity separately from database-pool acquisitions.

`GET /v1/sandboxes?view=status` provides a compact, fresh status projection,
with optional repeated exact `id` filters. It omits full specifications and
attached snapshot descriptors at the database read. Default full responses
remain unchanged. Clients must opt in to receive this benefit. See
[the API contract](../../fleet-status.md) and
[inventory measurements](../gateway-capacity-2026-09-28/fleet-status.md).

The final wheel differs from the previously deployed wheel in exactly four
Python modules: control plane, fleet reader, routing and relay (plus wheel
RECORD). See `wheel-comparison.json`. Deployment changed only the configured
future-node bundle root. Native binaries, dependencies, PostgreSQL, registry,
nginx and host settings were preserved. Future builder and sandbox bundles
contain the same wheel, with unchanged dependency/native inventories verified.

## Local performance evidence

The relay comparison used disposable PostgreSQL 17 schemas in ABBA order.
Each case offered 200 model cycles at 25 cycles/s plus 200 observer receipts,
with the same input/output payloads and real durable lifecycle transitions.
Park callbacks deferred and wake callbacks completed. All 800 model responses
and 800 observer receipts across four cases were correct.

| Mean per 200 model cycles + 200 receipts | Before | After | Reduction |
| --- | ---: | ---: | ---: |
| Lifecycle claim queries | 4,380 | 472.5 | 89.2% |
| Empty lifecycle claims | 3,980 | 72.5 | 98.2% |
| Total observed database transactions | 8,393.5 | 4,399.5 | 47.6% |
| Python relay plus in-process driver CPU | 3.750 s | 2.476 s | 34.0% |

Useful claimed rows remained 400 in each case. Response-to-delivery p95 was
13.1–13.4 ms after, versus 14.6–14.7 ms before. This isolates coordination work:
it does not include real sandboxes, HTTP/TLS, registry/NAT, or PostgreSQL CPU.
Do not apply its CPU percentage to the entire production gateway.

`relay-scan-abba.json` retains all cases and source hashes. Reproduce with
`scripts/benchmark_relay_lifecycle_scans.py --baseline PATH --output PATH`,
using `UCLOUD_TEST_POSTGRES_DSN` for a disposable database. The baseline module
comes from commit `4a5df3ab643a86bb77e1f15a97301247704ffbc4`, path
`ucloud_sandboxes/shared_control/relay.py`; SHA-256
`7ef5be560d61b600c0606d3e5b7991404a42af72830220fbd8601eb4da73759a`.
The measured candidate module matches the deployed wheel.

Synthetic PostgreSQL fleet reads at 500 and 1,000 routes used 18–20% less
Python CPU for unchanged inventories and 14–18% less with a fresh complete
heartbeat before each read. Compact responses were 74.5% smaller. These
measurements exclude IPC, HTTP/TLS and client parsing and do not measure
PostgreSQL server CPU. At only 20 unchanged polls/s, the observed 500-route
rendering CPU saving is approximately 0.019 cores, not a whole-host 75% gain.
The unchanged 512-agent full-inventory workload therefore does not receive the
compact-view savings automatically.

## Validation and production boundaries

- 78 real-PostgreSQL relay tests passed, including notification loss, peer
  compatibility, reconnect/replacement, saturated capacity, wake priority,
  competing owners, lease fencing, cancellation and result acknowledgement.
- 41 inventory/cache/API tests passed, including four contracts exercised
  against PostgreSQL; 83 existing control-plane tests passed.
- 40 load-harness tests passed; six deployment safety tests covered rollback,
  dependency preservation, configuration modes and work arriving before stop.
- Focused lint, compilation and `git diff --check` passed.

Automatic approval review declined copying production tokens to the external
driver without explicit credential-export permission. No tokens were copied.
Functional canaries instead run the driver on the gateway with credentials
remaining on-server. Their CPU and latency cannot qualify external-driver
capacity. They use 32 real sandboxes, four cycles, 512 MiB random resident
memory, 128 MiB dirty pages/cycle, 20–25 second simulated inference, actual
tool uploads/execs, and four inventory pollers.

The baseline completed all 128 cycles correctly, with no workload or cleanup
errors and healthy worker observations. Ready-to-usable-exec p95 was 0.436 s.
The small rolling run had no responses during creation and therefore correctly
failed the harness's full qualification gate despite passing its measured
latency thresholds. No qualification threshold was weakened.

Both post-deployment runs completed all 128 cycles each, with no workload,
inventory-poll or cleanup errors and healthy worker observations. Full-view
ready-to-usable-exec p95 was **0.432 s**; compact-view p95 was **0.435 s**.
Both again had zero creation-overlap cycles and therefore failed the full
qualification gate; their passing correctness and measured latency do not
override that boundary. See the three `*32-*-summary.json` files. The full-view
run provisioned a fresh worker using the updated future-node bundle; the
compact run reused it. Driver placement and differing startup/cache conditions
prevent treating this as a capacity test or a precise latency speedup claim.

During full-32-agent intervals, the relay's main plus lifecycle pool recorded
**72.91 acquisitions/s before and 46.36/s after**. Snapshot windows were 90.00
and 85.00 seconds, covering 97 and 96 model completions, respectively. Counts
include observer requests and background work; they are not SQL statement
counts. The unchanged harness and SDK hashes are retained in
`production-pool-comparison.json`. After optimization, 678 of 678 measured
empty lifecycle claims corresponded to the two periodic scans across 339
reconciliations; 696 self-notification echoes were suppressed. This supports
reduced redundant coordination without establishing whole-host CPU savings.

A paired live 32-record inventory read was 53,206 bytes full versus 12,071
bytes compact. Visible IDs and exact filtering matched; an unauthenticated
compact request returned 401. Deployed source hashes matched the tested wheel.
See `production-status-smoke.json`.

New 512/750/1,000 whole-gateway qualification, concurrent bulk transfers and
memory-pressure recovery remain separate work; these changes do not establish
that two workers can retain 1,000 copies of the prior 512 MiB private-heap
workload. Credentials were not exported, and the external-driver permission
question remains unanswered.

## Deployment and rollback

`staging-receipt.json` pins the wheel, controller, repacker, source bundles and
rebuilt bundle hashes. `deployment-receipt.json` records healthy public/local
gateway and relay, working metrics and the unchanged 64 GiB relay budget.

The gateway retains its full previous virtualenv and configuration under
`/work/ucloud-sandboxes/gateway-dispatch-optimization-20260928/rollback`.
The verified controller supports rollback only after the fleet and relay are
idle. Its apply path automatically restores the backup if installation or
health checks fail. See `deployment-controller.py` and
`deployment-instructions.md`. No database migration or provider resize occurred.

Final checks at 18:57:38 UTC found all four control services active, public
gateway/relay health passing, zero remaining sandboxes or relay work, and no
running canary drivers or samplers. The five gateway unattended/APT units
remain masked. `final-health-receipt.json` records those checks. Canary
reservations and rollouts were removed by the harness; worker lifecycle remains
under the normal autoscaler. Backups and reports are retained for review.
