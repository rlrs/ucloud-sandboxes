# Performance and correctness remediation

This checklist tracks the repository-wide review performed on 2026-08-04.
Fixes are implemented in priority order. Each issue receives its own code and
regression-test commit; the commit column records that commit's unique subject.

Status values: `pending`, `in progress`, `complete`, or `blocked`.

| # | Priority | Status | Issue | Intended fix | Commit |
|---:|:---:|:---:|---|---|---|
| 1 | P1 | complete | Concurrent heartbeats can overwrite newer node state. | Make receipt ordering, previous-state lookup, and idle-clock derivation atomic; reject older gateway receipts. | `fix: make heartbeat updates monotonic` |
| 2 | P1 | complete | A request larger than the configured VM shape scales through `max_nodes`. | Reject or terminally classify unschedulable shapes before they contribute create demand. | `fix: reject unschedulable sandbox shapes` |
| 3 | P1 | pending | A stale provisioning VM can suppress fragmentation replacement indefinitely. | Apply the aggregate stale-provisioning eligibility rule to placement bins. | — |
| 4 | P1 | pending | Waking a released storage-native volume double-counts its capacity. | Reserve only the transition's incremental capacity; do not count the current volume twice. | — |
| 5 | P1 | pending | A pooled-backend release failure can permanently leak an active ublk device. | Preserve durable device ownership until backend release/delete succeeds and retry reconciliation. | — |
| 6 | P1 | pending | Polling a stale legacy exec route can delete the entire sandbox route. | Delete only the exec route and make stale detection inventory-aware. | — |
| 7 | P1 | pending | A fresh replacement heartbeat can hide a destructive VM replacement. | Retrieve/latch replacement history on owned-route epoch mismatch regardless of freshness. | — |
| 8 | P1 | pending | A maximum/default managed-process log read exceeds the control response limit. | Separate bounded request and encoded-response limits and test the maximum chunk. | — |
| 9 | P1 | pending | Async-exec cancellation can leak a lifecycle lease. | Make thread-backed acquisition and every subsequent stage cancellation-safe with unconditional cleanup. | — |
| 10 | P1 | pending | Gateway node responses are buffered without a byte limit. | Stream bulk responses and enforce bounded buffering for structured control/error responses. | — |
| 11 | P1 | pending | Completed model-relay requests have no byte budget and persistence blocks the event loop. | Add byte-aware retention, prune before restore, discard redundant bodies, and isolate durable writes from the event loop. | — |
| 12 | P2 | pending | SQLite metrics ignore the configured byte budget. | Enforce a logical/physical SQLite budget including WAL growth and reclaim old pages. | — |
| 13 | P2 | pending | Direct-runtime restart performs quadratic registry work and repeated node-global networking checks. | Reconcile from one registry snapshot and perform node-global network setup once. | — |
| 14 | P2 | pending | Complete heartbeats scan all historical exec routes. | Query only required sandbox routes and avoid loading exec sessions. | — |
| 15 | P2 | pending | Permanent direct-registry tombstones eventually exceed the hard read ceiling. | Add generation-safe bounded compaction and prevent writes that make the registry unreadable. | — |
| 16 | P2 | pending | Managed-process running and terminal persistence failures are ignored. | Surface/retry durability failures and enter a visible degraded state instead of reporting false success. | — |
| 17 | P2 | pending | Successful image-build exit codes deserialize as missing. | Select snake/camel-case fields by presence so integer zero survives round trips. | — |

## Baseline verification

- Python unit suite: 956 passed, 1 external verifier integration skipped.
- Python byte compilation: passed.
- Managed-process Go package: Linux/amd64 test binary compiled successfully.
- Native macOS Go tests: blocked by the local linker's `missing LC_UUID` error.
- AgentEnv comparison: commit `f41abb21324f6b0520abf34b7720aa260ddd10eb`
  with the repository's pinned storage-native patches applied.
