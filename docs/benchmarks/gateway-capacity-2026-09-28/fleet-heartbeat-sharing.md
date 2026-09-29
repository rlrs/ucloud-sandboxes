# Avoiding repeated full-inventory copies during fleet reads

The isolated fleet reader renders read-only heartbeat data. It now requests
`load_heartbeats(shared=True)`, avoiding recursive copies of every sandbox's
nested storage descriptors on every poll. Complete inventory remains available
for route-absence checks; every read still checks and validates current SQLite
state. Renderer caches hold validated heartbeat objects and never mutate them.
External commits produce new objects and invalidate affected rendering normally.

The synthetic benchmark uses 500 routes and 500 complete inventory entries split
across three nodes, including nested snapshot/dependency descriptors. Heartbeat
payloads total 661,431 bytes; both modes return identical 402,554-byte responses.
It measures full warm fleet rendering, including route and heartbeat SQLite
reads, with 100 reads per sample and five alternating-order repeats.

| Mode | Median CPU per read |
| --- | ---: |
| Detached heartbeat objects | 15.758 ms |
| Shared heartbeat objects | 5.381 ms |

This is a **65.85% local CPU reduction**. At 24 polls per second the measured
local difference corresponds to about 0.249 CPU cores; this is an extrapolation,
not a production CPU or latency result. Benchmark script:
`scripts/benchmark_fleet_heartbeat_sharing.py`; raw samples:
`fleet-heartbeat-sharing.json`.

Validation: 27 fleet reader, render-cache and control-state cache tests plus 83
existing gateway HTTP tests passed. The new regression checks shared nested
input immutability, externally removed inventory, quarantine changes, and corrupt
durable payload rejection. No schema, response contract, or dependency changed.
