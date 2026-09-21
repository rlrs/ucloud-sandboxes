# Release 0.5.69 production qualification

Runtime commit: `8a803c0a2d0915e73fb6829d46b425314c664bca`.
Deployed September 21, 2026 at 11:46:24 UTC in DFM Pretraining.

## Deployment and verification

The gateway, relay and autoscaler were upgraded before any new workers.
All 93 installed package files matched the release wheel. Both sandbox and
builder bundles passed the Linux boot validators, and the autoscaler now uses
`/work/ucloud-sandboxes/release/0.5.69`. Native storage and gVisor were unchanged.
No active workers, sandbox routes, pending creates or capacity reservations were
present at preflight; there was no running worker fleet to upgrade. The old
quarantined worker was left untouched. No provider stop was issued by this
release procedure. A fresh canary worker, `12397818`, booted on 0.5.69 and sent
the new I/O PSI metrics successfully.

The old relay took its systemd stop grace period before the service restart;
there were no active sandbox routes or pending response deliveries at preflight.
The gateway subsequently served the exact new version and all three services
were active. Twenty final public health checks returned 200, median 19.6 ms,
maximum 25.3 ms. Smoke reservations and sandboxes were deleted; final routes,
pending creates and capacity reservations were all zero.

The local canonical check passed 999 server tests (six platform skips) and 118
SDK tests. Linux production-host qualification passed 119 targeted tests. An
initial test invocation named a nonexistent test module; correcting that test
list produced the passing result without code changes. Both Python 3.10 and
3.13 [CI jobs](https://github.com/rlrs/ucloud-sandboxes/actions/runs/35595568068)
passed, including shellcheck, Linux packet filtering and Registry/S3 contracts.
No SDK or Verifiers update is required.

## Measurements

The synthetic 516-sandbox heartbeat fixture ran in isolated temporary databases
on the production gateway (Python 3.14.4), three repetitions per release. Median
process CPU time for 1,000 owner reads fell from 1.183 s to 0.581 s (51%); 200
fleet reads fell from 1.294 s to 0.576 s (55%). Eight metrics appends with a pinned
SQLite reader fell from 5.008 s to 0.000598 s; both versions retained ten events
once the reader closed. The baseline ran before the candidate, not in randomized
order. These are component measurements, not client throughput results.

The live smoke completed eight park/publish/detach/restore cycles, waking through
sandbox-scoped SDK exec. All cycles preserved process identity and state, and
needed zero lifecycle retries.

| Operation | Median | Maximum |
| --- | ---: | ---: |
| Park | 0.166 s | 0.289 s |
| Publish and detach | 0.412 s | 7.218 s |
| Wake through exec | 0.774 s | 1.334 s |

The eighth publication was the slowest. Final worker counters were nine
publications, one compaction and 522,600,448 uploaded bytes. This is consistent
with retaining the chain-depth compaction bound; it does not establish a loaded
throughput improvement or isolate the compaction's contribution to that tail.

An eight-second sample overlapping the smoke showed worker CPU 98.1% idle,
physical-disk writes 34.3 MiB/s, reads rounded to 0.0 MiB/s, I/O PSI some 1.2%,
and memory PSI zero. These light-load readings cannot be compared causally with
the earlier 144-sandbox workload on worker 12397674. That workload had ended
before deployment. Large-base compaction amplification and pressure-aware
placement are covered by regression tests; their loaded production benefit
still needs a comparable workload.

A separate continuity check failed on its first three-second authenticated
heartbeat probe, which returned no result; the precise failure was not logged.
An independent direct fetch succeeded, and a full repeat passed with probe times
68 ms and 16 ms. The repeat verified suspension fencing, preservation through a
synthetic partition, authenticated same-boot recovery and sandbox state. Its
control database was isolated and it issued no provider mutations. This transient
is retained here rather than counted as an uninterrupted qualification pass.

Raw results: [host benchmark](../benchmarks/release-0.5.69-production-host-2026-09-21.json)
and [live smoke](../benchmarks/release-0.5.69-live-smoke-2026-09-21.json).
