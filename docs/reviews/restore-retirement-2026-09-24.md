# Restore cleanup candidate — 24 September 2026

Status: deployed as **0.5.114rc25** from `d6aac4b` at 08:39:24 UTC.
This selectively brings forward deferred checkpoint retirement from `65e7947`,
without the separate reclaim-ranking policy changes. Both role bundles preserve
rc24 native/OS/storage bytes; PostgreSQL, the ten-worker cap and memory policy
are unchanged. The SDK remains 0.4.27; no client update is needed for this change.

The rc24 sample measured synchronous artifact cleanup at 152 ms mean and 290 ms
maximum across seven wake traces. After the durable RUNNING commit, reflink
restores now leave this work to the existing five-second maintenance loop.
Durable overlap claims are the worklist; no additional queue or daemon is added.

Maintenance pins the allocation, checks lifecycle authority under the sandbox
lock, and unlinks only the exact superseded checkpoint outside that lock.
Immutable manifest identity and file identity are checked before deletion.
The manifest is removed last so interrupted cleanup can resume. Capacity stays
reserved until physical reclamation and reader closure permit release. A slow
old-generation unlink therefore does not block a later capture or wake. One
retirement lock is reused per incarnation to avoid accumulating a lock file per
checkpoint. Non-reflink cleanup keeps its existing behavior.

The existing aggregate restore-preparation timer is preserved, with separate
timers for workspace preparation, checkpoint validation, joining network
preparation, memory preparation, and source retention. These identify the next
target without weakening durability or integrity checks.

Validation: 134 focused local tests and Ruff passed; 205 focused Linux tests
passed against the candidate source in an isolated qualification directory.
Tests include blocked cleanup overlapping a subsequent park/wake, source-reader
retention, interrupted unlink/restart, failed restore, identity mismatch,
repeated-generation lock reuse, and memory-growth/admission regressions. Live verification on fresh worker **12401289** passed 368 forced park/wake
cycles across three runs using real native backing, with no scenario or cleanup
errors. Each run used 16 concurrent guests, 128 MiB resident heaps, 16 MiB dirty
per cycle, 2 GiB limits and three-second model waits. This is not a full-RAM
pressure qualification or evidence of 256/512-guest performance.

| Run | Cycles | Guest continuation p95 | Useful execution p95 |
| --- | ---: | ---: | ---: |
| rc25 fresh worker | 48 | 2.80 s | 3.10 s |
| rc25 warm | 160 | 2.59 s | 3.20 s |
| Old cleanup, same worker | 160 | 1.45 s | 1.98 s |
| rc25 repeat, same worker | 160 | 1.40 s | 1.93 s |

The previous rc24 run on another worker measured useful execution p95 2.52 s.
Because the first rc25 runs were slower, the old rc24 warden/artifact modules
were tested from a separate source copy on the same idle worker, then its
canonical rc25 launcher was restored before the repeat. The immutable bundle
cache was never modified. The gateway remained rc25 throughout this comparison.
The final same-worker comparison shows no clear overall speedup or repeatable
regression; temporal variation is substantial and subsecond latency remains unmet.

Six sampled rc25 traces show foreground cleanup averaging 0.003 ms versus
152 ms in the earlier seven-trace rc24 sample. Total warden time averaged
723 ms versus 827 ms, but workspace preparation reached 917 ms and source
retention reached 373 ms. These small samples identify remaining costs; they
are not a controlled throughput comparison. Background cleanup removes
foreground waiting, not the underlying deletion I/O.

Both longer rc25 runs passed fleet-health observation. The short cold run ended
inside the new-worker observation grace, with no reported health failure. Final
checks found zero sandbox routes, relay inflight/pending responses, worker
registrations, overlap claims or live retained checkpoints. Worker source hashes
match the committed rc25 files. All control-plane services are active.

Evidence: [deployment and benchmark receipts](../benchmarks/restore-retirement-2026-09-24/).
