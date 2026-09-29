# Registry publication optimization

Implementation and local tests are complete. Production byte-equivalence checks,
matched build-load qualification and deployment are still pending; no production
speedup is claimed here.

The preceding build qualification exposed Python dependency updates spending
33.8 seconds on average inside Docker pull during immutable filesystem
publication. That measurement includes downloading, decompression and filesystem
writes.

## Confirmed cause

Hash-verified inspection of an owned synthetic Python image found that its three
missing EROFS groups contain 345,718,795 compressed bytes and 1,109,656,576
unpacked tar bytes. Both exceeded the old selective-publication limits of
128 MiB compressed and 1 GiB unpacked. The 41,168 members had no unsupported
filesystem features, missing parents or duplicate paths. The fallback was
therefore avoidable for this workload.

The new aggregate limits are 512 MiB compressed and 2 GiB unpacked. Member,
filesystem-semantics and digest checks remain enforced. Production publication
admission remains two finishing builds per node, alongside four builds in
preparation/build/push.

## Changes

- Stream compressed registry bytes through hashing and decompression into the
  quarantine tar. This removes the compressed temporary-file write/read cycle.
  Both compressed and uncompressed identities must authenticate before extraction.
- Claim missing components in a consistent lock order before fetching their
  bytes. Recheck after waiting, allowing concurrent builders on the same node to
  reuse completed work. Claims release on success, fallback, cancellation and
  deadline expiry.
- Retain bounded fallback-reason counters and separate transfer, decompression
  and extraction measurements. Actual response-byte counts include failed
  attempts. Decompression timing excludes nested transfer wait but is not a
  CPU-only measurement.

No SDK changes or new client opt-in are required. The existing full Docker path
remains available for unsupported layers and inputs beyond the selective limits.

## Validation so far

The affected suite passed 170 tests in 6.640 seconds, with two existing platform
skips. Tests cover corrupt and truncated streams, expansion limits, deadline and
response cleanup, real preparation subprocesses, multi-group concurrency,
component identities, fallback and diagnostic allowlists. Ruff and whitespace
checks passed.

The layer-inspection, multi-group comparison and builder-identity qualification
helpers passed 23 additional tests.

A small local ABBA witness verified identical payloads while reducing quarantine
writes from 33,565,901 to 16,783,360 bytes. This demonstrates the removed disk
pass; it is not production throughput evidence.

Qualification tooling now supports forcing all three missing groups and comparing
their exact EROFS bytes and signed metadata against Docker publication. The planned
load comparison keeps the same four builders, 48 concurrent cases per arm,
four-plus-two admission policy and 600-second common arrival deadline. Raw
production telemetry and operational receipts remain outside this public report.
