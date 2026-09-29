# Registry I/O optimization — 2026-09-29

Deployed a bounded registry blob-mount optimization and removed duplicate
environment-manifest reads. The same 48-build fixture set passed on four fresh
builders. Physical registry writes fell 88.2%, but this run did **not** improve
batch completion time: 135.56 seconds versus 122.06 seconds previously.

## Change

Before BuildKit pushes a new managed image, the builder fetches the newest
matching Dockerfile cache manifest and mounts its existing layers into the
new image repository. Registry mounts create references without transferring
blob payloads. This addresses fresh builders repeatedly uploading large
layers already present in the shared registry. BuildKit still validates and
builds the requested image, imports its existing cache candidates, exports a
unique cache snapshot, and uploads any missing blobs normally.

The optional operation validates the raw manifest digest and descriptor list,
requires the same configured registry and a managed target repository, limits
the manifest to 256 KiB and 64 layers, and uses a cooperative three-second
budget. Blocking response reads and cleanup may extend elapsed time. A declined
mount's newly created upload session is cancelled individually. Cache misses,
pruning races, and mount failures preserve the ordinary push path.

EROFS component reuse now validates the already-fetched manifest document
instead of fetching it again. Signature, config-digest, blob-closure, root
validation, and retention-refresh behavior remain enforced.

## Measured result

The baseline is the immediately preceding selective-EROFS release's
`opt-repeat` run, not the older overloaded build test. Both runs used four
fresh CCX33 builders, four admitted builds per builder, 48 concurrent SDK
submissions, and the same frozen context hashes: 16 revisions each of Python
scientific/native-extension, TypeScript tools, and TypeScript multistage images.
Local BuildKit caches were empty before this run; shared registry cache and
page-cache contents were retained. Builder placement and shared-cache history
differ, so this is an observed historical comparison, not an isolated latency
experiment.

| Measurement | Previous release | Registry I/O release |
| --- | ---: | ---: |
| Builds succeeded | 48/48 | 48/48 |
| Managed-image committed blob descriptor bytes | 5,339,586,891 | 68,380,565 |
| All committed blob descriptor bytes | 5,646,876,661 | 378,792,468 |
| Successful blob mounts | 366 | 846 |
| Environment manifest GETs | 896 | 576 |
| Sampled physical registry writes | 5.518 GiB | 0.651 GiB |
| Registry disk I/O-weighted await | 31.05 ms | 2.84 ms |
| Registry CPU, mean occupied cores | 0.199 | 0.102 |
| Batch completion | 122.065 s | 135.561 s |
| Client median | 84.219 s | 76.800 s |
| Client p95 | 118.652 s | 132.432 s |

Registry event totals come from [baseline](baseline-repeat-registry.json) and
[candidate](candidate-registry.json) aggregates. Committed descriptor bytes
associate successful upload commits with immutable blob sizes; they are not
HTTP wire bytes or physical disk writes. The earlier run uploaded one 313 MB
blob ten times; the candidate had no corresponding large repeated commits.
All 846 candidate mount responses were HTTP 201, and the 320 fewer environment
manifest GETs match two avoided reads for each of 160 reused groups.

The candidate adds one cache-manifest GET per build. Mount counters emitted at
build start were not retained in the final bounded log tails; no per-build
mount elapsed-time claim is made. Registry-side mount durations alone do not
measure full client pre-mount overhead.

[Host findings](host-findings.md) distinguish physical writes, registry CPU,
the gateway-local SDK driver, builder CPU, and memory. Gateway host CPU averaged
0.38 occupied cores; all 22 in-window health probes passed. No sampled OOM or
swap activity occurred. Telemetry includes a post-run writeback tail; the table
uses complete intervals inside the client window, as did the baseline.
This build test does not qualify 500 or 1,000 running agent sandboxes.

[Per-build comparison](repeat-comparison.md) shows longer build/push and local
environment-preparation tails despite lower registry upload latency. Builder
execution/materialization and workload placement need profiling before making
further concurrency or sizing changes. The median improved, but p95 and total
batch time regressed; this release is an I/O improvement, not a demonstrated
end-to-end speedup. [Narrow-window analysis](builder-tail-analysis.md) locates
the late environment work in extraction and squashing, and confirms that
including 30 seconds of writeback preserves the approximately 88% write reduction.

## Deployment and verification

The [deployment receipt](deployment-receipt.json) records the healthy activation
at 10:34:03 UTC. Wheel SHA256:
`59229802be015443b0dd61f2c0c1b8a1eb9cd4460fc35eaf003a601c72b7009b`.
The pinned [staging receipt](staging-receipt.json) covers the new builder and
sandbox bundles. Native and dependency bytes were preserved. The gateway
configuration changed only `node_package_root`; the new controller retained
a paired configuration and gateway-venv rollback under this release root.
Use this release's controller for rollback, not the previous release's backup.

[Focused test verification](test-verification.md) records the exact suites and
platform skips. The [live API canary](mount-canary.json) exercised successful
mounts, readback, declined-mount cleanup, and ordinary upload fallback.
[Three application smoke sandboxes](smoke-io-repeat.json) executed the built
fixtures successfully and were deleted. All 48 builds retained durable history.
The [final audit](final-state.json), captured at 10:51:40 UTC, confirms all five
temporary VMs retired, no sandboxes or builds remained active, reservations and
samplers were cleared, all 48 exact history identities matched, and gateway,
relay, and metrics health passed. This local receipt is the unchanged server
`final-state-complete.json`; earlier observations are retained separately.
All 162 packaged source/resource files match the candidate wheel byte for byte.
The earlier `pool-ready.json` is an interim three-builder observation; the
measured run's [before receipt](io-repeat/before.json) records all four builders.

Normal cache pruning removed 48 old cache manifests and retained 64 entries
referencing 963,629,116 unique blob bytes. Registry storage was 15.6% used with
845.7 GB available. This was ordinary retention, without forced physical GC.
[Image inventory](image-inventory.json) preserves OCI/EROFS sharing accounting.
Managed test image aliases and tiny manifest-free mount-canary blobs remain
subject to ordinary lifecycle/GC policy; no temporary VM remains.

The [prepared protocol](QUALIFICATION.md) and frozen
[qualification receipt](io-repeat.qualification.json) preserve execution inputs.
Raw customer logs and credentials are not included. The telemetry, infrastructure
IDs, benchmark results, and deployment receipts are production-derived data;
publication of commits `da5c8ee` and `48f30bd` was explicitly approved by the
user and completed on 2026-09-29 before the builder-preparation qualification.
