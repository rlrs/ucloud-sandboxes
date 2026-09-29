# Production build optimizations — 2026-09-29

Deployed at 09:37:43 UTC. Small partial EROFS cache misses now download and
materialize only the missing OCI layers. Builder admission is limited to four
nonterminal builds, including context preparation, matching the four execution
slots. Excess work stays pending until a builder has capacity; SDK 0.4.33
already retries the existing admission responses.

The deployed wheel SHA256 is
`f38db1aae25b0ddb14401fce61d80e299a2d8740b2586998e521b21ef0a26a2f`.
The [deployment receipt](deployment-receipt.json) records healthy gateway/relay
HTTPS, unchanged dependency/native bytes, and paired configuration/venv rollback.
Only `node_package_root` changed in production configuration. Source hashes were
verified on all four newly provisioned builders before qualification; their
local BuildKit caches were initially empty.

## Measured SDK results

Each burst submitted 48 independent builds simultaneously across four CCX33
builders. Inputs used the existing Python data/scientific application with
1,500 modules, TypeScript tooling application with 2,000 modules, and TypeScript
multistage runtime. Base image digests and dependency locks stayed frozen.

| Measurement | Earlier overload | Repeated contexts | Fresh source edits |
| --- | ---: | ---: | ---: |
| Successful builds | 48/48 | 48/48 | 48/48 |
| Batch duration | 172.2 s | 122.1 s | 106.1 s |
| Client median | 77.3 s | 84.2 s | 58.9 s |
| Client p95 | 162.5 s | 118.7 s | 102.8 s |
| Node queue p95 | 108.38 s | 0.007 s | 0.010 s |
| Maximum admitted per builder | 8 | 4 | 4 |
| Maximum executing per builder | 4 | 4 | 4 |
| Submission HTTP 503s recovered by SDK | 133 | 761 | 396 |
| Durable terminal-history rows | 48 | 48 | 48 |

Both candidate bursts reached 16 simultaneous executions. Neither had an
interval with queued work behind a full builder while another builder had an
unused execution slot; the earlier run had a continuous 23.88-second interval.
The remaining millisecond queue times include cleanup handoff. More waiting
occurs during retryable submission now, so node queue reductions alone are not
claimed as client latency savings.

These are descriptive historical comparisons, not isolated causal speedups.
The repeated burst used app revisions 5–20; the fresh burst used newly generated
revisions 29–44. Earlier overload inputs, cache chronology and fleet age differ.
The fresh burst also benefited from the first burst warming local BuildKit.
Fresh fixtures were generated on the gateway during the first burst; that small
extra filesystem/CPU workload is another confound. All fresh inputs changed
only their revision source file, verified by the
[preparation receipt](fresh-preparation-receipt.json) and retained checksums.

All 96 builds used selective materialization and skipped Docker pulls during
EROFS publication. Across both bursts they reused 320 groups, built 96 groups
(767.0 MB of EROFS), and fetched 194.0 MB of selected compressed OCI layers.
The latter is descriptor/download accounting, not physical registry I/O.
Full metrics and limits: [repeated comparison](repeat-comparison.md),
[fresh comparison](fresh-comparison.md), and [host findings](host-findings.md).

## Correctness and boundaries

Read-only ABBA comparisons forced one cached application group to miss in
memory. For all three pinned images, candidate EROFS bytes, chunk digests,
parent/source bindings and signed metadata matched the existing Docker output
exactly. No cache tags were deleted to manufacture misses.

For the Python image, the first Docker-backed trial took 36.56 seconds; the
selective trials took 0.85–0.89 seconds and fetched 790,906 bytes for a 5.35 MB
EROFS group. The final Docker trial, with its local image cache warm, took
0.75 seconds. This demonstrates avoiding cold Docker materialization, not a
general 40-fold build speedup. The Node comparisons are retained in
[selective-summary.json](selective-summary.json); those two fixtures ran in
parallel, so their timing is not an isolated throughput comparison.

Explicit filesystem canaries matched exact EROFS bytes for ownership, modes,
same-tar hardlinks and relative/absolute/dangling symlinks. Deletion whiteouts,
opaque directories and missing explicit parent metadata correctly used Docker
fallback. Docker itself rejected the cross-layer-hardlink fixture; it is an
existing input rejection, not successful fallback coverage.

All three application images passed real sandbox smoke checks. Three semantic
sandboxes also verified the actual mounted filesystem and were deleted. The
first semantic attempt used SDK default UID 1000 and correctly could not enter
the fixture's UID-23123 mode-0750 directory. Its sandbox was deleted; the helper
was corrected to use UID/GID 23123:23124, retaining dropped capabilities and
no-new-privileges. The successful [runtime receipt](semantic-runtime.json)
records the checked identity. No deployed code or fixture bytes changed.

Selective extraction is conservative: at most 128 MiB compressed and 1 GiB
unpacked per attempt, with authenticated compressed and uncompressed digests,
private scratch directories, explicit parent metadata and bounded member
counts. Unsupported metadata/lower-layer semantics fall back to Docker;
integrity failures abort before publishing. Cold images without reusable
groups retain the existing Docker path and filesystem format.

Relevant verification: 131 scheduling/HTTP/ownership/admission tests; 94
environment/materializer/history tests with two platform skips; four signed
publication integration tests; focused qualification-helper checks. These
suites overlap and are not summed into a distinct-test total. Runtime sources
still match the deployed wheel byte for byte.

## Resource use and retained storage

Gateway API CPU averaged 0.064–0.080 cores; whole-host CPU averaged
0.402–0.626 cores, including the gateway-local SDK driver. Registry I/O pressure
remained visible, especially during the first burst: 31.05 ms I/O-weighted disk
await versus 3.01 ms in the fresh burst. Disk busy percentages remain excluded
because earlier measurements showed unreliable counter jumps; this run's
retained counters contained no such flagged anomalies. All 38 in-window HTTPS health probes passed.
No OOM or swap activity was observed; minimum available memory was 13.04 GiB
on the gateway and 24.69 GiB on builders. This does not qualify 500/1,000 running
agent sandboxes or their combined network/relay workload.

Local persistent BuildKit usage ended at 4.67–5.09 GB per builder. Registry
cache retention returned to 64 entries / 960,532,787 unique referenced bytes
after removing 96 old cache manifests under the existing maintenance lock.
There was no forced physical registry GC. Registry disk use was 15.57%, with
about 846 GB available. The 96 measured images reference 2.237 GB of unique
EROFS components versus 65.54 GB when summing each image separately.

Qualification image aliases, owned semantic source tags, contexts and locks
remain available under normal retention. Raw build logs and credentials stay
on production hosts; local artifacts contain sanitized receipts and telemetry.
The test reservation was released at 09:53:50 UTC and all telemetry units were
stopped. The final [provider/health/history audit](final-state.json) confirms all
seven temporary VMs retired, no active builds or sandboxes, healthy production
services, and 100/100 durable build records (96 measured builds plus four
semantic managed-image builds, including the first test-identity attempt).
The local [artifact validation](validation.json) also verifies the deployed
wheel against the five changed runtime source files.
