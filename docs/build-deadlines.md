# Build deadlines and capacity

A successful build means its image and immutable filesystem have been published.
An accepted build has a stable ID; waiting clients may disconnect and reconnect
without canceling it or starting another copy.

## Client waiting

The SDK submission budget includes local context preparation, context upload,
builder provisioning/admission retries, and the acceptance response. Its default
is 600 seconds. `build_image(timeout_seconds=T)` gives submission and completion
one end-to-end budget. `timeout_seconds=None` retains the submission limit but
does not impose a completion-wait deadline. A client wait timing out does not
cancel an accepted build; retain its ID to poll again.

SDK 0.4.34 prepares async build contexts in a dedicated two-thread pool. Each
event loop admits at most two preparation jobs at once; other callers wait
asynchronously within their existing deadline. One immutable archive is used
through upload/admission retries. Cancelled queued preparation is discarded;
running preparation owns its files until it finishes and then cleans them up.
There is no path-based cache: a later submission snapshots current source bytes.
Keep the source directory stable while its snapshot is being prepared.

A builder status timeout is a retryable 503/504, not evidence that the build is
missing. The gateway preserves known ownership while the builder is temporarily
unreachable. Only confirmed absence from the queried builders becomes 404.
Owner hints are in-memory and disposable; loss of the owner/fleet is not durable
build recovery. Builds are not automatically replayed after losing their VM.

## Server execution

`builder.build_execution_timeout_seconds` defaults to **1800 seconds** and must
be positive and finite. Provisioning passes it to every new builder. The budget
starts when the build worker begins execution and is shared by Docker build,
push and immutable filesystem publication. Admission/context preparation and
time spent waiting outside that worker are separate from this execution budget.

On expiry the build fails. The builder terminates the owned Docker/Buildx CLI
process group, waits up to one second, then kills remaining group members and
waits up to five seconds to reap the leader. It never kills the shared BuildKit
daemon or another build. Registry operations, filesystem locks, copying, signing
and publication subprocesses consume the same remaining execution budget.

Finalization has separate bounded allowances: registry upload abort gets one
second and temporary image collection gets ten seconds. Contended image cleanup
may defer to normal garbage collection. The admitted slot releases after
finalization, including on failure. Native filesystem calls and HTTP operations
are cooperative deadline boundaries; this is not a hard preemption guarantee for
an uninterruptible kernel operation or arbitrary third-party Python callbacks.

## Capacity and cache

Builders admit at most four builds in context preparation or Docker build/push.
`builder.max_finishing_builds` optionally adds one or two slots for immutable
filesystem publication and cleanup; its backward-compatible default is zero.
With two finishing slots, a builder can have at most six owned builds, while
preparation/build/push remains bounded at four and publication/cleanup at two.
The BuildKit worker's parallelism remains four.

A build retains its preparation/build slot until a finishing slot is available.
Only then can the gateway dispatch a replacement into that preparation/build
slot. When publication slows, occupied finishing slots apply backpressure to
new builds. Waiting for the phase transition consumes the execution deadline
and is reported as `timings.phases.finishing_wait_ms`. Cleanup continues to own
capacity and prevent node drain even after the result becomes terminal.

The builder publishes its current total admission ceiling in the reserved
heartbeat label `ucloud.image-build-admission-capacity`. The gateway reads the
live value and accounts for its concurrent dispatches; the builder atomically
enforces the phase and total limits. Missing labels retain the legacy four-slot
behavior; malformed values close new admission. Replaying an existing accepted
build does not require another slot. No cache-hit prediction or sampled CPU
threshold changes these limits.

Extra requests receive retryable 503s; their wait consumes the client submission
budget. There is no additional unbounded build queue. Prebuild images before
starting an expensive training run when its burst cannot meet the caller's
deadline. Adding publication overlap does not increase CPU or registry bandwidth.

Shared cache retention prefers distinct verified build contexts before repeated
exports of the same context. Production permits 512 tags while retaining the
32 GiB unique-blob budget and seven-day age limit. Both entry count and byte
budget apply; pruning still preserves unknown aliases. See [cache policy](build-cache.md).

Qualification must record maximum end-to-end latency and every deadline miss,
including local queue time. Warm repeated builds alone do not qualify hundreds
of cold dependency builds mixed with running sandboxes.
