# Registry I/O qualification — prepared protocol

This directory initially contains the prior run's registry summaries and the
next release's controller/protocol. The candidate deployment and qualification
below are not claimed complete until their receipts are added. All preparation
here was local; no production calls were made by this preparation step.

## Baseline and intended comparison

Use the `opt-repeat` phase in
`../build-optimization-2026-09-29/opt-repeat/summary.json` as the baseline:
48/48 successful builds on four fresh CCX33 builders, 122.0649 seconds batch
duration, client p95 118.6523 seconds, and 48 durable terminal-history rows.
The reported physical registry-volume write delta was 5.518 GiB. Execution and
admission were both four per builder, including preparation; no extra local
build queue was admitted. The baseline wheel is
`f38db1aae25b0ddb14401fce61d80e299a2d8740b2586998e521b21ef0a26a2f`.

The accompanying [registry summary](baseline-repeat-registry.json) covers
09:41:55–09:43:58 UTC. Managed-image upload commits referenced 5,339,586,891
bytes. One 313,045,072-byte digest appeared in ten completed upload commits,
accounting for 3,130,450,720 bytes of that total. These are immutable blob sizes
associated with completed registry events; they are **not** measured HTTP body
bytes, unique retained storage, or physical block-device writes.

The candidate should link already-known matching cache layers into the new
managed-image repository before BuildKit pushes them. A successful registry
mount avoids the repeated upload path; a miss must preserve the original
build/push behavior. This does not promise to eliminate dependency downloads,
cache reads, genuinely new layers, EROFS publication, registry metadata IO, or
all physical writes. Record the final candidate scope and per-operation counters
alongside the wheel hash so another changed optimization is not silently
attributed to mounting alone.

## Pinned deployment and rollback

The new `deployment-controller.py` is the previous qualified controller with
only three substitutions: release ROOT
`/work/ucloud-sandboxes/registry-io-20260929-r1`, plus unique
`.registry-io-candidate` and `.registry-io-rollback` config temporary filenames.
The controller preserves its existing guards and changes only
`node_package_root` in the production configuration. It retains the relay's
64-GiB budget, cache policy, gateway/worker sizes, dependency bytes, native
artifacts, and service settings.

Stage the candidate wheel, unchanged `scripts/repack_node_bundle.py`, and this
controller under the new ROOT. Source bundles come from the **currently deployed**
`/work/ucloud-sandboxes/build-optimization-20260929-r1`, not the older BuildKit
release. Frozen source bundle hashes:

* Builder: `e4f775e6ca8911f94f778c4a2f76f53e0708df2430a4f9423737e5038d7a3091`
* Sandbox: `ec49a3aadbcff6491059ad2b267152eeb7561d8cdc62e471f485c205f36ea7b3`
* Repacker: `9fddb39307442fabea50f78c80b296a5c32b2e00952e7624af7c02709bd0da5b`

```sh
/work/ucloud-sandboxes/gateway-venv/bin/python \
  /work/ucloud-sandboxes/registry-io-20260929-r1/deployment-controller.py stage \
  --wheel-sha256 CANDIDATE-WHEEL-SHA256 \
  --repacker-sha256 9fddb39307442fabea50f78c80b296a5c32b2e00952e7624af7c02709bd0da5b \
  --builder-source-sha256 e4f775e6ca8911f94f778c4a2f76f53e0708df2430a4f9423737e5038d7a3091 \
  --sandbox-source-sha256 ec49a3aadbcff6491059ad2b267152eeb7561d8cdc62e471f485c205f36ea7b3
```

Keep the resulting staging receipt and its SHA256. Pass that exact digest to
`check --receipt-sha256 DIGEST`, then the authorized
`apply --receipt-sha256 DIGEST`. The controller fences configuration/source
changes between stage and apply, confirms idle sandboxes/builds/relay lifecycle,
captures a fresh gateway-venv/config rollback, preserves dependency/native
inventories, and checks local/public health in a fresh interpreter. Failure
automatically restores this release's backup. Explicit rollback is this new
controller's `rollback` command; never use the older release's snapshot to undo
this deployment.

Drain and replace only idle builders. The gateway's new package-root pointer
does not update existing nodes. Provision four fresh candidate CCX33 builders
and verify installed source/bundle fingerprints and empty local BuildKit cache
before qualification. Preserve the shared registry cache; do not clear it,
drop page caches, delete layer tags, or run global GC to manufacture a cold run.

## Small API/fallback canaries before the burst

The main implementation's unit tests must cover a successful known-source
mount, missing source/unsupported mount, bounded candidate selection, and the
unchanged build/push path when no usable source exists. Preserve atomic build
admission, manifest integrity, and original errors on ordinary upload failure.

A separate live API canary may use a unique owned source/target repository and
a deterministic payload of at most 64 KiB. Mount the known blob into the empty
target, require the successful mount response, and verify the target's size and
digest through the ordinary registry read API. For a deliberately nonexistent
owned source, require a graceful miss and complete the ordinary small upload;
read back and verify its bytes. Bound the number of attempts. An unsupported
mount may create an upload session: close only that owned session through the
normal client cleanup, never a broad registry delete/GC. Retain sanitized
status/digest/byte receipts; keep tokens, raw authorization headers, and request
bodies out of the report. This canary is implemented/run by the main task;
this preparation adds no production executor for it.

## Repeat the exact 48-request workload

Keep the baseline SDK 0.4.33 and frozen harness:

* `live_build_load_benchmark.py`: `d0b2754af69c69d9a303fd15117f60fad044ca0e559b3a1cfbe934f49682c6ec`
* `qualify_build_optimization.py`: `e9c60880598396d18aa377cc02b5659d9f4d3610cf2ae65411b829361beb288b`
* Original fixture inventory: `45efcdbd714d7fbc6748b26f7fcdc29b4b18d1f4117baac0feb8af24e32505a7`

Use the untouched contexts at
`/work/ucloud-sandboxes/build-load-20260929/contexts`: three recipes × application
revisions 5–20, exactly 48 concurrent requests. Do not substitute the later
fresh-edit revisions 29–44. Use a separate preparation reservation such as
`registry-io-20260929` with count four and bounded TTL; release only that ID.

```sh
/work/ucloud-sandboxes/gateway-venv/bin/python \
  /work/ucloud-sandboxes/qualify_build_optimization.py \
  --source-root /work/ucloud-sandboxes/build-load-20260929 \
  --output-root /work/ucloud-sandboxes/registry-io-load-20260929 \
  --fixture-manifests /work/ucloud-sandboxes/FROZEN-fixture-manifests.json \
  --harness /work/ucloud-sandboxes/build-load-20260929/live_build_load_benchmark.py \
  --harness-sha256 d0b2754af69c69d9a303fd15117f60fad044ca0e559b3a1cfbe934f49682c6ec \
  --phase io-repeat --candidate B2-selective-and-scheduling \
  --artifact-sha256 CANDIDATE-WHEEL-SHA256
```

The first invocation verifies all frozen bytes and prints the plan; add `--run`
to execute. The wrapper's existing `B2-selective-and-scheduling` label records
the preserved EROFS/admission configuration; the new phase and candidate wheel
digest identify this registry-I/O release. This deliberately avoids editing the
frozen driver for a new label. Preserve `io-repeat.qualification.json` and the
48-request summary. Never reuse a phase/image ID or overwrite evidence.

The driver retains 1,200-second per-request limits, SDK retry behavior, fresh
heartbeat/disk guards, and no-unrelated-work checks. The same four-node count,
four execution/admission slots, cache import selection, export mode, GC policy,
base digests, dependency locks, and request order must remain unchanged.

## Measurement and acceptance

Start bounded `build_load_telemetry.py sample` collectors on the gateway and
all four builders before the request barrier (2-second interval, 1,800-second
maximum). Verify which block device backs the registry volume after provisioning;
do not assume a name or sum whole-device and partition counters together.
Retain at least 30 seconds after the final build so delayed writeback is visible.
Report the exact request-to-completion window and the separate post-run tail.

Preserve these three independent measurements:

1. SDK/build results: 48 distinct builds, terminal status, durable history count,
   client p50/p95/max, batch duration, submission/retry waits, node preparation,
   queue and execution timing, EROFS cache/path counts, and new mount counters.
2. Registry behavior: successful mount responses versus miss/fallback attempts,
   upload commits and immutable blob sizes by repository kind, repeated digest
   counts, GET response bytes, and request-duration distributions. Use the same
   log parser and classifications as `baseline-repeat-registry.json`. Logs
   count only completed events in the selected window; account for uploads
   spanning its boundaries. Concurrent request-duration sums are not wall time.
3. Host resources: actual registry block-device read/write-byte deltas, await,
   queue, IO/CPU PSI, dirty/writeback memory, host/service CPU, and builder
   utilization. The known anomalous raw disk busy counter remains excluded.
   Sector counts use 512-byte sectors. Physical writes include filesystem
   metadata/journal/cache export/EROFS activity and need not equal blob bytes.

Do not reinterpret HTTP 201 upload-commit counts as cross-repository mount
successes: both can use 201 on different requests. Classify the mount POST
separately. Do not treat a mounted blob's descriptor size as a measured network
transfer. BuildKit `CACHED` lines and EROFS group reuse do not prove no OCI push;
the baseline demonstrated that distinction.

Hard gates are 48/48 successful distinct builds, 48 durable history rows,
unchanged admission/execution bounds, healthy gateway/relay, and real sandbox
smokes for all three recipes using the existing
`build_load_qualify_images.py --root ROOT smoke --phase io-repeat`. A cache miss
must still build successfully. Cleanup must remove all owned smoke sandboxes
and release only the owned builder reservation.

Report the actual before/after write and upload reductions, including any
latency regression. A successful optimization should materially reduce repeated
managed-image uploads and physical writes without causing correctness failures
or moving substantial time into preflight scans. Do not hide extra HEAD/GET/
mount requests: count them and include their latency in the client total.

This remains a historical comparison. Fresh local caches make fleet state
closer, but registry page-cache warmth, the current eight selected shared-cache
entries, retained EROFS groups, cache LRU order, underlying volume noise, and
concurrent retention maintenance may differ from the 09:41 run. Capture selected
cache references per build and retention activity. A second warm candidate
repeat is a separate observation, not a substitute baseline. No added load,
source generation, or pruning should occur inside the measured window.
