# Durable sandbox snapshots in object storage

## Decision

Local NVMe remains the working store. Running sandboxes, their writable COW
layers, gVisor application-memory files, Docker layers, and journals stay on
the worker. An ordinary park is also retained locally, so the common
same-worker wake path does not depend on a network service.

Object storage is the durable authority for a park that must outlive its
worker. Before autoscaling may detach the park or stop the node, the worker
must publish every sealed storage-native layer and a content-addressed
manifest directly to S3-compatible storage. A later detached wake can run on
any worker. AgentEnv reads the remote lower layers with native S3 range reads
through its bounded node-local cache; new writes still land on local NVMe.

The gateway is the metadata authority, not a data proxy. Its route journal
stores the exact publication descriptor and sandbox incarnation. Snapshot
bytes never flow through the gateway or its Hetzner Volume. Docker
Distribution remains the OCI image/build registry only.

This matches the storage model in AgentEnv v0.1.2: its OSS snapshot repository
places managed layers below `managed-layers/`, emits `s3://` OverlayBD lower
URLs, and configures the runtime with `ossConfig`. We retain our streaming
dense exporter because AgentEnv's repository currently stages a complete
local file before upload. Relevant upstream sources are the
[OSS configuration](https://github.com/kvcache-ai/AgentENV/blob/db1492b7915a408b37f863c9e3a34b2ccb2fb1b0/config/default.toml),
[repository backend](https://github.com/kvcache-ai/AgentENV/tree/db1492b7915a408b37f863c9e3a34b2ccb2fb1b0/src/snapshot/repository/backends/oss),
and [native range-read backend](https://github.com/kvcache-ai/AgentENV/blob/db1492b7915a408b37f863c9e3a34b2ccb2fb1b0/storage/overlaybd/src/backend/oss.rs).

## Publication transaction

1. Freeze and seal the local writable layer as today.
2. Stream AgentEnv's dense or compacted output into a multipart upload under
   `<prefix>/.uploads/<uuid>`, independently hashing and counting the stream.
   Parts are 64 MiB and at most four are uploaded concurrently, bounding one
   publication's payload buffers at 256 MiB.
3. Reject the export unless its observed digest and size exactly match the
   descriptor returned by AgentEnv.
4. Complete the temporary object, server-side copy it to
   `<prefix>/managed-layers/sha256:<digest>`, and verify the final size.
5. Write the small content-addressed snapshot config and manifest only after
   every referenced layer is durable.
6. Journal the complete publication on the worker and gateway. Only then may
   local sealed layers be removed and worker ownership be detached.

An interrupted multipart upload is aborted. If completion succeeds but its
HTTP response is lost, the publisher resolves the ambiguous outcome by
checking the temporary object's exact size before continuing. A failure before
the manifest is written leaves the previous publication and local delta
authoritative. Object keys are content-addressed, so concurrent writers of the
same digest are idempotent. Temporary-upload lifecycle expiry is still
required as defense in depth for process or node loss during an upload.

A publication never mixes blob origins. The first publication after changing
from Registry storage to S3 compacts the old remote chain and current local
delta into one S3-backed layer. Old Registry descriptors remain readable while
the transition is in progress.

## Configuration and credentials

Deployment schema 5 keeps snapshot authority and OCI registry storage as
independent deployment choices. S3 credentials are referenced by
environment-variable name and are never serialized into `deployment.json`:

```json
{
  "snapshot_store": {
    "kind": "s3",
    "endpoint": "https://fsn1.your-objectstorage.com",
    "bucket": "ucloud-sandbox-snapshots",
    "region": "fsn1",
    "prefix": "production",
    "access_key_id_env": "UCLOUD_SNAPSHOT_S3_ACCESS_KEY_ID",
    "secret_access_key_env": "UCLOUD_SNAPSHOT_S3_SECRET_ACCESS_KEY",
    "security_token_env": "UCLOUD_SNAPSHOT_S3_SECURITY_TOKEN"
  }
}
```

The private OCI registry independently selects a filesystem or S3-compatible
Distribution storage driver. A UCloud deployment can retain its durable
filesystem mount:

```json
{
  "registry_store": {
    "kind": "filesystem",
    "mount_point": "/work/data",
    "data_root": "/work/data/ucloud-sandbox-registry/docker-registry",
    "endpoint": "",
    "bucket": "",
    "region": "",
    "prefix": "",
    "access_key_id_env": "UCLOUD_REGISTRY_S3_ACCESS_KEY_ID",
    "secret_access_key_env": "UCLOUD_REGISTRY_S3_SECRET_ACCESS_KEY",
    "force_path_style": false
  }
}
```

An object-storage deployment instead leaves the filesystem fields empty and
configures its own bucket or prefix. OCI content and direct sandbox snapshots
must use distinct prefixes and independent collectors even when they share a
bucket. The registry's native driver owns OCI layout and redirects; the
snapshot publisher owns storage-native manifests and range-readable layers.

The gateway autoscaler resolves the named variables only while rendering a
worker bootstrap. The worker installs a root-only credential file and a
root-only credential-process executable shared by the Python multipart
publisher and AgentEnv. Static Hetzner S3 keys are re-read for every
publication and every new AgentEnv credential resolution; rotating a key
requires replacing the worker credential file or reprovisioning the worker.

Older deployment schemas are intentionally rejected. Before deploying schema 5,
construct a complete current document and map the old registry paths explicitly
to `registry_store.kind=filesystem`; keep `snapshot_store.kind=registry` until
the backend transition is ready. Loading configuration never moves bytes.
`ucloud-sandboxes-registry-migrate` dry-runs and verifies an explicit,
stopped-registry filesystem-to-S3 copy before the configuration is switched.
Changing either production backend remains a separate rollout after its
bucket, key policy, lifecycle policy, and performance gate pass.

## Retention and garbage collection

The gateway's durable routes, retained storage dependencies, and unfinished
migrations form the reference set. A successful wake clears the old snapshot's
restore authority, but its remote lower layers remain live: running volumes
read them lazily. The routing journal therefore retains a separate dependency
record until a newer complete publication replaces it or the route is deleted.
Garbage collection must be mark-and-sweep, not age-only deletion:

- mark every manifest and layer reachable from a live route, pending detach,
  or retained rollback publication;
- when the final durable reference disappears, write a durable first-seen-
  unreferenced marker under `<prefix>/.gc/unreferenced/`;
- sweep an object only after it has remained continuously unreferenced for the
  full rollback grace period;
- sweep a managed layer only when no retained manifest references its digest;
- expire incomplete objects under `.uploads/` after 24 hours with an S3
  lifecycle rule;
- enable bucket versioning or object lock only if its additional retained-byte
  cost and deletion semantics are accepted explicitly.

`ucloud-sandboxes-snapshot-gc` implements this mark-and-sweep operation. It is
dry-run by default, refuses to exceed its configured deletion bound, and
aborts if a protected manifest is missing or malformed. A daily systemd timer
runs it with a seven-day grace period. The grace clock starts on the first
sweep after the final route reference disappears, not at object creation; a
returning reference clears the marker and a later write restarts the clock.
This deliberately retains objects for between seven and roughly eight days
after last route use with a daily sweep. Bucket lifecycle is still responsible
for abandoned multipart uploads because they are not ordinary listed objects.

Running-sandbox heartbeats report remote layer dependencies independently of
parked restore metadata. The collector refuses to delete when any route lacks
a dependency report or retained publication; a new local sandbox reports an
explicit empty dependency set. This also covers a worker that publishes and
wakes before the gateway observes the parked snapshot.

When upgrading, update the workers and gateway before resuming the snapshot GC
timer. Existing parked publications are backfilled automatically. References
erased by an older gateway are recovered from current worker heartbeats;
collection remains blocked until every route has dependency metadata. An
unreachable worker must recover or its sandbox must be retired before that
proof can be completed.

Each worker admits up to four concurrent snapshot publications by default,
configured by `sandbox.storage_native_max_concurrent_publications`. This
queue is deliberately narrower than the storage daemon's global operation
limit, leaving slots available for wake, mount, release, and delete work. Park
still returns after the local checkpoint when background
publication is requested. A wake arriving before publication completes gets a
retryable `snapshot_publication_pending` response with `Retry-After: 1` instead
of holding an HTTP request behind a long remote upload. The SDK retries only
this exact pre-dispatch fence for non-idempotent operations. Once publication
is complete, the worker caches the descriptor and reports it in heartbeats; the
gateway validates it and acquires the Registry reference before granting
portable route authority. Heartbeats do not rebuild descriptors or scan the
Registry lease database.

The same publication gate, phase spans, and metric keys apply to Registry and
S3 backends. `sandbox.storage_native_max_ublk_devices` independently bounds
active plus pooled ublk devices; its default is 128, while the warm pool remains
bounded by its separate high watermark.

## Performance qualification

Hetzner describes Object Storage as shared infrastructure, recommends
multipart upload above 100 MB, favors objects of at least about 1 MB, and warns
that bursty or sustained high concurrency can be throttled with 503 responses.
Those constraints fit sealed layers better than mutable COW I/O, but require a
real load test rather than an assumed SLO.

Qualification records both API latency and time until useful work completes:

| Case | What is measured | Initial acceptance gate |
| --- | --- | --- |
| Attached local wake | park, wake API, first command, checksum | no material regression from the current local baseline; p95 wake remains below 750 ms |
| Detached warm-cache wake | import, mount, resume, first command | p95 first-command latency below 3 s for the qualified 1 GiB/2 GiB sandbox |
| Detached cold-cache wake | same, with remote cache dropped | report by changed-byte/working-set size; no correctness failures or full-layer predownload |
| Publication | seal through durable manifest | throughput, p50/p95, local temporary bytes, and retry count for 100 MiB, 1 GiB, and 10 GiB changed layers |
| Burst | concurrent detach and cold wake | 1, 2, 4, 8 workers; zero corrupt descriptors, bounded cache/disk, successful 503 retry |
| Faults | exporter crash, worker loss, 503/timeout, missing object | old publication remains wakeable and scale-down remains fenced |

Every run validates filesystem sentinels, managed-process identity, checkpoint
state, and post-wake reads across the full touched working set. Measuring only
the mount or wake HTTP response is insufficient because lazy remote faults may
move the cost into the first command.

The production switch requires a real Hetzner bucket in the same location as
the workers. A local S3-compatible test proves protocol and transaction
correctness but cannot establish Hetzner tail latency or throttling behavior.

### Hetzner `hel1` qualification, 2026-08-13

The production path was exercised between a CPX62 worker and the `sandboxes`
bucket in `hel1`. These are individual qualification observations rather than
p95 claims; the machine-readable record is
[`hetzner-object-storage-snapshots-2026-08-13.json`](benchmarks/hetzner-object-storage-snapshots-2026-08-13.json).

| Operation | Result |
| --- | --- |
| Local attached park / wake | 0.227 s / 0.544 s; exact sentinel |
| Small remote snapshot | 113.2 MB; 14.621 s serial publication before uploader tuning |
| Small detached cold wake | 2.258 s and 2.690 s; exact sentinel |
| Parallel publisher, 256 MiB | 3.245 s, 78.89 MiB/s; full digest verified |
| Parallel publisher, 1 GiB | 9.488 s, 107.93 MiB/s; full digest verified |
| Real detach after 256 MiB mutation | 7.076 s; 539.6 MB new layer |
| Seven-layer cold wake | 7.978 s; then full 256 MiB read in 3.479 s with exact digest |
| Compact publication | 13.268 s; one 650.5 MB remote layer |
| Compacted cold wake | 11.223 s; then full 256 MiB read in 1.060 s with exact digest |
| Fresh-worker 0.4.1 release canary | create 0.974 s, park 0.229 s, detach 3.414 s, detached wake 2.473 s; exact sentinel |
| Promoted snapshot `419550929` canary | snapshot-baked dynamic init 4.525 s; first park/detach/wake 0.286/4.017/9.212 s; three-cycle warm medians 0.247/0.897/2.703 s; all checks exact, zero retries |

For the forced-cold cases the node services were stopped, the recoverable
remote-block cache was deleted, and services were restarted before wake. They
therefore include a deliberately pessimistic recovered-service/device-pool
state, not just S3 range latency. After the full 256 MiB verification read the
remote cache contained 263 MiB, which confirms lazy range population rather
than mandatory full-layer predownload during the wake call.

The qualification found temporary `403 AccessDenied` and `404 NoSuchBucket`
responses distributed across Hetzner S3 gateway instances immediately after
bucket/key creation. Identical signed requests alternated between success and
failure. The client now applies a bounded Hetzner-only retry for exactly those
propagation responses; other authorization and missing-object errors remain
terminal.

The release canary also forced Hetzner to reuse `10.42.0.3` immediately after
deleting the previous worker. The gateway initially retained the accepted
stop's old heartbeat binding across a service restart and correctly rejected
the new job's conflicting identity. The autoscaler now recovers an accepted
delete when its target disappears from exhaustive provider inventory, retires
the orphan heartbeat, and permits the replacement job to bind the reused IP.

The final golden-image qualification started from an empty worker pool and the
Hetzner API confirmed that the replacement CPX62 used snapshot `419550929`.
The snapshot-baked bundle fast path spent 90 ms validating the package and
4.525 seconds in all dynamic initialization. Hetzner took about 69 seconds from
accepted create to SSH readiness; that provider restore/boot interval remains
the dominant cold-node cost. The first detached wake on the newly started node
took 9.212 seconds. A following three-cycle run on the same node produced
detached-wake times of 2.873, 1.996, and 2.703 seconds, showing the first result
was startup/cache warmup rather than steady remote-layer latency. Every cycle
preserved the managed-process PID and spec hash, advanced the checksummed
counter, and completed without a lifecycle retry.

## Rollout

1. Create the private bucket and least-privilege S3 key; add an incomplete
   multipart-abort lifecycle rule.
2. Run publication, cold/warm wake, concurrency, and fault qualification on a
   disposable worker.
3. Deploy schema 5 and the reference-based GC timer.
4. Change the primary backend to `s3`. Keep the old Registry during
   the rollback window; old descriptors remain readable.
5. Observe publication latency, first-command wake latency, S3 retries, cache
   hit rate, worker local bytes, and gateway network traffic.
6. Dry-run reference-based object GC before enabling scheduled deletion.
7. Once no live Registry-backed park remains, remove sandbox snapshots from
   Docker Distribution. Retain that registry for images/builds.
