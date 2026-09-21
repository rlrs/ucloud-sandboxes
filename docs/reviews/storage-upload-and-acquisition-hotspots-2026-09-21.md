# Upload reuse and device acquisition hotspots

Two regressions were reproduced locally against the 0.5.71 source. These are
code-path findings, not measurements of their frequency in production.

## Immutable layer pins invalidated completed uploads

The upload cache included inode ctime in its input identity. Background local
compaction and the published-local cache hardlink sealed layers to keep them
alive. Creating or removing those links changes ctime without changing content.
If that happened during export, the publisher rejected the completed upload as
mutated; afterward, it missed the completed-upload cache and exported again.
It could also lose the association used to populate the published-local cache.

The identity now uses path, device, inode, size and mtime. This avoids reading
and hashing the source just to find a cached export. Sealed inputs remain trusted
immutable files; this metadata guard is not a replacement for that contract or
for the digest computed by export. Replacement and ordinary content writes still
invalidate reuse. Remote blob existence and lifecycle ownership checks remain.

The regression test creates a hardlink during real Unix-socket export and removes
it before retry. It failed in all four Registry/S3 and dense/compacted cases
before the fix. It now publishes successfully and reuses the completed blob
without another export. A separate same-size write during export is rejected.

## Uncapped device acquisitions unnecessarily serialized

The node service held its device admission lock across the native device-create
RPC, even when the operator device ceiling was disabled (the default). A slow
acquisition therefore blocked independent creates and restores on the worker.
It also fetched the complete owner inventory for an unused capacity check.

The uncapped path now skips that lock and inventory fetch. Native per-owner
fencing still coordinates duplicate acquisitions and device transitions. When
an operator explicitly configures a device ceiling, the existing admission lock
and reservation-to-owner transfer remain unchanged. Pool diagnostics still
take their existing inventory snapshot.

The regression test holds one native acquisition open and requires a second
volume to reach MOUNTED before the first is released, with pooling both enabled
and disabled. Both variants timed out before the fix and now pass. Existing
capacity, failed-acquisition, retirement and ownership tests also pass.

These changes need production deployment and loaded measurement before assigning
an end-to-end speedup or claiming greater supported concurrency.

Validation: the full server suite ran 1,045 tests successfully (six skipped);
Ruff on the changed Python files and `git diff --check` passed. No native backend
binary, SDK or Verifiers changes are required.
