# RAM worker bootstrap qualification

The first full RAM-active deployment candidate (0.5.114rc5) failed fresh-worker
bootstrap before accepting any sandbox. Native runtime/component tests had passed,
but did not cover the assembled worker image and its systemd configuration.

Two integration defects were found and corrected:

1. `replace_direct_runtime` copied the gVisor build manifest without adding its
   size/hash descriptor to `runtime.direct_runsc.build_manifest`. The fresh-worker
   validator rejected the unauthenticated file. Repacking now records and validates
   the descriptor; the regression executes the complete generated bootstrap
   validator on the repacker output and checks rejection when metadata is removed.
2. Split-memory provisioning mounts XFS at the volume mount root. The storage
   service runtime root remained on the parent filesystem, violating the existing
   atomic-restack requirement. Split workers now put runtime artifacts in the
   reserved `.runtime` directory on that XFS filesystem. Legacy workers retain
   their old path; this is a fresh-worker layout, not an in-place data migration.

The first natural-64 driver was cancelled without creating sandboxes; its wait
for bootstrap is not a performance measurement. The autoscaler retired the empty,
unreachable workers through its existing eviction policy. These were owned test
workers, with no workload loss. Candidate rc6 includes both bootstrap corrections;
its load results must be assessed separately from rc5 and earlier runtime-only
qualification.

The isolated native qualification VM (12399833) was stopped after native runtime,
physical-write comparisons and optional environment-artifact qualification
completed. Evidence is retained in the corresponding benchmark directories.

Candidate rc6 then reached the real storage service and exposed a third wiring
error: runtime assembly called the daemon's `metrics()` name on the RPC client,
whose API is `get_metrics()`. Candidate rc7 reuses the already-authenticated
`wait_ready()` response, avoiding both the nonexistent method and a redundant
metrics RPC. The added Unix-socket fixture constructs the real storage service
and client rather than granting arbitrary methods to a mock. rc6 also accepted
no sandboxes; its waiting driver was cancelled before replacement.
