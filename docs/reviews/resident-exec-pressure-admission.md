# Resident execution under memory PSI

Node-wide memory PSI includes file-cache reclaim and stalls from individual sandbox cgroups. Treating it as an unconditional admission veto for every command prevents already-resident workloads from making progress, even when physical memory headroom is sufficient.

Resident execution now ignores the PSI placement threshold, as it already ignores CPU placement thresholds. Physical memory headroom, missing-metrics rejection, generation ownership, drain fencing, and full-lifetime execution leases remain enforced. New sandbox creation and restoration retain the existing pressure-based placement checks.

Restore admission now reports its safe-retry guarantee at the point where admission fails, before resume begins. This includes implicit restoration from other API operations and does not depend on a subsequent inventory read succeeding. Capacity errors after entering the resume body are not reclassified by the admission wrapper.

Tests cover resident exec and file operations under PSI, physical-memory exhaustion despite free swap, missing metrics, unchanged create/restore pressure rejection, lease cleanup, and distinguishing admission failures from errors after resume begins.

Validation: 943 server tests (6 skips), 118 SDK tests, Go tests, lint and installed-wheel checks passed. The release also passed 350 Linux tests (1 skip) and boot validation for both node bundles. [CI run 35503069097](https://github.com/rlrs/ucloud-sandboxes/actions/runs/35503069097) passed.

The deployed 0.5.60 release passed a live 64-sandbox check with SDK 0.4.23: all creates, 32 concurrent large uploads plus 320 small writes, park/resume operations, and restored checksums passed. Small-write p95 was 1.909 seconds during the upload burst; resume-and-tool p95 was 11.747 seconds. Health probes and cleanup passed. Cold creation included worker provisioning and SDK retries for unavailable capacity. This check does not establish 256- or 512-agent capacity.

## rc14 preparation cleanup, 2026-09-23

Exec start now uses the required capacity-admission contract instead of first
materializing an unused inventory record. The removed read repeated lifecycle
inspection and sentry liveness checks immediately after shared activity had
ensured the runtime was running. Capacity admission validates current owned
registration after resource sampling; its redundant earlier registration read
is also removed. Acquisition and release are both required operations, with no
silent path that skips admission.

No registry snapshot becomes authority across the preparation interval. Forced
delete can still preempt shared activity, so post-sampling generation and drain
checks remain mandatory, as does the final Warden exec lease under its lifecycle
lock. The existing service generation selection and runtime lease are unchanged.
Tests use the real registry/service to revoke ownership during sampling, close
admission, fail the final Warden lease, and verify that failed preparation cannot
dispatch or leak capacity/activity leases. A separate check proves that normal
preparation no longer builds an inventory record. Loaded latency qualification
is separate from these component tests.

The rc15 pressure test later found an error-classification gap in managed-primary
startup: its 30-second growth admission deadline expired before supervisor
dispatch, but the caller received an untyped 503. rc16 maps only this pre-dispatch
admission failure to the existing `node_startup_busy` retry contract, retaining
the queued primary identity. The supervisor RPC stays outside the conversion;
an ambiguous error after dispatch does not authorize replay. The equivalent
pre-acknowledgement continuation boundary uses the same retry type without
claiming that a retained live sandbox is parked. Gateway routing remains pinned
to the assigned incarnation, and released SDK 0.4.26 retries within the caller's
deadline without changing the primary job ID.
