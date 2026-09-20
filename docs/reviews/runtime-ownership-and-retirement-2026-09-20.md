# Runtime ownership and unreachable-worker retirement

Release 0.5.63 hardens process ownership and the decision to retire a silent
UCloud worker. Neither change establishes the cause of the earlier VM power-offs.
The investigation follows the [earlier signal review](vm-power-signal-code-review-2026-09-11.md).

## Process ownership

The Warden previously accepted a PID from runsc state and adopted its current
start time. Synthetic stale state reproduced adoption of an unrelated process.
The initial lookup now requires the expected executable, runtime root, OCI bundle,
container ID, process role and cgroup. Start ticks bracket these checks, ownership
is verified again after opening a pidfd, and a durable boot marker rejects
cross-boot adoption. PID 1 and process-group selectors are never valid targets.
The pinned distribution's packaged sentry sidecar is explicitly supported.

Cleanup also fences the sentry and gofer through verified pidfds, then clears
runsc's numeric PID fields under its metadata lock before invoking runsc teardown.
This covers the downstream numeric-kill path identified in the earlier review.
The integration is tied to the pinned runsc metadata schema and must be qualified
again when changing the runtime. Unexpected provenance fails closed. Process
exit during lookup is distinguished from a live process with another owner.

## Worker retirement

Silence does not prove suspension. Provider history showing SUSPENDED after
RUNNING remains independent evidence of execution loss, including when the
current provider state returns to RUNNING.

The configured unreachable deadline remains effective. Before declaring a
worker lost because that deadline expired, the controller fetches an authenticated
heartbeat directly from it. A fresh response with matching job, node and deployment
identity repairs the missing push heartbeat and prevents retirement. Transport
failure after the deadline permits deliberate retirement, including loss of
assigned sandboxes; it does not mean their prior death was established. HTTP
authentication failures, invalid response schemas and identity mismatches do not
authorize retirement. The plan exposes the probe outcome. Old stop proofs lacking
the new probe evidence cannot authorize a new termination request.

Once a retirement decision has durably fenced sandbox ownership, its existing
provider-operation journal governs completion and retry. It must not resurrect
assignments merely because a heartbeat later returns.

## Isolated experiments

A dedicated worker was used, separate from the production pool. Tests covered:

- Real sentry/gofer provenance and adoption across node-agent restart.
- A stale gofer PID pointing to an unrelated host process: cleanup rejected it;
  both that process and the sandbox survived.
- Reboot, poweroff and halt calls inside ordinary and init-enabled containers:
  denied with default capabilities. With explicit SYS_BOOT, including SYS_ADMIN,
  gVisor returned ENOSYS. Sysrq writes failed in each configuration.
- Killing container PID 1 and attempting namespace-wide SIGKILL: the worker and
  an unrelated sandbox survived. Kernel tracing during the initial probes
  recorded no host reboot syscall or signal to host PID 1.
- Thirty-two persistent sandboxes, four concurrent operations, two park/resume
  cycles and a node-agent restart while parked: all 64 restores preserved process
  nonce, memory and filesystem state. Cleanup completed.
- A managed primary process survived three park/resume cycles with its job
  still reported running.

The experiments exposed exit races in the initial hardening patch; those were
corrected and covered by regression tests before release. These checks are not
an exhaustive proof against every possible VM failure, and they do not represent
a new 256- or 512-agent end-to-end capacity qualification. Private raw evidence
is kept outside the public repository.

## Release checks

The canonical local check passed: 969 server tests (six skipped), 118 SDK tests,
Ruff, shell checks, Go tests, wheel builds and installed-wheel checks. The initial
Linux subset passed 180 tests; release validation also exercises the final wheel
on Linux before installation. No SDK or Verifiers behavior changes are required.
