# VM power-control and process-signal code review

Reviewed source at `5cb043e` (release 0.5.32), the pinned gVisor upstream
commit `50e1502a95d36ad2faf2c7ef33b8bf21fe975293`, and the three patches
selected by `runtime/gvisor/build_pinned.sh`. No VM was started, stopped,
reconfigured or deployed during this review.

## Conclusion

No explicit guest reboot, poweroff or system-suspend request was found in
the reviewed application, density workload, runtime patches or provisioning
code. This is a source-review finding, **not proof that our processes cannot
cause a VM failure**. The Python services run as root and their generated
systemd units do not establish an OS-enforced prohibition on power control.

There is also a process-identity weakness: acquiring a runtime identity from
`runsc state` trusts the reported PID and records the current process's start
time without independently verifying runtime ownership. A synthetic check
confirmed that an unrelated process at that PID can be accepted. This merits
hardening, but neither proves a shutdown request nor attributes the incidents.

## Signal and shutdown paths

| Path | Actual operation | Assessment |
| --- | --- | --- |
| `direct_warden.py`, `LinuxPidfdHandle.terminate` | SIGKILL through an already-open pidfd, flags 0 | Targets one process. No numeric-PID fallback. Start ticks are checked before and after opening the descriptor. This protects against ordinary PID reuse around an already-known identity, but does not independently prove that the initial identity belongs to our runtime. |
| `direct_service.py`, `DirectProcessRunner.run` | SIGKILL to a subprocess group on timeout/output/error cleanup | `Popen(start_new_session=True)` makes a separate child session/group. The group number comes from the child object, not a request parameter. A real POSIX test verified the target and survival of a sibling in the caller's group. |
| `sandbox_exec.py` | Requested signal 1–64 to a stored `Popen` child; SIGKILL on startup cleanup failure | Caller supplies a session ID and signal, not a host PID. The command is constructed through `DirectExecRuntime` and a Warden exec lease. This does not target guest PID 1. |
| `runtime/managed_process/main.go` | Forward or send a signal to the workload child group | Runs inside the sandbox; `Setpgid` separates the launched workload group. |
| Python image/qualification/benchmark helpers | `Popen.terminate`, `Popen.kill`, or child-group SIGKILL | Reviewed calls target child processes. `images.py` also uses signal 0 solely as a liveness probe. |
| Storage `shutdown` methods and SIGINT/SIGTERM handlers | Stop the service/server/backend | These names do not invoke OS shutdown. |
| `providers/ucloud/api.py`, `terminate_jobs` | POST `/api/jobs/terminate` for explicit job IDs | Our gateway-side Python **can intentionally terminate VMs through the provider API**. Autoscaler callers use persisted stop operations and safety proofs. This is separate from a guest process signalling its own OS. |
| `vm_init.py` and supplied systemd units | Start/restart named services | `Restart=always` restarts the service. No reboot/poweroff failure action was found. |

Searches covered direct signal calls, process termination, reboot syscalls,
shell power commands, systemd/login1 control, sysrq, `/sys/power`, and
service failure actions. The density fixture performs memory, filesystem,
SQLite, socket and CPU operations; its lifecycle operations use the node API.
No machine-level suspension operation was found in it.

The OCI workload has a separate PID namespace and its default capability set
omits `CAP_SYS_BOOT`. The appearance of `SYS_BOOT` in the supported-capability
name list is not a default grant. `/proc/sysrq-trigger` is in `readonlyPaths`.
These workload settings do not remove the privileged node service's powers.

## Identity limitations that prevent an absolute guarantee

1. `_state_identity_status` and `_candidate_identity_or_none` read the PID from
   `runsc state`, then read its current `/proc/PID/stat` start ticks. If state
   is stale and the PID now belongs to another live process, the latter's
   identity can be adopted. A pidfd pins that selected process; it cannot
   establish whether selection was correct.
2. The durable Warden process identity is PID plus start ticks, without a
   boot ID. Different ticks are rejected, as an existing regression verifies.
   This is not an unconditional cross-boot identity guarantee: ticks restart
   each boot. The separate node heartbeat epoch does not change this identity
   comparison.
3. The pinned upstream gVisor teardown also uses numeric-PID SIGKILL for
   sentry and gofer processes. Our Python pidfd protection does not replace
   those downstream calls. Its `IsRunning` check uses signal 0. See the pinned
   [sandbox implementation](https://github.com/google/gvisor/blob/50e1502a95d36ad2faf2c7ef33b8bf21fe975293/runsc/sandbox/sandbox.go)
   and [container implementation](https://github.com/google/gvisor/blob/50e1502a95d36ad2faf2c7ef33b8bf21fe975293/runsc/container/container.go).
   The additional `terminateForHibernation` function in our patch has no call
   site in the reviewed selected patch series; its name alone is not evidence
   that it executes.

The synthetic identity check used the existing fake runsc/procfs test fixture:
after creating an identity, it replaced that fake process with an unrelated
name and different start ticks while leaving runsc's reported PID/status
unchanged. Candidate discovery and the fencer accepted the new identity.
All pidfd operations were mocked; no signal was sent. The result is retained
in ignored `dist/restart-review-2026-09-11/identity-provenance-check.json`.
This establishes the missing validation under those inputs, not that stale
state or a PID collision occurred during any incident.

SIGKILL cleanup is not a systemd power-control request. Systemd documents
specific signals to its manager for poweroff/reboot; no such manager-targeted
call was found. See [systemd signal documentation](https://github.com/systemd/systemd/blob/main/man/systemd.xml).
The exact-descriptor guarantees and limits are documented in
[pidfd_send_signal(2)](https://man7.org/linux/man-pages/man2/pidfd_send_signal.2.html).

## Validation and next investigation

Added seven targeted regression tests in `tests/test_process_signal_safety.py`:
six mocked pidfd boundary/error checks and one real POSIX subprocess-group
cleanup check. Together with Warden, exec, deadline and density benchmark tests,
**79 tests passed** on local macOS/Python 3.10.13. Ruff lint and format checks
passed. Linux pidfd kernel behavior was not exercised live during this review.
Production runtime code was not changed.

The next code-hardening work should establish boot-scoped runtime ownership
before adopting a process identity and cover downstream runsc deletion too.
Removing reboot privileges alone would not cover all systemd/IPC power-control
routes; a service restriction needs Linux compatibility validation because
the node and storage services perform privileged runtime and mount work.

For incident attribution, obtain the original sysadmin line with timestamp,
signal, sender/target and host-versus-guest origin. Our retained logs do not
record every signal sender/target. A future authorized reproduction should
export those events and reboot/exec audit records off the VM continuously.
Temporary diagnostic scripts that were not retained cannot be fully audited
from this checkout. No root cause is established by this review.
