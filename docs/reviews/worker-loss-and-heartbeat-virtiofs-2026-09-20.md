# Worker losses and heartbeat virtiofs failure

Four workers in the production run powered off between 14:57 and 15:07 UTC.
For each worker, the provider's post-start power-off event preceded the durable
creation of our autoscaler's stop intent by 2–5 seconds. This excludes those
particular stop requests as the initiating action; it does not establish the
cause of the power-offs or exclude a workload-triggered failure. Earlier
pre-start SUSPENDED events are ordinary startup transitions, not worker losses.

The last retained samples did not show exhausted guest RAM: approximately
80–85 GB remained available, including reclaimable cache. This does not exclude
host memory pressure, guest fragmentation, or short events between samples.
Surviving busy workers showed considerable direct reclaim and compaction. A
20-second kernel profile attributed direct compaction to filesystem page-cache
reads and writes in the native storage backend and runtime, with a smaller XFS
mount allocation component. These are observed performance costs, not an
established explanation for power-off.

## Captured heartbeat failure

A bounded, off-node kernel capture recorded a live worker warning at 15:15:50:

```
python3: page allocation failure: order:4, mode:0x40820(GFP_ATOMIC|__GFP_COMP)
cpuset=ucloud-sandbox-heartbeat.service
```

The stack passed through `copy_args_to_argbuf`, `virtio_fs_enqueue_req`,
`fuse_readdir_uncached`, and `getdents64`. This is a guest kernel allocation
warning encountered by the Python heartbeat process. The worker continued
running afterward. It is not a reboot request, an OOM-kill record, or evidence
that Python intentionally terminated the VM.

The generated heartbeat unit used the shared `/work/ucloud-sandboxes` directory
as its working directory. Python's `-m` startup adds that directory to the
module search path even though installed modules are cached on local disk.
A production `strace` comparison of the same CLI's harmless `--help` command
confirmed four shared-directory `getdents64` calls with the original working
directory and zero when started from `/`; both commands succeeded. This does
not remove every shared-file access: the launcher itself remains at its
existing absolute path.

The heartbeat unit now uses `/` as its working directory. Its executable,
configuration, and authentication file paths remain absolute. This removes an
unnecessary recurring virtiofs directory scan without altering scheduling
limits, runtime versions, sandbox state, or API behavior. It does not fix the
underlying memory fragmentation or claim to prevent VM power-offs.

## Investigation limits

The four lost VMs were already gone before tracing began. Bounded live traces
on six survivors export kernel logs, reboot syscalls, and signals generated for
PID 1 to the gateway. Ordinary SIGCHLD notifications must not be interpreted as
shutdown signals. No absence of events in a later healthy interval proves what
happened on an earlier lost VM. Provider-side QEMU/Kubernetes exit, OOM, and
power-control records are still needed to establish the original loss cause.

## Qualification and deployment

Commit `2b2f5e429c8e7a319bd74d55d98d954deb3b8828` passed the canonical
`scripts/check.sh`: 974 server tests (six platform skips), 118 SDK tests,
Ruff, shell checks, managed-process Go tests, wheel builds, and installation
checks. The live comparison above exercised the actual Linux import behavior.

Applied as a configuration hotfix at approximately 15:24 UTC, without changing
the 0.5.65 runtime or 0.4.23 SDK versions:

- All five workers still running received a heartbeat-only systemd drop-in
  setting `WorkingDirectory=/`. Each heartbeat then exited successfully.
- The gateway's installed `vm_init.py` renderer received the exact committed
  file after verifying its previous SHA-256 against the parent commit. The
  original file and commit/hash receipt were retained on the gateway. Only the
  autoscaler was restarted to load it, so replacement workers receive the fix.
  This is a recorded hotfix to the installed renderer, not a rebuilt 0.5.65
  release artifact; a subsequent package installation must include this commit.
- Gateway and relay stayed active; worker runtimes were not restarted. The
  final readback found five responding workers and no pending creates.

The approximately 15:14:50–15:24:50 capture recorded no guest reboot syscall
or non-SIGCHLD signal to guest PID 1. One empty worker was terminated by a
recorded idle scale-down during capture, and four idle builders were also
intentionally stopped. There was no additional unexplained worker loss in the
provider inventory through the final readback. The four earlier power-offs
remain unresolved; this healthy observation window is not a reproduction test.

Private job-specific provider histories, stop-intent times, profiles, the full
allocation warning, off-node traces, and deployment/check logs were retained
with a SHA-256 manifest. They are not committed to the public repository.
