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
