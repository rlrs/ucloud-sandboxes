# Worker resource evidence

`NodeRuntimeMetrics.resource_evidence` is optional diagnostic evidence. The
worker's process-wide collector reads it once per second in one daemon thread.
HTTP/admission callers only read the cached value; the first sample and samples
older than five seconds are unavailable (`null`). Existing admission sampling
and physical safety rules retain their current freshness and decisions.

The value has its own UTC `collected_at` and monotonic `interval_seconds`.
Memory/cgroup fields and device rates remain `null` when unavailable. A zero
means a counter or rate was actually observed. Await is unavailable when no
corresponding requests completed during the interval.

- Host dirty, writeback, mapped, shared and cached memory are bytes. Mapped/cache
  are node-global evidence, **not** exact private sandbox attribution.
- Host major faults and anon/file refaults are cumulative kernel counters.
- `cgroup_path` identifies the agent's cgroup v2 scope. CPU usage/throttling are
  cumulative microseconds; throttled periods and fault/refaults are cumulative
  counts. Memory values are bytes. This scope may exclude sandbox processes;
  it must not be presented as total sandbox or fleet consumption.
- Device data contains read/write bytes per second, operations per second,
  completed-operation await in milliseconds, average queue depth, and busy
  percentage. Linux diskstats sectors always use 512-byte units here.

Devices are **guest-visible leaf whole block devices**, not a claim about
provider-side physical disks. Partitions, sysfs virtual devices (including
ublk, loop and device-mapper), and devices with lower-layer slaves are excluded.
Never add their counters to these device values. Device identity combines the
boot ID, major/minor, disk sequence and sysfs path. Missing identity, first
observation, a counter reset or device replacement cannot yield an interval
rate. Disappearance removes the prior baseline. Up to 256 devices are retained.
The collector does not infer filesystem-to-device or per-sandbox attribution.

The lightweight memory/PSI parser is shared with background pacing and warm
retention. It preserves missing fields; each policy explicitly selects its own
conservative fallback. Diagnostic collection never changes admission policy.

## Upgrade order

Upgrade the gateway/control-state readers **before** workers. The new decoder
accepts old heartbeats without `resource_evidence` and preserves canonical
legacy persisted records; new heartbeats include the optional nested object.
Older strict heartbeat decoders do not accept new fields. Rollback therefore
requires reverting workers before reverting the gateway, or retaining the new
reader until all new-format heartbeats have aged out. There is no claim of
bidirectional schema compatibility.

Existing publication bytes, storage reservations and maintenance queue counters
remain in their canonical `NodeRuntimeMetrics` fields rather than being copied
into this object. Per-node metrics recording already retains the complete
heartbeat evidence; no second telemetry store is introduced.

## Resident model waits

Workers include optional `runtime_metrics.resident_wait` observations: resident
wait count, checkpoint count/in-flight count, projected reclaim bytes, target
bytes, and the current policy reason. The local policy grants no lifecycle
authority: managed model-wait generation fences and attached activity leases
still govern every park. Upgrade the gateway reader before workers; older strict
readers reject this added field. New readers accept older workers without it.

The default policy retains model waits without an age expiry while real memory
headroom is available. Its reserve is the greater of 5% of host RAM and 2 GiB
(capped at 10% on small hosts); recovery must reach 1.5 times the reserve to leave
reclaim mode. Memory reservations cover active transitions and the next FIFO
start/restore wave, rather than the configured limits of an entire queued burst.
Storage pressure suppresses checkpointing caused only by reclaim PSI. An actual
memory deficit or pending foreground memory demand still triggers reclaim.

Reclaim concurrency follows the byte deficit. In-flight checkpoints receive
provisional credits from configured guest footprints, then actual available
memory is checked again after completion. These are estimates, not per-guest RSS
measurements. Failed or busy candidates return their credits and yield to another
safe wait. On saturated storage the projected release target is reduced to 5%
of host RAM per observation cycle to avoid launching a full checkpoint storm.
No fixed sandbox concurrency cap or model-wait timeout is added.

## Cumulative CPU accounting

`host_cpu_usage_usec` counts whole-worker user, nice, system, IRQ and soft-IRQ
time since boot. Guest time is already included in user/nice and is not added
again. `host_cpu_steal_usec` records hypervisor steal separately; idle and I/O
wait are not CPU work. These counters use the same `/proc/stat` parser as the
instantaneous node utilization sample.

The load report integrates counter differences only within a fresh, unchanged
worker incarnation and discards counter resets. It preserves the older
point-sampled CPU estimate under its existing name, with separate coverage for
the cumulative measurements. Older workers omit both new counters and remain
readable by the new gateway; deploy gateway readers before new worker writers.
