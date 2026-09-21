# Isolated density-node reboot evidence — 2026-09-07

The isolated 32-vCPU test VM, UCloud job **12383398**, powered off during the
128-sandbox hot wake attempt. The retained evidence does **not establish the
cause of the poweroff**. This report supplements the
[density performance review](sandbox-density-performance-2026-09-07.md);
selected records are preserved in the
[machine-readable evidence](sandbox-density-reboot-2026-09-07.json).

| Observation | UTC time on 2026-09-07 |
| --- | --- |
| Last retained previous-boot journal entry | 22:23:53.212433 |
| Provider update: SUSPENDED, “The virtual machine is powered off.” | 22:23:57.925 |
| Provider update: RUNNING, “Your job is now running.” | 22:24:32.465 |
| First current-boot journal entry | 22:24:38 |
| Read-only journal capture | 22:34:40.705754 |

The previous boot ID was `84fa5d94e3b040cf9a9ddc5bee6e5daf`; the replacement
boot ID was `ed48305013f2405ca1a184edb1f6bf21`. A scan of **5,041** retained
previous-boot kernel records found no OOM, panic, watchdog, I/O-error, or shutdown
cause. The only keyword match was the benign boot message registering a
`bochs-drm` panic display plane. The final journal entries contain XFS mount/log
recovery and storage-native ublk allocation activity. These observations do not
rule out an unrecorded guest failure or a host/provider event.

## Host telemetry limits

The requested `/tmp/density-host-metrics-v4.jsonl` was absent from the worker
after reboot. Neither of the two checked gateway copies existed. Consequently,
**final-minute memory availability, PSI, vmstat, disk activity and OOM counters
cannot be quantified** from this capture. No values have been inferred from
other runs.

The separately retained CPU sampler contains 588 records covering 300.002
seconds, from 22:17:27.262299 through 22:22:27.264317 UTC. It completed 600
scans with zero read errors and observed 293 sentry process identities over
the sampling interval. It ended **90.661 seconds before** the provider's
poweroff update, covering none of the final minute. This sampler measures
process CPU and contains none of the missing host counters.

## Startup failure after the reboot

Read-only inspection found the node service repeatedly exiting before binding
port 8090. Its startup reconciliation attempted to mount a storage-native
volume already in `ERROR` state and received:

```text
StorageNativeConflictError: storage-native volume is error; cannot mount
```

The preserved stack reaches `direct_warden._mount_storage`,
`storage_native_daemon.ensure_mounted`, and `_call_unobserved` in the v4
candidate. The service logged nine matching failures among the last 600
captured entries. This explains the unavailable node API after the reboot;
it does not explain the earlier VM poweroff.

The old external SSH port 2435 refused connections after the reboot. Recovery
used a temporary local SSH ProxyCommand over the production gateway's SHELL
WebSocket to the fixed target `density-review-20260907:22`. SSH verified the
existing VM host key, and the private key remained local. The recovered VM's
private address was `10.36.86.228/32`.

## Temporary recovery materials

The helper `/tmp/prime-dev-ssh-proxy.py` and its existing local authentication
and known-host files are needed until recovery work finishes. Each proxy
process lives only for its SSH connection. The final gateway scan found no
remaining oracle HTTP bridges or SSH TCP bridges owned by this investigation;
all local command sessions opened by the evidence capture completed. Other
agents' subsequent connections and service changes are outside this snapshot.

The investigation changed no guest services or sandbox state. Node recovery,
API cleanup, and subsequent validation are coordinated separately and must
be assessed using their own results.
