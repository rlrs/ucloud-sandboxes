# UCloud platform incidents during sandbox density testing

Prepared for the user to report to UCloud sysadmins. No report has been sent.
The user identified these unexpected VM poweroffs as a known UCloud platform
bug on September 8. Further platform root-cause investigation is out of scope
for this sandbox performance task.

Project: **DFM Pretraining** (`4827bd3a-4e74-4393-9b82-49f71636c141`).
VM job: **12383398**, `density-review-20260907`, last reported host
**bit-c26a-03**. Shape: **32 vCPU / 96 GiB configured RAM / 2,000-GB disk**,
Ubuntu 26.04, guest kernel `7.0.0-30-generic`.

All incident times below are UTC. The brief initial provisioning transition
at September 7 21:39:52 is excluded from the incident count.

| Incident | Provider reports powered off | Provider reports RUNNING | Affected workload / result |
| --- | --- | --- | --- |
| UC-DENSITY-001 | 2026-09-07 22:23:57.925 | 2026-09-07 22:24:32.465 | 128-sandbox v4 hot wake; benchmark interrupted |
| UC-DENSITY-002 | 2026-09-08 06:12:58.143 | 2026-09-08 06:13:41.261 | Mountfix c16 second wake; 56/128 completed, 72 timed out |
| UC-DENSITY-003 | 2026-09-08 06:47:28.807 | 2026-09-08 06:48:09.103 | Mountfix v9 c24 third wake; 95/128 completed, 33 timed out |
| UC-DENSITY-004 | 2026-09-08 06:59:42.364 | 2026-09-08 07:00:26.134 | Mountfix v10 c24 third wake; 104/128 completed, 24 timed out |

## Evidence and impact

[Provider event export](ucloud-platform-incident-provider-events-2026-09-08.json)
contains the original state/status/timestamp fields with UTC conversions.
It identifies the unexpected poweroffs separately from initial provisioning.
Guest evidence does not independently identify the underlying host failure.

- **001:** [Incident report](sandbox-density-reboot-2026-09-07.md),
  [structured evidence](sandbox-density-reboot-2026-09-07.json),
  [benchmark](../benchmarks/sandbox-density-128-c16-hot-memory-reboot-2026-09-07.json).
  The previous-boot journal showed no recorded OOM, panic, watchdog or shutdown
  cause. The temporary host sampler was lost on reboot.
- **002:** [Incident evidence](sandbox-density-mountfix-reboot-2026-09-08.json),
  [benchmark](../benchmarks/sandbox-density-128-mountfix-c16-2026-09-08.json),
  [host samples](../benchmarks/sandbox-density-128-mountfix-c16-host-2026-09-08.jsonl.gz),
  [kernel journal](../benchmarks/sandbox-density-128-mountfix-c16-kernel-2026-09-08.jsonl.gz),
  [service journal](../benchmarks/sandbox-density-128-mountfix-c16-services-2026-09-08.jsonl.gz).
  The last valid sample was 06:12:53.447, followed by 1,124 zero-filled bytes.
  The captured guest OOM-kill count stayed zero and swap-out did not rise.
  A test-only `/tmp` entrypoint disappeared, delaying service recovery; it is
  now stored persistently. [Separate cleanup](sandbox-density-mountfix-reboot-cleanup-2026-09-08.json)
  verified zero remaining sandboxes, active disks, storage errors or quota.
- **003:** [Benchmark](../benchmarks/sandbox-density-128-mountfix-v9-c24-2026-09-08.json),
  [host samples](../benchmarks/sandbox-density-128-mountfix-v9-c24-host-2026-09-08.jsonl.gz),
  [kernel journal](../benchmarks/sandbox-density-128-mountfix-v9-c24-kernel-2026-09-08.jsonl.gz),
  [service journal](../benchmarks/sandbox-density-128-mountfix-v9-c24-services-2026-09-08.jsonl.gz).
  Services recovered after reboot and the benchmark's own cleanup completed
  without errors, verifying zero remaining owned sandboxes and active disks.

- **004:** [Benchmark](../benchmarks/sandbox-density-128-mountfix-v10-c24-2026-09-08.json),
  [host samples](../benchmarks/sandbox-density-128-mountfix-v10-c24-host-2026-09-08.jsonl.gz),
  [service journal](../benchmarks/sandbox-density-128-mountfix-v10-c24-services-2026-09-08.jsonl.gz).
  Both services recovered automatically. The benchmark finished with no cleanup
  errors and no remaining owned sandboxes. The node was healthy when checked
  again at 07:26 UTC.

Boot transitions recorded for 002 through 004:
`ed483050-13f2-405c-a1a1-84edb1f6bf21` →
`8f66facc-8d4e-493e-9b10-476ba6189ba6` →
`e50346d4-2bb5-4e60-8fcc-b322bfe6c64d` →
`d271a076-0181-4fba-bb8e-32cf34193f97`.

Interrupted results remain marked failed. Successful calls within an interrupted
wake wave are censored samples and are not presented as a successful capacity
or latency qualification. No production sandbox workload was used for these
benchmarks.


Follow-up: the v10 16-worker run from **07:27:44 to 07:30:43 UTC** completed
all 128 sandboxes through three cycles without a restart or operation errors.
[Its result](../benchmarks/sandbox-density-128-mountfix-v10-c16-2026-09-08.json)
still fails latency acceptance. Cleanup completed successfully; the dev node
and both services remained up. This successful run does not remove or close
the four platform incident records above.
