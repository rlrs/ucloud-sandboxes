# Sandbox CPU quota evidence, candidate 11

These are temporary read-only cgroup-v2 samples from four production workers
during the 23 September 2026 natural512 and diagnostic512 runs. They do not
change CPU limits, heartbeat schemas, or scheduling. Existing heartbeat cgroup
CPU evidence describes the node-agent process group, not these sandbox groups.

The natural run covered 12:28–12:34:20 UTC and completed all 4,096 tool actions
correctly, with zero actual parks. Its continuation/tool p95 was 3.782/9.34s.
The diagnostic run covered 12:35–12:43 UTC and overlapped a nonblocking gateway
profile. It is diagnostic evidence, not an independent performance comparison.

| Worker | Natural median guest CPU cores | Natural throttled periods | Diagnostic median guest CPU cores | Diagnostic throttled periods |
|---|---:|---:|---:|---:|
| 12400367 | 3.83 | 1.51% | 2.53 | 2.08% |
| 12400368 | 5.51 | 2.11% | 4.58 | 2.42% |
| 12400369 | 4.03 | 1.54% | 2.74 | 2.08% |
| 12400370 | 6.74 | 2.87% | 5.72 | 3.08% |

These counters do not identify sandbox CPU quota as the dominant cause of the
fleet-wide multi-second tail. The largest single sandbox interval recorded
0.989s of kernel throttled time over approximately ten seconds. Throttled time
is a cumulative cgroup counter, not an attribution of tool latency; occasional
quota effects remain possible. Do not remove quotas or activate burst policy
from this evidence alone.

`sample.py` reads cpu.stat, cpu.max and cgroup.events once per ten seconds in a
single process, without spawning a subprocess per sandbox. Device/inode identity
is checked around reads; missing fields remain unknown, counter resets discard
the interval, and new/removed identities are counted explicitly. Output retains
aggregate counters and only the eight most throttled/busy runtimes per worker.
The median scan cost was 11–24ms per ten seconds. Steady summaries require at
least 120 observed groups and no new/removed identities. Natural run logs contain
two read races per worker during creation/deletion; diagnostic logs contain none.
Neither run recorded a counter reset. Full compressed interval logs and the
machine-readable summary are retained alongside this note.

Run this collector only for a bounded diagnostic window; its default duration
is 1,800 seconds. Guest identity labels are intentionally absent from persistent
Prometheus metrics. No credentials or application contents are collected.
