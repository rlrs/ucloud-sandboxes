# rc17 pressure trial evidence

Run `relay-load-d0bf2463839e` requested the same 256-primary pressure profile and
failed during provisioning: a relay claim returned HTTP 500 at 14:15:43 UTC.
No complete cycles were recorded. Fleet health also failed. The report finished
at 14:18:27 UTC; this is not a successful pressure qualification.

At 14:17:40 UTC, retained managed-process records showed 140 running primaries
and no terminal failures. Worker memory samplers continued during the failure;
the raw observations and minimum physical/backing headroom are in `summary.json`.
They showed no exhausted RAM-backing filesystem. These periodic observations
cannot establish free space at every fault or identify the cause of the HTTP 500.

All four workers recorded multi-second clocksource watchdog readout intervals
during the trial or cleanup (`kernel-evidence.txt`). These are timing-gap evidence,
not attribution to a host or guest component. The inspected journals had no OOM
kill or block-I/O-error matches.

Worker 12400606 independently reported 17 concurrent `runsc checkpoint
--hibernate` command timeouts, each at the existing 60-second deadline, at
14:17:03 UTC. Its later inventory retained 17 hibernating sandboxes while I/O full
PSI reached 80.99%. The captured command errors are in `capture-timeouts.txt`.
They were foreground relay-park requests, not a new background lifecycle path.

The active observation at 14:15:29 showed 17 checkpoint operations but only
3.33 GB of projected reclaim on that worker. Per-candidate sample timestamps were
not retained, so this cannot distinguish missing observations from small samples
taken before heap growth. Source review nevertheless reproduced a concrete bug:
unknown or stale footprints contributed one byte each to capture selection,
allowing an arbitrary number of I/O-heavy captures to fit the byte target.

The follow-up fix preserves unknown footprints as unknown. One unknown capture
is probed alone, then existing headroom settling applies; measured footprints
still run concurrently against the byte target. A footprint must also have been
sampled after that request's first safe wait, so a still-fresh startup observation
cannot price a heap allocated before the wait. Neither rule adds lifecycle
authority or labels a configured memory limit as measured reclaim.

Focused qualification passed 68 tests on Linux, including a real sampler age
fence, a fresh-but-pre-wait sample, 256 unknown candidates, 17 combined foreground
and background requests, and continued parallel selection for measured heaps.
A subsequent production pressure run remains required.
