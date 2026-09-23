# Isolate frequent routing commits from HTTP contention

The 512-agent rolling workload completed 4,096 correct cycles on 0.5.99 but
failed latency qualification: measured wake p95 5.75s and first usable exec
p95 11.74s. During one 20-second trace, worker wake RPC p95 was 114ms while
route/wake confirmation waited 3.07s. Exec-session registration and park
projections shared the same delayed SQLite writer. A separate queue sample
processed 227 writes in roughly 4.5s, with 2.05s inside transactions and
739ms in commits. This is not a claim that SQLite fsync alone took 3 seconds.

The gateway now uses a spawned process for wake confirmation, exec registration,
and relay lifecycle projections. The existing SQL implementation still checks
owner, generation and activity epochs and uses FULL commits. Other mutations
and fresh reads stay in their existing paths. The child never automatically
replays an operation after a lost IPC acknowledgment. Such failures surface as
routing-state unavailable; wake handling retains its durable readback logic.
The process is warmed before the gateway accepts requests and closed with it.
Its private, inherited IPC carries no public commands or network listener.

The final create admission recheck reads only the chosen node's routes, including
incoming migration reservations, instead of decoding the entire fleet again.

142 Linux tests pass, including HTTP lifecycle/implicit-exec integration with
the process enabled, rollback, concurrent writes, stale generations, missing
and replaced state files, process death, and the existing routing/batch/CLI
checks. A lightweight four-core component benchmark was slower with IPC
(601 versus 871 requests/s). It does not reproduce the HTTP contention of the
real workload; this release remains a candidate until measured under that load.

Kernel diagnosis also found a separate density problem. A clean 256-agent run
on two workers had first-usable p95 30.57s. Deferring transparent-huge-page
compaction did not eliminate stalls. BPF stacks on the next diagnostic run
showed mostly XFS file-backed memory read-ahead, plus ext4 backing-file writes;
XFS log-recovery allocation was also present. THP counters stayed zero. The
experimental THP setting was restored on both workers and is not shipped.
The 512 run had sparse sampled kernel tracing plus short gateway profiling;
it is diagnostic evidence, not clean qualification.

## Clean production measurements

After deployment, the rolling 256-agent × 8-cycle run completed all 2,048
cycles with no scenario or cleanup failures. Each guest retained 512 MiB,
dirtied 128 MiB per cycle, and performed filesystem, CPU, relay, and tool work.
Measured wake p95 was 0.545s; response-ready to externally confirmed usable
tool p95 was 1.436s. Provisioning overlap remained slower (2.475s tool p95)
than steady operation (1.061s). No parks were observed in this naturally
warm-retained run, so it does not establish restore performance.

A separate 64-agent × 4-cycle forced-parking run completed all 256 cycles,
with 192/192 measured cycles actually parked, and no errors. Wake p95 was
0.979s; first usable tool p95 was 1.676s. Both runs were unprofiled. The
strict end-to-end <1s acceptance still fails. These are partial improvements,
not completed qualification. Raw reports are retained alongside this release's
diagnostic reports.
