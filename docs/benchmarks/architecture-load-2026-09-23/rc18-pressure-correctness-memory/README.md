# rc18 pressure correctness trial

Run `relay-load-c4f46ab62793` kept the same 256 primaries, eight cycles, 1.5 GiB
resident heaps, and 384 MiB dirtied per cycle. The SDK operation budget was 1800
seconds; the guest-continuation deadline remained 180 seconds. All 256 primaries
started. The run completed 1282 cycles, then failed because sandbox 0021 did not
continue after its cycle-three response. Cleanup completed without errors.
Fleet health passed, but workload correctness and latency did **not** pass.

At 15:30:04 UTC all 256 durable managed-process records were running, with no
terminal failures. Two-second samples showed minimum physical available memory
of 6.91–11.62 GiB and minimum RAM-backing available space of 8.99–13.53 GiB across
the four workers. No sampler failed, and no checkpoint command timeout appeared
in the captured worker journals. Sampling is not proof of allocation-time values.

The runtime observed an unknown-footprint capture as a single probe with zero
projected bytes. Later measured capture bursts completed, and steady observations
usually showed one to three captures per worker. Completion counters are
cumulative across runs because this trial reused the rc18 workers; the raw
observations retain their values without attributing all prior work to this run.

Sandbox 0021 was on worker 12400623. Its workspace capture completed at
15:27:57 and the device was released at 15:28:00. A retained wake trace ran from
15:27:42 to 15:28:31 and ended HTTP 503. Its 146-byte response size matches the
existing typed, retryable parked-restore memory-admission response; this is an
inference from schema and size, not a captured response body. Subsequent relay
investigation found five retry attempts and a successful wake about 180.307
seconds after the response commit, just after the unchanged 180-second observer
deadline. This was delayed resource admission, not proof of a restored TCP fault.

An independent workload audit found that finished benchmark primaries deliberately
slept forever, retaining their heaps after all eight checks. That makes eventual
completion under overcommitted physical memory an invalid end-state: finished
primaries issue no more safe model waits. However, zero scenarios had finished
all eight cycles when 0021's wake returned 503. Only 15 had finished by the failure
(two on worker 12400623), so this does not explain that first failed admission or
the initial delayed wake. The harness follow-up now permits normal primary exit
after all final integrity proofs, then verifies authoritative exit status zero
with no signal. That status observation retires the worker's growth forecast;
global sandbox cleanup remains unchanged. Active heap size, cycle count, overlap,
and integrity checks are unchanged. All scenarios must pass this new completion
gate for workload correctness. The 29-test Linux harness gate includes actual
guest normal exit, pinned SQLite WAL recovery, and abnormal exit rejection.

Raw RAM samples, five bounded observations, and kernel timing messages are
retained here. Full journals remain in the private qualification directory
`/private/tmp/rc18-correctness-growth-evidence`. Source and production services
were not changed during collection.
