# Use an asynchronous transport for buffered worker RPCs

The 0.5.111 cold 256-agent run completed all 2,048 cycles correctly. Measured
wake p95 was 295 ms and the uploaded-tool path 905 ms, but the provisioning
phase still had tool p95 1.386 s. Forced park/restore at 64 agents completed all
256 cycles correctly, with wake p95 774 ms and tool p95 1.300 s. These results
do not satisfy the complete acceptance gate.

A 256-agent barrier experiment compared one-period CPU burst credit against
zero credit, balanced within each worker. All 2,048 cycles completed correctly.
Bursting shortened memory verification but did not resolve the latency tail;
no CPU policy change is shipped. The diagnostic gateway sample had roughly 35%
CPU idle and low disk activity. Synchronized request traffic still delayed the
HTTP path. That experiment also included a brief diagnostic profile, so it is
not clean qualification. Reports and assignments are retained.

This release retains the synchronous gateway handlers and their scheduling,
generation checks, durable writes and response semantics. Bounded worker RPCs
use one aiohttp I/O loop instead of each handler running the blocking urllib3
HTTP parser and socket machinery. Event polls have their own connection pool.
Uploads and streaming downloads retain their existing streaming transports.

The asynchronous transport does not follow redirects, decode compressed bytes,
or replay requests. A public aiohttp tracing hook prevents a reconnect retry
from sending request headers twice, including PUT and DELETE. Connection-pool
queue timeouts retain the existing proven-before-dispatch retry fence; read
and transport failures remain ambiguous. Responses are buffered only up to the
existing gateway limit plus one sentinel byte. Shutdown cancels waiting callers.

Linux validation: 98 HTTP/control-plane tests passed, followed by five transport
tests including the added queue-timeout distinction. The same transport tests
are run against the packaged wheel on production's Python 3.14 interpreter.
The four-core Linux proxy component comparison reduced gateway CPU about 12–17%
with the final safety checks; tail latency was not consistently better. A full
live test is required before claiming a production latency improvement.

Native storage, worker resource limits, VM shape and SDK are unchanged.

## Deployed qualification

Server 0.5.112 (558c015) was deployed at 05:44:55 UTC on September 23. Production
retains four gateway vCPUs and PostgreSQL. Fresh workers report 0.5.112. No native
storage binaries, worker CPU policy, or interpreter tuning changed.

| Scenario | Correct cycles | Wake p95 | Response-ready through uploaded tool p95 |
| --- | ---: | ---: | ---: |
| 256 rolling agents | 2,048 / 2,048 | 291 ms | 866 ms |
| 64 forced park/restore agents | 256 / 256 | 736 ms | 1,265 ms |

Both runs had zero workload, health, and cleanup errors. The 256-agent run's
provisioning phase had tool p95 1.189 s and post-provisioning p95 0.848 s.
The complete subsecond gate therefore still fails. These are individual runs,
not a controlled estimate of the transport's causal speedup.

A separate four-core proxy component experiment tested process isolation and a
fully asynchronous Python frontend. The latter used about one-third of the CPU
and halved p95 compared with the blocking baseline. A bridge retaining synchronous
handlers did not reproduce that advantage; it added thread/event-loop crossings
and was slower than the deployed transport in that comparison. Moving just the
worker client to a separate process reduced component latency but raised total
CPU use. None of those experimental frontends or processes is deployed. These
components omit production routing, scheduling, authentication and persistence;
they identify options to investigate, not a production performance guarantee.

SDK 0.4.25 is published at
https://github.com/rlrs/ucloud-sandboxes-sdk/releases/tag/v0.4.25 and fast-forwarded
to SDK main (1adf5d5). Its runtime files are byte-identical to the wheel used in
these live tests. Linux release CI passed on Python 3.10 and 3.13, including
Inspect-enabled tests and a minimal wheel installation. The published wheel SHA256
is b277bcb77c9725e73e202901bb31a9b5127ccd2fdba6eb5302b783ea8fdfefc5.

## 512-agent limit and closeout

The subsequent 512-agent rolling run (`relay-load-079fbea702d8`) completed all
4,096 cycles correctly, with no workload, health, or cleanup errors. It failed
the latency target: response-ready-to-wake p95 was 3.132 s and the uploaded-tool
path p95 was 9.525 s. This release is not qualified for subsecond performance at
512 agents. The user requested finishing this release rather than extending the
optimization effort.

A lightweight eight-second OS sample during that run showed roughly 92% gateway
CPU busy, with the main HTTP process consuming one core. The service has no
systemd CPU quota; the VM has four vCPUs. Memory pressure was zero, disk writes
were about 4.9 MiB/s and I/O wait 0.4%. This supports a CPU/concurrency bottleneck
at this load, but does not isolate the GIL from other serialized work. No sampling
profiler, packaging, or component benchmark ran concurrently with this live test.

Server CI initially stopped on semicolon/one-line formatting in three test files.
Those test-only changes were reformatted and verified to have identical Python
ASTs. Repository lint passes. The deployed runtime remains byte-for-byte the
qualified 0.5.112 artifact; the closeout changes only tests and retained evidence.
