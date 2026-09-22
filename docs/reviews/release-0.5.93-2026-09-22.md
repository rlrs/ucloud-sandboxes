# Gateway acceptance under mixed load

The 0.5.92 run completed 2,048 correct cycles but failed latency: wake p95
4.768 s and first usable tool p95 11.958 s. This is worse than 0.5.91's full
run despite a better isolated fleet-scan benchmark. It is not a successful
optimization qualification. The cold fleets and resource histories differ, and
phase instrumentation was enabled after the run was already above target.

During the run, `ss` showed 199 established connections waiting at the gateway's
listening socket. The gateway used about 1.2 CPU cores with substantial context
switching. Phase timing showed a fast guest/tool path but gateway lifecycle
commits and request acceptance added delay. Deferred park traffic was substantial
(468 worker park calls versus 84 wakes in the 20-second sample).

Release 0.5.93 reuses daemon HTTP request workers across connections instead of
starting a new thread in the accept loop for every connection. Existing admission
slots bound both queued and running requests. Fully consumed framed requests
close on their worker outside the drain-reactor lock; rejected/partial uploads
retain half-close and bounded draining. The accept loop skips polling an empty
drain selector. Idle workers exit on server close; handler failures and failed
thread startup do not leak request slots or strand queued connections.

Linux HTTP/gateway/streaming-upload/direct-node tests: 160 passed. After the final
close-lock adjustment, all 18 HTTP/streaming tests passed again. The isolated
4-vCPU gateway HTTP benchmark improved from 953 to 1,847 requests/s and p95 from
41 to 22 ms. Adding concurrent routing DB reads removed that improvement
(340 versus 336 requests/s, p95 109 versus 123 ms). Both results are retained;
full load remains necessary to determine whether this removes the observed
accept queue and what bottleneck remains. No subsecond claim is made.

Full 256 × 8 mixed load completed 2026-09-22 23:49 UTC: all 2,048 cycles
correct, but 1,792 measured cycles had wake p95 12.53 s and first usable tool
p95 16.81 s. This release does not meet the target. It was profiled after the
latency failure was already apparent, so it is diagnostic rather than a clean
A/B comparison. TCP accept queue was 1, versus 199 sampled on .92, but the
routing writer queue held roughly 70–76 operations. During a 20-second sample,
100 worker wakes took median 39 ms / p95 130 ms; gateway wake handlers took
median 4.15 s / p95 5.58 s. Each program/route write waited about 1–2 seconds.
548 park preparation calls occurred against 100 wakes. The live fleet is idle
and the harness cleaned its owned sandboxes and relay requests.
