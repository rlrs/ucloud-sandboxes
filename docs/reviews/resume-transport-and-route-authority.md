# Resume transport failures and route authority

Server 0.5.62 fixes two gateway recovery boundaries.

Exec routing reads the indexed durable route instead of trusting a process-local cache. The autoscaler writes worker-loss state in a separate process, so its route deletion could leave gateway polls targeting a dead worker even after a durable terminal loss record existed. Route reads use SQLite WAL without taking the process-wide writer lock. Missing or stale worker heartbeats defer exec traffic without making further network requests; they do not by themselves establish terminal loss.

An implicit wake precedes dispatch of the caller's exec, file or job operation. DNS, connect or read failures during that internal wake now return the existing SDK-safe `node_restore_busy` marker, retaining the underlying cause code. This certifies that the original operation has not started; it does not assert that the wake failed before resume started or that the sandbox is still parked. Transport failures after the original command is dispatched keep their ambiguous outcome and are not promoted to safe mutation retries.

Regression tests exercise route removal by a separate process, missing-heartbeat behavior, and DNS/timeout/transport failures before versus after command dispatch. Existing SDK 0.4.23 understands the retry marker. Worker loss remains terminal for live processes; these changes do not recreate or replay a lost command.

Validation: 949 server tests (6 skips), 118 SDK tests, Go tests, lint and installed-wheel checks passed. The release also passed 373 Linux tests (1 skip), both bundle boot validators, and [CI run 35506534945](https://github.com/rlrs/ucloud-sandboxes/actions/runs/35506534945). Synchronous and asynchronous SDK checks recovered from an injected DNS failure followed by a wake timeout, then dispatched the original command exactly once.

Production verification on gateway and worker 0.5.62 with SDK 0.4.23 passed all 64 sandbox creates, 32 concurrent large uploads plus 320 small writes, tool calls, park/resume and restored checksums. Small-write p95 was 1.917 seconds; resume-and-tool p95 was 12.251 seconds. Health probes and cleanup passed. A known lost exec session returned the terminal HTTP 410 contract. This is a 64-sandbox canary, not a 256- or 512-agent capacity certification.
