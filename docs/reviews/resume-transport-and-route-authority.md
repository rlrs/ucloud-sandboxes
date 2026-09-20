# Resume transport failures and route authority

Server 0.5.62 fixes two gateway recovery boundaries.

Exec routing reads the indexed durable route instead of trusting a process-local cache. The autoscaler writes worker-loss state in a separate process, so its route deletion could leave gateway polls targeting a dead worker even after a durable terminal loss record existed. Route reads use SQLite WAL without taking the process-wide writer lock. Missing or stale worker heartbeats defer exec traffic without making further network requests; they do not by themselves establish terminal loss.

An implicit wake precedes dispatch of the caller's exec, file or job operation. DNS, connect or read failures during that internal wake now return the existing SDK-safe `node_restore_busy` marker, retaining the underlying cause code. This certifies that the original operation has not started; it does not assert that the wake failed before resume started or that the sandbox is still parked. Transport failures after the original command is dispatched keep their ambiguous outcome and are not promoted to safe mutation retries.

Regression tests exercise route removal by a separate process, missing-heartbeat behavior, and DNS/timeout/transport failures before versus after command dispatch. Existing SDK 0.4.23 understands the retry marker. Worker loss remains terminal for live processes; these changes do not recreate or replay a lost command.
