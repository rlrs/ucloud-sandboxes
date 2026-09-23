# Worker transition accounting

`DirectSandboxService` owns one `TransitionLedger` under its existing capacity
lock. Startup, restore, capture and publication have explicit operation tokens;
release of one token cannot release a peer's claim. The existing heartbeat's
transient resource reservations are derived from this ledger, rather than kept
in a second ownership dictionary. Configured shapes and physical cost forecasts
remain different quantities.

Cold startup has no measured footprint yet, so it uses the requested memory as
a conservative temporary bound. Restore uses authenticated checkpoint metadata's
allocated bytes as its read forecast. For ordinary sandboxes this is also the
memory forecast; managed primary restore reserves the larger of allocation and
the requested memory bound, covering future workload growth after the copy. Thus
a crossed checkpoint that suspends an earlier continuation grant cannot make the
actual restore silently fall back to sparse-copy bytes. Missing metadata falls
back to the requested bound; the actual restore still performs its full artifact
validation. These forecasts do not measure peak runtime overhead and remain
subject to the live physical-memory and configured RAM-backing checks.

An admitted foreground transition reserves its forecast before allocating. The
next concurrent transition cannot spend the same sampled headroom; the existing
2 GiB host floor remains protected during aggregate admission. For one operation,
the existing physical shape and pressure rules remain authoritative. A release
notifies memory waiters immediately. Repeated requests for the same incarnation
share the forecast, because the lifecycle lock serializes their actual work.
CPU resampling and the FIFO queues retain their existing bounded deadlines and
physical concurrency ceilings. No estimated I/O bandwidth becomes a new limit.

Restore waits hold no sandbox lifecycle lock. After the queue, generation and
checkpoint generation are rechecked under that same lock; a duplicate wake that
has already completed is reused. Delete cancels queued tickets by incarnation;
drain cancels all pending starts/restores. Dispatch failures are never recast as
safe pre-dispatch retries. Failed, cancelled and timed-out operations release
both their FIFO slot and their exact transient memory claim. A managed launch
that may already have dispatched retains its durable growth forecast until a
matching safe wait, completed checkpoint, terminal observation or deletion; see
[managed growth admission](managed-growth-admission.md).

Capture is pressure-relieving work. Its ledger entry observes the existing native
capture path without charging already-resident guest memory again or placing it
behind the work waiting for that memory. Capture and publication buffer memory
and I/O volume are unknown in the ledger, not zero or the sandbox quota. Native
exporter buffer/device ceilings still apply. During a foreground burst, one
publication can continue, but a new publication wave does not jump ahead of
waiting wakes. Completed publications permit the next maintenance operation.

`transition_admission_snapshot()` exposes active/waiting counts and known versus
unknown byte forecasts for diagnosis. It is process-local evidence, not another
scheduler or durable ownership authority. Startup footprint calibration and
measured capture/publication byte attribution remain separate measurement work;
no new wire contract or production activation is required for the accounting.
