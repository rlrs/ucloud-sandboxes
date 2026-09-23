# Autoscaler memory forecast qualification — 23 September 2026

Deployed `0.5.114rc22`: exactly three product files differ from the qualified
`rc20` package (`policy.py`, `cli.py`, `consolidation.py`). Deferred density and
unlink changes are excluded. Production permits 0–8 workers, retains the 80%
soft memory target and two creates per cycle, and keeps parking and healthy wake
consolidation enabled. No new admission ceiling or SDK requirement was added.

The full Linux/PostgreSQL rerun passed all 1,748 tests (12 environment skips),
with source hashes verified before and after. Initial failures were legacy
expectations for max-single pending RAM and one-worker pressure waves; only the
tests were corrected before the successful rerun. See `qualification.json` and
`full-linux.log`.

The eight-sandbox production smoke booted one fresh worker from zero, completed
16 cycles with no execution/integrity/cleanup errors, and required no further
workers. Eight measured forced parks completed. The original latency gate still
failed: p95 guest continuation 0.968 seconds and useful execution 1.710 seconds.
This is functional qualification of the scaling release, not a high-density or
subsecond-at-load result.

`rc20-pressure-replay.json` contains counterfactual decisions from the previous
four-worker run with the ceiling raised to eight. It does not simulate feedback
from newly created workers or reconstruct unavailable pending-demand history.
Peak observed resident forecast was 438,653 MiB, about five actual workers. The
first sustained pressure sample requests two workers rather than one. This
replay is evidence of changed policy behavior, not an observed latency gain.
