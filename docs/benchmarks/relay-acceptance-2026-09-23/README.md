# Durable acceptance and database separation qualification

These are isolated Linux component tests, not production sandbox restore
measurements. Two relay processes and an isolated PostgreSQL 17 container ran on
`rasmus-dev`; wake is a **600 ms timer**, and bodies are 32 KiB with per-request
checksums. Python 3.10.13 and the repository's locked dependencies were used.
The JSON files retain source hashes and per-operation database timings.

| Run | Accepted / verified caller bodies | Acceptance p95 | Actual caller HTTP response p95 |
| --- | --- | --- | --- |
| [64, kill one relay](http64-kill.json) | 64 / 64 | 61.5 ms | 1.870 s |
| [512, concurrent burst](http512.json) | 512 / 512 | 442.7 ms | 1.264 s |

The killed-relay run recovered 32 caller connections and retained every accepted
result. No response submission needed retry in that run: all acceptance receipts
arrived before the kill. The separate regression below covers the exact lost-ACK
window. The 512 run had zero caller/submission retries. These runs establish the
acceptance/delivery distinction and recovery behavior; they do not establish a
subsecond production restore SLO or prove a performance improvement over a
matched old-server baseline.

## Regression and migration checks

- Python 3.10.13: 62 real PostgreSQL transaction/relay tests passed in 9.864 s.
- Python 3.13.2: 104 PostgreSQL, relay and stateful tests passed in 11.917 s
  (one optional stateful test skipped).
- Explicit socket tests cover acceptance while wake is blocked, identical and
  conflicting duplicate submissions, terminal inference errors, independent
  sibling delivery obligations, lost TCP ACK after commit, and relay restart
  with expiry of the dead dispatcher's claim.
- Existing tests cover storage admission/pinned results, notification loss,
  commit rollback, old-registration and lease fencing, delayed park/wake races,
  caller loss and authenticated result replay.
- Fresh relay migration creates only `relay_*` tables. Read-only `status` starts
  no dispatcher and writes no runtime configuration. Existing unrelated
  qualification tables and their schema version are preserved. Qualification
  migration separately creates its own tables and no relay tables.
- A two-phase database crash test prepared both domains explicitly, killed the
  **owned test PostgreSQL container** with SIGKILL, restarted it, and verified
  retained responses, claims, replay and reservation release. Docker reassigned
  its ephemeral host port on restart; verification succeeded after using the
  new port. The recovered test schema was removed.
- Ruff checks passed for the changed relay/database/qualification tests and
  component scripts.

## CI hang diagnosis

The former `shared-control-postgres` lane set up Python 3.13 but `uv sync` selected
`.python-version` (3.10.13). Run 35825536652 consequently hung for its ten-minute
job budget. The same hang reproduced on Linux 3.10.13, while 3.13 passed.

A cancellation racing a ready timed wait could be consumed in Python 3.10,
including a dependency's pool wait. Canceling the dispatcher at an arbitrary
transaction boundary left it running and the test awaiting shutdown forever.
The relay now uses cancellation-preserving bounded waits, records explicit
shutdown, and repeats cancellation for tasks whose dependency consumed the first
signal. The lifecycle-ordering test gates claim admission instead of canceling
arbitrary database work; dedicated regressions cover cancellation and dependency
suppression. The CI lane explicitly tests **both 3.10.13 and 3.13**, sets
`UV_PYTHON`, and prints individual test names. The job timeout is unchanged.

This evidence is from the isolated Linux environment; the updated GitHub Actions
workflow still needs to run after the change is pushed.
