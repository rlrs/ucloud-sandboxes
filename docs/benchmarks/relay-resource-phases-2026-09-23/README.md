# Advisory phase qualification

The phase implementation was tested on Linux/Python 3.13.2 against the existing
isolated PostgreSQL container on `rasmus-dev`. The source mirror is
`/home/alex-admin/ucloud-relay-phase-20260923`; neither production services nor
the frozen rc5 driver were changed. Database credentials were passed only in the
test subprocess environment, and every test used its own schema with cleanup.

- **48 real PostgreSQL tests passed**, including HTTP startup without constructing
  a legacy relay engine, advisory ordering/idempotence/expiry/revocation, cancelled
  transactions, reserved-metadata rejection and advice on an existing park without
  creation of new lifecycle work. Actual sync and async SDK clients both exercised
  the authenticated HTTP endpoint against PostgreSQL.
- **21 SDK relay tests passed on Linux**, including identical sync/async update
  payloads and malformed advice validation.
- **59 relay/stateful/lifecycle transport tests passed** on the same Linux mirror.
- **16 Verifiers integration tests passed** in its existing local environment,
  including normal completion, cancellation, tool failure and unavailable hint
  delivery. These are integration unit tests, not a Linux runtime performance gate.
- The separate downstream worker policy/transport implementation passed its
  **170-test Linux gate**, recorded by its owning agent.

The first attempted SDK HTTP test used an unsupported context manager on the
synchronous client; that fixture was corrected, and the complete 48-test lane
then passed. A prior broad test command named a nonexistent lifecycle module;
the corrected 59-test invocation passed. These were qualification fixture issues,
not hidden successful runs.

This checkpoint predates the subsequent full live SQLite relay removal. That
removal must rerun its complete PostgreSQL and compatibility suites against the
combined source. The phase tests are permanent in `tests/test_postgres_relay.py`
and the coordinated SDK suite; they do not claim performance or density gains.
