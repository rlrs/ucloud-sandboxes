# SDK 0.4.26 Linux qualification

Candidate: nested `ucloud-sandboxes-sdk` working tree, version 0.4.26. Isolated
Linux checkout `/home/alex-admin/ucloud-sdk-release-20260923`, CPython 3.13.2.
`uv sync --all-extras --locked`, `uv run --all-extras python -m unittest`, and
`uv build` passed: **133 tests**, wheel and source distribution built.

The 512-connection loopback fixture needs file descriptors for both client and
server sockets in the same process. Qualification uses `ulimit -n 8192`; an
initial run at the host's default limit exhausted descriptors and was rerun at
that explicit fixture limit. No application resource limit was changed to obtain
this test result. Real PostgreSQL SDK phase-wire tests are recorded separately
under `relay-resource-phases-2026-09-23`.

This is packaging/test evidence, not publication or production latency evidence.
