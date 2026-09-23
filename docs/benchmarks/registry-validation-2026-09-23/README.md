# Repeated registration validation, 23 September 2026

The rc13 diagnostic 512-agent run showed repeated worker preparation reads consuming CPU before process launch. On worker 12400471, an isolated Python 3.14.4 process profiled the installed rc13 DirectSandboxRegistry against 128 live records of 2030 bytes each. The production database was opened with SQLite URI `mode=ro` and `query_only=ON`; no production record or installed module changed. The diagnostic run is not an unperturbed acceptance measurement.

For 512 reads, cProfile recorded 733 ms CPU: 616 ms in registration decoding, including 387 ms validating SandboxSpec and 219 ms validating setup paths. Transaction management consumed 98 ms. These are instrumented costs, useful for attribution rather than absolute production latency.

The change replaces repeated PurePosixPath construction and parent traversal with equivalent POSIX lexical component checks, a compiled ASCII-control-character expression, and reserved-tree prefixes tested at slash boundaries. It retains complete registration validation and canonical JSON comparison. It does not cache mutable registrations, change ownership authority, or resolve paths against the controller filesystem.

The subsequent component benchmark uses an isolated temporary database and a 1449-byte planned registration with the default writable-path list. It runs the installed rc13 registry and substitutes only the candidate guest-path functions inside this separate process. Two ABBA rounds each execute 4096 reads and 4096 decodes per variant after warmup. The temporary database is automatically removed. Production services and their installed code remain unchanged.

| CPU seconds per 4096 operations | Baseline median | Lexical median | Reduction |
| --- | ---: | ---: | ---: |
| Complete registry get | 1.072 | 0.607 | 43% |
| Decode only | 0.736 | 0.422 | 43% |

CPU times vary during host cooldown, so the retained individual ABBA results matter. Both rounds improve. This is a component result, not a claim that full exec latency or the fleet wake SLO improved by 43%.

The same Linux Python 3.14 process ran all three guest-path tests successfully. The equivalence test compares both policies and exact error precedence across 1672 inputs per validator, including all ASCII characters, Unicode, surrogate code points, double/triple leading slashes, repeated separators, dot segments, colon/comma delimiters, and reserved-tree prefix lookalikes. Local guest-path plus registration tests also passed (18 tests).

To repeat `benchmark.py`, stage the baseline installed package, candidate `ucloud_sandboxes/guest_paths.py` at `/tmp/candidate_guest_paths.py`, and `tests/test_guest_paths.py` at `/tmp/test_guest_paths.py`. Run it in an isolated process with the baseline package on PYTHONPATH. The script creates only its own temporary fixture. It does not need access to a production database.
