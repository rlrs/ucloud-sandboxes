# Local verification

These results were reported by the agents that ran the commands against the
candidate. They are separate suites, not repeated whole-repository runs. Runtime
wheel SHA-256 is
`21c77e27c4a064c6a52e93d8af870611d7f56f0559adb609172d9558b5366054`.
The source receipt binds all 163 packaged runtime files.

| Scope | Result |
| --- | --- |
| Cache selection, mount runtime, maintenance and images | 73 passed in 3.431 s |
| Affinity input identity, concurrency, queueing, reset and failure cleanup | 6 passed in 0.264 s |
| Bootstrap, bundle repacking, real child isolation and build history | 26 passed in 3.236 s |
| Retained BuildKit progress parser | 6 passed in 0.002 s; no skips |
| Parser plus controlled cache-proof helper | 9 passed in 0.003 s; includes the same six parser tests |
| Parser, controlled proof and concurrency diagnostic helpers | 12 passed in 0.004 s; includes the preceding nine tests |
| Isolation helper's actual observation/context functions | The same three concurrency utility tests passed in 0.001 s; overlapping coverage |
| Final audit synthetic checks | Passed; no network or production calls |
| Selection-report synthetic checks and 48 historical logs | Passed; no network or production calls |
| Host-attribution correction self-test | Passed; other process/host values preserved, repeated application idempotent |
| Runtime/cache/test lint and diff whitespace checks | Passed |

Exact runtime commands:

```sh
.venv/bin/python -m unittest tests.test_build_cache tests.test_build_cache_runtime tests.test_build_cache_maintenance tests.test_images
.venv/bin/python -m unittest tests.test_build_cache_affinity_runtime
.venv/bin/python -m unittest tests.test_bootstrap tests.test_repack_node_bundle tests.test_environment_prepare_isolation tests.test_build_history
```

The last command initially encountered eight restricted-environment socket
permission errors. Its full rerun with local HTTP/socket access passed all 26;
the initial errors were environmental, not silently skipped tests. No live
production workload ran as part of these commands.

Exact parser/helper commands, reported by their owner:

```sh
.venv/bin/python -m unittest tests.test_analyze_buildkit_progress -q
.venv/bin/python -m unittest tests.test_analyze_buildkit_progress tests.test_qualify_build_cache_affinity -q
.venv/bin/python -m unittest tests.test_analyze_buildkit_progress tests.test_qualify_build_cache_affinity tests.test_qualify_buildkit_cache_concurrency -q
```

The combined twelve-test result supersedes the preceding parser/helper runs for
aggregate counting. It checks synthetic marker parsing, source/ARG identity
differences, tar proof-file verification, mixed materialization/execution,
missing or ambiguous application evidence, and frozen-context checksum parity.
It does not run real BuildKit or establish live resource cleanup.

The same three utility tests were also run against the isolation helper's actual
function implementations, without changing the tests or performing Docker or
network operations:

```sh
.venv/bin/python - <<'PY'
import unittest
from scripts import qualify_buildkit_cache_isolation as candidate
from tests import test_qualify_buildkit_cache_concurrency as checks
checks.application_observation = candidate.application_observation
checks.context_identity = candidate.context_identity
result = unittest.TextTestRunner(verbosity=1).run(
    unittest.defaultTestLoader.loadTestsFromModule(checks))
raise SystemExit(not result.wasSuccessful())
PY
```

Both diagnostic helpers passed Ruff, Python compilation and whitespace checks,
and received independent source reviews before their frozen versions ran. R1
helper SHA-256 is
`9311328c077554ca455f090aee9ab571bf913e2f7b939cfdc67d4e9e1cf54d02`;
R2 is
`cd046d289ad849381db870b4b3deb8571e7e21de95239cc88fac26ca18e826ae`.
Their [live receipts](cache-concurrency-diagnostic-r1.json) and
[R2 receipt](cache-isolation-diagnostic-r2.json) provide the separate BuildKit
outcomes and owned-driver cleanup. The [final audit](final-state.json) passed at
13:58:21 UTC and supplies fleet cleanup, history, health and idle-state evidence.

The audit check was run locally:

```sh
python3 docs/benchmarks/build-cache-affinity-2026-09-29/final-audit.py --self-test
```

It covers both 48-record phases, absent seed smoke input, repeat image/recipe
binding, duplicate identities, changed context bytes, reused builder pools,
exact read-only history lookup, missing-file behavior, held resources/samplers
and installed-wheel mismatch. Syntax was parsed independently without executing
the live audit. These are audit-contract tests, not evidence that production
cleanup or cache correctness has completed.

The selection helper's network-free self-test also passed:

```sh
python3 docs/benchmarks/build-cache-affinity-2026-09-29/selection-report.py --self-test
```

Replay against the 48 retained historical `exec-repeat` logs found one immutable
cache export digest and owned legacy import tags per case. All 48 lacked the new
affinity selection observation, as expected. No raw log content was serialized.

```sh
python3 docs/benchmarks/build-cache-affinity-2026-09-29/host-attribution.py --self-test
```

The correction marks the two SSH-launched gateway SDK process groups unavailable
and leaves sampled host/API/registry values and raw telemetry intact. It is a
measurement-provenance correction, not newly collected telemetry.
