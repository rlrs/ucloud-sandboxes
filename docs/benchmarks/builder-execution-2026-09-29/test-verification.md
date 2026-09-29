# Selective child preparation: test verification

These recorded commands overlap and their counts must not be summed. They are
local tests, separate from the Linux microbenchmark, real EROFS equivalence,
representative production load and application-smoke gates.

The final broad regression completed **93 tests in 0.520 seconds**, with two
existing platform skips:

```sh
.venv/bin/python -m unittest tests.test_environment_prepare \
  tests.test_environment_layers tests.test_environment_builder \
  tests.test_selective_environment_publication \
  tests.test_environment_artifact.EnvironmentDocumentLoadTests \
  tests.test_qualify_selective_environment tests.test_oci_layer_materialize
```

The skipped tests were
`SquashSemanticsTests.test_squash_matches_the_separate_layers` (stand-in overlay
encoding uses BSD file flags) and
`LinuxSquashTests.test_squash_matches_the_separate_layers_with_overlay_encoding`
(overlay whiteouts need Linux root). The separate Linux root microbenchmark
exercised its privileged metadata/whiteout probes without capability skips.

The initial regression pass completed **52 tests in 1.213 seconds**, with two
existing platform skips, before four additional integration cases were added:

```sh
.venv/bin/python -m unittest tests.test_environment_layers \
  tests.test_selective_environment_publication tests.test_environment_builder \
  tests.test_build_cache_runtime tests.test_build_history -q
```

The initial protocol pass completed **7 tests in 0.015 seconds**, without skips:

```sh
.venv/bin/python -m unittest tests.test_environment_prepare -q
```

The initial real-child pass completed **4 tests in 1.025 seconds**, without
skips. Later shadow-module and real-timeout cases extend this suite; this early
count does not claim coverage of those later tests:

```sh
.venv/bin/python -m unittest tests.test_environment_prepare_isolation -q
```

The final independent isolation/bootstrap pass superseded that four-test scope:
**10 tests in 2.453 seconds, without skips**, on frozen runtime fingerprints
`environment_prepare.py` `a04d9ca8…` and `environment_builder.py` `da4348d1…`:

```sh
.venv/bin/python -m unittest tests.test_environment_prepare_isolation \
  tests.test_environment_bootstrap -q
```

It includes an actual child blocked on HTTP that times out and is reaped, an
actual shadow-module working directory that cannot replace the pinned runtime,
two output groups with metadata equivalence, unsupported-layer handling and
unchanged default bootstrap behavior. These are real child-process checks,
separate from the synthetic execution-model benchmark.

History and qualifier regression completed **11 tests in 0.802 seconds**, without
skips:

```sh
.venv/bin/python -m unittest tests.test_build_history \
  tests.test_qualify_selective_environment -q
```

The combined final-audit self-test passed duplicate and cross-phase identity
rejection, missing/mismatched exact history, failed/deleted and relabelled/reused
smoke-image checks, reservation/node/sampler cleanup gates and installed-file
mismatch checks. Its direct read-only heartbeat query leaves existing database
bytes/mode unchanged and does not create a missing state file. The
execution analyzer self-test passed numeric subprocess coverage, missing-value
handling, per-record residual calculation and negative-residual reporting:

```sh
python3 docs/benchmarks/builder-execution-2026-09-29/final-audit.py --self-test
python3 docs/benchmarks/builder-execution-2026-09-29/analyze-execution.py --self-test
```

Both self-tests use synthetic local data and perform no network or production
calls. Ruff passed both scripts. No aggregate test total is implied here.
