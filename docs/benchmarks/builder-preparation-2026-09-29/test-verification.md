# Builder preparation optimization verification — 2026-09-29

This record covers the candidate wheel with SHA-256
`7b00954ae39cae9c552c238769dea193df0f5388b4ef8de6155670aeba5a947a`.
It records focused suite executions rather than claiming a full repository test
run. Counts from overlapping suites must not be summed into a unique-test total.

The main task ran image/build runtime/history coverage:

```sh
.venv/bin/python -m unittest tests.test_images tests.test_build_cache_runtime \
  tests.test_build_history -q
```

Result: **39 tests passed in 4.174 seconds, no skips**.

The extraction implementation agent ran the materializer and publication suites:

```sh
.venv/bin/python -m unittest tests.test_oci_layer_materialize \
  tests.test_selective_environment_publication tests.test_environment_layers \
  tests.test_environment_artifact -q
```

Result: **100 tests, suite passed in 2.911 seconds with two existing platform
skips**. An initial restricted run had four localhost HTTP fixture bind errors;
the complete command then passed with localhost socket access. Those environment
errors are not reported as a successful test execution. The skips require BSD
`os.chflags`/`stat.UF_NODUMP` or root-only Linux whiteout behavior; neither is
counted as exercised by the skipped local tests.

The builder implementation agent ran the focused layer/squash/publication suite:

```sh
.venv/bin/python -m unittest tests.test_environment_layers \
  tests.test_environment_builder tests.test_selective_environment_publication \
  tests.test_environment_artifact.EnvironmentDocumentLoadTests \
  tests.test_qualify_selective_environment
```

Result: **42 tests, suite passed in 0.367 seconds with the same two existing
platform skips**. This suite overlaps the 100-test command above. Separate Ruff
checks passed for the changed runtime and associated test files.
The new consume-path overlay test using Linux user-xattr/FIFO stand-ins ran and
passed; it is distinct from the skipped real-root overlay encoding test.

The [microbenchmark record](MICROBENCHMARK.md) separately documents local
single-publication and four-thread comparisons, source fingerprints, filesystem
equivalence, and capability skips. It is not a registry, Docker, EROFS byte, or
live sandbox test. The root-capable builder run must explicitly verify that all
overlay capability probes were exercised.

The new read-only `final-audit.py` passed Ruff/help checks and synthetic local
fixtures for the 48 `prep-repeat` build identities; mandatory inclusion of
profile VM `167955324`; before/summary/explicit node-ID union; exact-UUID history
success and mismatch; and inactive, collected, active, or unavailable sampler
states. These checks made no production calls. The audit requires three successful
and deleted application smoke sandboxes, all 48 durable success records, no
remaining workload/reservations, retired test provider nodes, healthy deployed
services, and stopped samplers before declaring completion. Its only mutation is
writing a new audit receipt; an existing output requires another filename.

Final production load, semantic smoke, provider retirement, reservations, and
health results belong in their retained receipts. Unit-test and microbenchmark
success alone do not establish these deployment gates.
