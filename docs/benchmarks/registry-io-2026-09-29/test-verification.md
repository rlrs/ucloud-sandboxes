# Registry I/O optimization verification — 2026-09-29

This record covers the registry-I/O candidate deployed at 10:34:03 UTC. Its
exact wheel and source-bundle hashes are recorded in
[the deployment receipt](deployment-receipt.json)
and [staging receipt](staging-receipt.json).
It does not retroactively claim a full repository test run.

The implementation task reported these focused local suites:

| Scope | Reported result | Executed by |
| --- | --- | --- |
| Registry blob-mount and registry-client contracts | 21 tests; suite passed | Main task |
| Build-cache selection/mounting, build runtime, images, and managed registry | 86 tests; suite passed | Build-cache implementation agent |
| Environment artifacts, layer publication, and related EROFS behavior | 70 tests; suite passed with 2 skips | Environment implementation agent |

These suite counts are separate execution reports. Their scopes may overlap;
they are not summed into a purported unique test total. Skipped tests are not
treated as exercised runtime behavior. No additional suite was run merely to
produce this documentation.

The mount/client-contract command was run by the main task with localhost
socket access and passed all 21 tests without skips:

```sh
.venv/bin/python -m unittest tests.test_registry_blob_mount tests.test_registry_client_contract
```

The cache/runtime/managed-registry command passed all 86 tests without skips:

```sh
.venv/bin/python -m unittest tests.test_build_cache tests.test_build_cache_runtime \
  tests.test_build_cache_maintenance tests.test_images tests.test_managed_registry -q
```

The separately reported 36-test cache/runtime run is included in this command;
it is not an additional unique-test count. Terminal-history completeness is
checked independently by the live exact-UUID audit, not claimed from this suite.

The environment suite's recorded command was:

```sh
.venv/bin/python -m unittest tests.test_environment_artifact \
  tests.test_environment_layers tests.test_environment_builder \
  tests.test_environment_rootfs tests.test_selective_environment_publication \
  tests.test_qualify_selective_environment
```

Its two skips are platform requirements in `test_environment_layers`: the BSD
stand-in overlay test requires `os.chflags`/`stat.UF_NODUMP`, and the real Linux
whiteout test requires root. They are not counted as live whiteout coverage.
The earlier deployment's real Docker/EROFS whiteout-and-opaque canary evidence
is a separate result, preserved under `build-optimization-2026-09-29`.

The live controlled registry canary passed separately, exercising mount success
and the small normal-upload fallback with readback. Its results are retained in
[mount-canary.json](mount-canary.json).
This API check is distinct from the 48-build load test and real-sandbox smoke
checks. Final load, history, cleanup, provider retirement, reservation, sampler,
and health gates belong in the release's `final-state.json`; do not infer their
completion from unit-test counts or deployment health alone.

The release controller was compared with the previously qualified controller:
only its release root and two temporary configuration-file suffixes changed.
Its Python syntax/help and Ruff checks passed without production calls during
preparation. It still changes only `node_package_root`, verifies pinned source
bundles/dependencies/native bytes, and captures a fresh paired configuration and
gateway-venv rollback before service changes.

The new read-only `final-audit.py` passed Ruff/help checks and local synthetic
fixtures for 48 distinct build identities; node-ID union from before/summary/
explicit inputs; exact-UUID history success, mismatch, and missing rows; and
inactive, collected, active, or unavailable sampler states. These local helper
checks made no production/provider calls and are separate from the suites in
the table.
