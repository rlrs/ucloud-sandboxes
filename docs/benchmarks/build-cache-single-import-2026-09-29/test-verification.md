# Verification

The deployment owner ran the focused cache/runtime/image suite against this
candidate before deployment:

```sh
.venv/bin/python -m unittest tests.test_build_cache tests.test_build_cache_runtime tests.test_build_cache_maintenance tests.test_build_cache_affinity_runtime tests.test_images
```

**80 tests passed in 3.725 seconds, no skips.** This includes exact-affinity
selection, the unchanged eight-import fallback, legacy cache tags, pruning,
optional mount failures and image/runtime behavior. Ruff passed for the two
edited files, and `git diff --check` passed. The narrow runtime selection change
received an independent read-only review with no blocking finding. These are
reported execution receipts, not another rerun or an aggregate of overlapping
earlier suites.

[Local scaffolding checks](scaffolding-verification.json) record network-free
helper self-tests, context/launch validation, read-only heartbeat inspection,
syntax checks and controller-root comparison. A separate read-only reviewer
checked the frozen runner, final audit and qualification controller against the
actual deployment and initial-builder receipt schemas, with no blocking finding.
The reviewed helpers enforce exact build/context identity, distinct recipe-bound
smokes, installed wheel/bundles and explicit owned-resource cleanup.

| Helper | SHA-256 |
| --- | --- |
| Deployment controller | `fdbc0b7d2073a9c6a3a517c646d10a61e4efcfb43818c5eff239e0d4094ec037` |
| Qualification controller | `4416e944526f46c829555815e9c66bae95e008e4f53f03d9c20423f4fe705d34` |
| Run helper | `f716be10b67b98bfd1b6ca0d4b96fca06e19ed94d833b475ad3a394ad1ecbfe0` |
| Final audit | `b2b7c3ac9dde9c289c7512b325e899c94bc4e2b985a97c4b02e7f416618cb8f7` |
| Selection reporter | `fc4f1b794759dd70ce64b8217db5fb0f2e0d5825202fef54773549f3582fa9ee` |

The deployment controller and qualification controller are distinct files with
different responsibilities and digests. Additional VMs or sampler units outside
the phase receipts must be passed explicitly to the final audit; they cannot be
inferred from the build history alone.

Production evidence is retained separately: [48 successful builds](single-import-repeat/summary.json),
[48 exact matches with one import each](single-import-repeat-selection.json),
[three verified/deleted application smokes](smoke-single-import-repeat.json),
and [deployment health and rollback guards](deployment-receipt.json). Frozen
fixtures and SDK 0.4.33 are verified by the [qualification receipt](single-import-repeat.qualification.json).
This suite and workload do not qualify mixed running-agent capacity.
