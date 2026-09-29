# Build reliability release — 2026-09-29

Service commits `2452ac7` and `da415f8` fix false build-not-found responses,
bound server execution and filesystem cleanup, and retain distinct shared-cache
contexts before duplicate exports. SDK [0.4.34](https://github.com/rlrs/ucloud-sandboxes-sdk/releases/tag/v0.4.34)
was published from `162b2f0fe66e762707896ab38fb3134e4a68edde`.
The [behavior contract](../../build-deadlines.md) distinguishes client waiting,
server execution, cancellation and admission capacity.

## Results

| Verification | Result |
| --- | --- |
| Real BuildKit `RUN` sleeping for 60 seconds, with a five-second execution budget | Failed and released its slot in **5.108 seconds**; observed RUN process disappeared |
| Following normal build on the same shared daemon | Succeeded in **0.593 seconds** |
| Four concurrent 16 MiB SDK context preparations | **1.306–1.318 s → 0.678–0.684 s** |
| Maximum gap in a 5 ms async heartbeat during packaging | **1.297–1.309 s → 5.88–8.81 ms** |
| Cache regression: 20 distinct contexts plus 60 newer exports of one hot context, 21-tag budget | All **21 distinct contexts** retained, respecting the byte budget |

The [BuildKit receipt](buildkit-deadline-canary.json) records a unique marked
RUN, persisted terminal failure, zero occupied slots, disappearance of its
process, subsequent success and removal of the canary's two published manifests.
It used the installed `images.py` and `build_deadline.py` whose hashes are
unchanged in the final follow-up. The following RUN can hit the warmed cache;
this verifies a new build/push and slot recovery, not a second cold compile.
This canary does not exercise EROFS publication or establish daemon-wide leak freedom.

The [SDK ABBA receipt](sdk-packaging-abba.json) uses the same incompressible
fixture across four fresh processes with mocked HTTP. CPU time remains roughly
1.31–1.33 seconds; compression overlaps without monopolizing the event loop.
These numbers measure context preparation, not overall image-build latency.
The [recorded driver](sdk-packaging-abba.py) contains this session's baseline
wheel and candidate source paths, which must be adjusted for reproduction.

## Verification and deployment

- [Service suites](service-tests.log): 259 discovered, 257 passed and two sibling
  SDK contract cases initially skipped. Both [contract cases](sdk-contract-tests.log)
  then passed against the actual candidate checkout.
- Final [filesystem/publication suite](privileged-tests.log): 36 passed, including
  deadline contention, bounded cleanup, failed bind/remount rollback, rejected
  reuse of an incomplete mount and later recovery. Earlier overlapping publication
  and cache suites also passed; their counts are not added to these totals.
- [SDK standard](sdk-tests.log) and [Inspect-enabled](sdk-inspect-tests.log):
  187 passed each. [Release-commit CI](https://github.com/rlrs/ucloud-sandboxes-sdk/actions/runs/36591682654)
  passed on Python 3.10 and 3.13, including lint and installed-wheel smoke.
  Published wheel and source archive were downloaded and matched the tested
  artifacts byte for byte. A fresh dependency-free wheel installation also passed.
- Changed service Python files pass Ruff. The repository-wide invocation still
  reports [11 pre-existing lint findings](repository-lint-existing-errors.log)
  in four untouched files; these files match the pre-change `33e7e04` baseline.
  This is not a claim that the entire service CI suite passed.

Final deployment uses `/work/ucloud-sandboxes/build-reliability-20260929-r3`,
the same tested final wheel, unchanged dependencies/native bundle contents and
idle-fleet checks. Production retains **512 cache tags within 32 GiB**, and new
builders receive the configurable **1800-second execution budget**. See the
[deployment receipt](deployment-receipt.json), [staging receipt](staging-receipt.json)
and [final audit](final-audit.json).

One follow-up rollout inherited the test shell's restrictive `umask 077`, so
new package files were unreadable by the `ucloud` service user. Its health gate
triggered [automatic rollback](automatic-rollback-receipt.json), which restored
the first release and verified health. The [final controller](deployment-controller.py)
sets `umask 022` for installation and checks isolated package imports as the
service user before starting services. No active training work was present.

The [owned builder reservation was released](pool-release.json); the final audit
checks zero fleet nodes, prepared builders, active builds and sandboxes. It also
checks gateway/relay health, installed wheel contents, SDK sync/async HTTPS reads
and the five masked unattended-upgrade units.

## Remaining qualification

The pool still has **16 build slots**. This release does not establish that a
burst of hundreds of cold builds meets a particular training deadline alongside
512 running agents. The next mixed qualification must include cold dependencies,
cache churn and local submission waiting, reporting maximum latency and every
deadline miss. Client timeout does not cancel accepted work; server deadlines
bound its execution independently. HTTP and filesystem checks are cooperative
boundaries, not preemption of arbitrary blocked kernel calls.

## Artifact identities

```text
ac538d82a2536c29854e32fb8ee897103a03b4edb5c60892c5c3090bf77dc778  service wheel
520b15d66c828193179e86a1521d2871c1e08bfef2227d0bc7a1036445fd8957  SDK 0.4.34 wheel
c42f6bf3087b91dfe71e68d96daa208530b43979a2a0c15b4bf1e39f55282eb4  SDK 0.4.34 source archive
```
