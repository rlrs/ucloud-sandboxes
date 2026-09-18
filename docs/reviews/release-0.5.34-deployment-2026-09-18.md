# Release 0.5.34 deployment verification

Server **0.5.34**, code commit `b8edcb8bd96563aeba969c9416856e8dda0c5156`,
was pushed to `codex/sandbox-linux-compatibility` and deployed to DFM Pretraining
production on September 18, 2026. Gateway installation completed at **10:50 UTC**;
live verification and cleanup completed at **10:56 UTC**.

The release includes builder load balancing, bounded cold-image preparation,
parking activity fixes, and conservative consolidation when parked sandboxes
wake. Production has wake consolidation enabled and a maximum of four builders.
See the [health investigation](production-health-2026-09-18.md) and
[capacity investigation](production-capacity-2026-09-18.md) for scope and limits.

## Reserved worker coverage

The three previously reserved sandbox workers (`12395758`, `12395759`,
`12395762`) already contained the tested parking fix despite their 0.5.33 version
labels. This was verified against the source in each running service's actual
Python path, rather than inferred from the staged bundle.

The replacement reserved workers (`12395796`, `12395797`, `12395798`) all report
0.5.34 and use the new sandbox bundle. Each running worker's `direct_service.py`
has SHA-256 `b51aca36060a5c30f3b168801b726b63b39c4617667bcc3e5cb369ba28703d92`.
Gateway routing, timeout handling, and consolidation selection run centrally;
they are active for these workers. Builder `12395803` also booted with 0.5.34.

The existing reservation for 100 sandboxes, each requesting 4 vCPU, 8192 MB RAM,
and 37952 MB disk, remained unchanged. Its existing expiry was 11:41:52 UTC.
No occupied worker was forcibly restarted or terminated for this deployment.

## Validation

- Canonical local checks passed: 869 server tests run, six skipped; 95 SDK tests
  passed; Ruff, Go tests, builds, and isolated installed-wheel checks passed.
  ShellCheck was unavailable locally and was covered by CI.
- [CI run 35336129641](https://github.com/rlrs/ucloud-sandboxes/actions/runs/35336129641)
  passed on Python 3.10 and 3.13, including Linux namespace isolation,
  Docker registry, and S3-compatible contracts.
- All 90 installed gateway package files matched the release wheel exactly.
- Public gateway and relay health returned 0.5.34. The unauthenticated sandbox
  API correctly returned 401. Gateway, relay, autoscaler, and registry were active.
- A fresh 0.5.34 builder built and pushed a managed image; a reserved worker
  created a sandbox from it. Cold preparation returned retryable 503 in 2.051 s;
  the next create attempt succeeded in 1.094 s.
- Three automatic park/wake cycles preserved the test process's in-memory UUID
  and advancing counter. Wake/exec requests took 1.454, 0.711, and 0.602 s.
- Explicit snapshot publication and detach succeeded; restoration preserved
  the same process identity, with wake/exec taking 1.046 s.
- The test sandbox and its builder preparation were deleted. Final inventory
  had no sandbox routes, pending creates/builds, or incomplete migrations;
  the user's capacity reservation remained present. The small test image and
  build history remain subject to normal retention and garbage collection.
- Collector, Grafana, Tempo, and VictoriaMetrics health checks passed.

The earlier two-worker live consolidation check preserved process identity
while moving a parked sandbox from worker `12395623` to `12395622` in 0.943 s.
This release packages that same tested source; the post-install smoke above
verifies the newly bootstrapped runtime and snapshot lifecycle.

## Deployment exceptions and remaining evidence limits

The first installation attempt used an incorrect CLI entry point for service
reconciliation. The script restored the previous hotfixed source and config,
but its restart command had the same mistake, briefly leaving gateway services
stopped. Services were recovered with the canonical command
`python -m ucloud_sandboxes.systemd gateway-reconcile --config ...`, and the
corrected installation then succeeded. There were no sandbox routes in the
restart preflight inventory. Existing capacity reservations were preserved.

The first build smoke request supplied an unqualified tag with `push=true`,
which attempted a Docker Hub push and was denied. The corrected request used
the gateway-managed private registry and succeeded.

The final metric window reported the expected cold-image pending response as
an ensure-image error, plus one background snapshot-publication error alongside
three successful publications. The snapshot error trace was not present in the
retained error search. The successful park/restore checks do not establish that
this intermittent publication error is fixed. Historical unexplained worker
shutdown and publication errors remain as described in the investigation.
These smoke checks do not establish behavior under a full production workload.

## Artifact identity

Artifacts, manifests, deployment records, smoke output, and rollback files are
retained on the gateway under `/work/ucloud-sandboxes/release/0.5.34`.

| Artifact | SHA-256 |
| --- | --- |
| Wheel | `0a76b217a79773d739b93f252d013571c8f76ac7f4a849a4cdeab5fa07849644` |
| Sandbox bundle | `8c7a0285d56d51ac33b7062cad99278d57ba808f23683c18238e70eb4a6b8f3b` |
| Builder bundle | `b2c052e4aa9f262af72708a9b88537d98da6bf2bd8856f593a94a91021e66b96` |

Both bundles retain the previously qualified operating-system/runtime and
storage binaries and contain the committed release wheel. Bundle directory
traversal and archive readability were checked as the service user. Rollback
database snapshots are audit evidence; they must not overwrite current state.
