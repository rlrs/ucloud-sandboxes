# Release 0.5.75: prepared, deployment blocked by workspace quota

Commit `746ad507d03e0f197fb4d47640f4e94112a94382` was pushed to
`codex/checkpoint-publication-efficiency`. It includes the PostgreSQL relay
implementation already qualified/activated in production, subsequent gateway
and worker coordination improvements, durable journal batching, warm retention,
size-tiered compaction, and cancellation of deleted volumes' maintenance work.
Automatic trim is still disabled. No SDK or Verifiers update is required.

The exact server wheel SHA-256 is
`69bd3d2d73f11602c0f72ecd97ee932b7cec56b26bdc02a07f0204099d4566b8`.
Linux validation passed **1,195 tests, 10 skipped**, with the real PostgreSQL
qualification database enabled, in 101.818 seconds. Ruff and diff checks passed.
The built wheel installed and passed the installed-package verifier in a clean
Linux virtual environment. [Evidence](../benchmarks/release-0.5.75-2026-09-22/status.json).

## Production interruption before deployment

At **09:17:00 UTC / 11:17 Copenhagen on 22 September**, UCloud suspended gateway
job **12379311** with `This workspace has reached the storage quota`, followed by
`The virtual machine is powered off`. This happened before this release was
installed. Public gateway and relay health endpoints returned HTTP 503; the
provider shell fell back to an unavailable serial console. No serving packages,
configuration, node bundles or systemd units were modified during this rollout.

UCloud accounting reports **173,000 GB used / 173,000 GB allocated**, zero remaining,
for the DFM Pretraining project's storage product. The previous native tests
used `/var/tmp` on the worker's local `/dev/vda1`; their disposable volumes were
cleaned up. This establishes the reported suspension reason, not which project
workload exhausted the shared quota. Unrelated project data was not deleted.

## Remaining rollout and load qualification

Free project storage or increase the allocation, then recover the gateway and
validate its PostgreSQL authority/backups before installing. Preserve the
PostgreSQL configuration and SQLite cutover fence. Existing gateway/relay units
have qualification overrides; updating an unused gateway venv is insufficient.
Update the actual serving executable paths, future-node bundles and all current
workers/builders; verify exact wheel contents and advertised versions before load.

Release wheel, tests, manifest, repacker and bundle validators are prepared in
`/private/tmp/release-server-0.5.75` locally, with staging script
`/private/tmp/release-0575-stage.sh`. The new realistic workload script is staged
on the independent `rasmus-dev` driver at
`/home/alex-admin/ucloud-pg-qualification-20260921/scripts/live_relay_load_benchmark.py`.
Credentials remain in its existing private files, outside the repository.

Run the public PostgreSQL relay path with four-agent smoke, then 64 and 256
agents and at least eight rounds. Keep the established 128 MiB resident memory,
16 MiB dirty pages, 4 MiB file writes, 100 ms CPU and 10–15 s synthetic model waits.
Measure natural parking and report observed parking coverage; separately test
forced park/restore so warm retention cannot conceal a restore regression.
Report completion-to-wake and response-ready-to-verified-exec p95 against 0.8 s,
plus failures, retries, cleanup, worker placement and storage/memory pressure.
The native microbenchmarks are not a substitute for these product load tests.

**Deployment and realistic production load tests have not run for this release.**
