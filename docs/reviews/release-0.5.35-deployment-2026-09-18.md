# Release 0.5.35 deployment verification

Server **0.5.35**, commit `e3d805fefe839133f7ec1545e7d9a9938d01fa22`, was
pushed to `codex/sandbox-linux-compatibility` and installed on the DFM Pretraining
gateway at **21:03:12 UTC on September 18, 2026**. The final worker bundles include
the packaging correction in `bed4ac2aa57fa019f6950d27a0b9436122448489`.
SDK **0.4.20**, commit `8e09a8dd394bf63473d43188638a7113a8a87e86`, was pushed
to `main` and [published with wheel and source artifacts](https://github.com/rlrs/ucloud-sandboxes-sdk/releases/tag/v0.4.20).

The release bounds shared gateway/worker startup admission, avoids synchronous
full-inventory work for one-sandbox lookups, and allows bounded early scale-out
for sustained capacity queues. The SDK retries explicit pre-dispatch rejection
responses within the caller's deadline. See the
[burst investigation](cold-start-burst-2026-09-18.md) for the evidence and limits.

## Deployment coverage

There were no sandbox routes or active capacity reservations before installation.
Gateway and autoscaler were restarted; relay and registry remained running. All
90 installed gateway package files matched the release wheel. Both public health
endpoints returned healthy, and the gateway/relay file descriptor limits remained
65536. No production scheduling policy setting changed.

The temporary qualification reservation booted worker **12396266** using sandbox
bundle `59b8155346372e01dc4e3a0058949ec90cc5739d8f0a4f8ba27c140c1063f6a1`.
The running process used that bundle's Python path, reported version 0.5.35,
and had `--max-concurrent-startups 8`. Its installed `direct_service.py`,
`node_agent.py`, `node_runtime.py`, and `cli.py` hashes matched the committed
source. Importing the CLI as the unprivileged heartbeat user succeeded. The
heartbeat independently reported 0.5.35.

Both sandbox and builder bundles were replaced and validated. The builder
bundle was not separately boot-tested in this deployment. The actual Verifiers
runner's environment was not identified or upgraded; it must install SDK 0.4.20
to receive the new safe-retry behavior.

## Validation

- Local canonical checks passed: 884 server tests (six skipped), 99 SDK tests,
  Ruff, ShellCheck, shell syntax, Go tests, builds and installed-wheel checks.
  The five repacker tests passed after adding the permission regression.
- [Server release CI](https://github.com/rlrs/ucloud-sandboxes/actions/runs/35394432299),
  [packaging-fix CI](https://github.com/rlrs/ucloud-sandboxes/actions/runs/35395160586),
  and [SDK CI](https://github.com/rlrs/ucloud-sandboxes-sdk/actions/runs/35394437516)
  all passed.
- Sixteen concurrent SDK creates eventually succeeded on the new worker. All
  sixteen automatically parked, then passed simultaneous implicit restore,
  upload, execution and byte-exact download checks. Each combined restore/I/O/
  execution sequence took 1.291–2.881 seconds.
- The SDK observed eight `node_restore_busy` and two `node_startup_busy`
  responses and completed the operations. It also handled capacity/image
  preparation responses while the worker became ready.
- All 216 public gateway health probes passed during the test; maximum observed
  latency was 71 ms.
- Four immediate cleanup requests met active-activity 409 fences; a later
  bounded cleanup retried them successfully. At **21:10:22 UTC**, no sandbox
  routes or capacity reservations remained. Worker 12396266 reported zero
  active sandboxes/creates, and all four gateway services were active. The idle
  worker remains subject to normal autoscaler scale-down.

## Deployment exceptions and measurement limits

Preflight caught an inaccessible release directory and an incorrect relay-file
check before services were changed. Those installer issues were corrected.

The first qualification worker, 12396265, started the root node service but its
unprivileged heartbeat could not import the package. Repacking under the
terminal's restrictive umask had created implicit wheel directories without
traversal permission for that user. Bootstrap retried and the failed worker was
retired. The repacker now explicitly sets package-directory permissions; a
regression exercises umask 077. Both bundles were rebuilt, their archive modes
checked, and replacement worker 12396266 booted successfully.

The live create batch overlapped this packaging repair and worker replacement:
it took 196.355 seconds, including 1,351 `no_ready_node` retry responses. This is
not a clean cold-start throughput measurement. The successful restore/I/O checks
and health probes establish deployment functionality, not 256-way capacity.
No new 256-sandbox production run was performed, and the initial incident's VM
poweroff remains unexplained by retained telemetry.

## Artifact identity

Artifacts, rollback files, deployment result, smoke output, and cleanup record
are retained under `/work/ucloud-sandboxes/release/0.5.35` on the gateway.

| Artifact | SHA-256 |
| --- | --- |
| Server wheel | `0ff532de042ad93c6ac3df54141721b3ef82e79a3998e0a4b540023068bcf63b` |
| Sandbox bundle | `59b8155346372e01dc4e3a0058949ec90cc5739d8f0a4f8ba27c140c1063f6a1` |
| Builder bundle | `3f4ce55539579c41ecdf7e43ef7e72715a4c4b597233a4d5f9c1cd5f3ac0673f` |
| SDK wheel | `27c8b06e5214461874b86983c0fc5794c489c8828e92625cf946cade6d35b366` |

Both worker bundles preserve the qualified OS, gVisor, and storage binaries.
Rollback SQLite copies are audit artifacts and must not overwrite live state.
