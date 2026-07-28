# Direct runtime production qualification

Status: the direct Warden is deployed behind the public SDK gateway as a
dedicated UCloud canary and its basic client lifecycle has passed. The broader
production-flow and product-contract gates listed below remain. The direct
Warden is the replacement runtime; it must not share a node or state store with
the legacy Docker-owned task runtime.

## Deployment invariant

Every node persists one immutable runtime identity containing the runtime
kind, state schema, runsc digest, runsc commit, boot-configuration digest, and
rootfs format. Startup fails closed on mismatch. There is no per-sandbox
runtime selector.

Qualification uses dedicated drained nodes:

```text
legacy release -> drain to zero -> direct release -> production-flow tests
```

Rollback is:

```text
close direct-node admission -> drain/delete on matching direct binary
-> terminate node -> deploy legacy only after direct state is empty
```

No direct artifact is ever handed to the legacy runtime.

## Required flow matrix

`local` means deterministic tests can run in this repository. `production`
means the flow must also be run by an operator against the real gateway,
scheduler, registry, network, and workload clients.

| Flow | Local gate | Production gate | Current state |
| :--- | :--- | :--- | :--- |
| Image pull/cache/export | content-addressed rootfs, crash cleanup, no started Docker task | private registry pull, auth, tag update, cache restart | public BusyBox pull/export passes on a clean node; private registry flow pending |
| Create/idempotent retry | durable intent, exact generation/spec fencing, quota before runsc | gateway create and retry through normal placement | public SDK create through normal autoscaled placement passes |
| Command/env/workdir/security | OCI translation tests and real runsc conformance | representative production images and profiles | deterministic OCI translation passes; named users require a product decision |
| Tool exec | wake once, readiness, exit/stdout/stderr | real tool calls through gateway | public SDK exec before and after automatic park passes |
| Streaming exec/stdin/TTY | bounded backpressure, cancellation, resize/close | SDK websocket and interactive clients | stdout/stderr/stdin and full-lifetime fencing wired; TTY/resize pending |
| File upload/download | lifecycle lease, path/size limits, parked wake policy | SDK file APIs and large-file limits | public SDK binary upload/download and persistence across park pass; large-file limits pending |
| Networking | none/bridge policy, sockets across park | private network, DNS, egress and service callbacks | pending |
| SSH | lifecycle-safe session policy or explicit removal | SDK SSH proxy and reconnect | decision pending |
| Snapshot/fork | excluded from the initial replacement release | none for initial release | explicitly deferred; do not retain legacy runtime |
| Park/wake | two-phase artifact, bounded restores, exact fingerprint | real inference/tool traces and burst tails | local stateful benchmarks and clean-node 24-way API burst pass |
| Delete/TTL | running/parked/transitional races and idempotent GC | gateway delete, expiry, retry and cancellation | public SDK parked delete and exact registry/quota/bundle/journal cleanup pass; broader boundary crash matrix required |
| Drain/deployment | admission close, complete inventory, zero-leak exit | actual node replacement and rollback drill | autoscaler-created direct nodes and repeated scheduler replacements pass; explicit rollback drill pending |
| Crash/restart | every durable boundary, candidate adoption/fencing | kill node agent/runsc during real requests | 11,000 local reopen cases pass |
| Disk guarantee | ledger equals XFS project quota for all mutable state | fill-to-quota/ENOSPC on production filesystem | total hard quota passes on XFS; separate writable/runtime hard limits and native production volume pending |
| Heartbeat/accounting | parked inventory and logical reservation reported | scheduler placement at capacity | live parked journal has no runsc backend, zero active memory, and exact disk/XFS ownership; saturation pending |
| Observability | latency, faults, bytes, queueing, reconciliation metrics | dashboards and alerts | pending |

## Clean-node qualification, 2026-07-28

The standalone node API was deployed on UCloud job `12360789`
(`gvisor-direct-runtime-qualification`) in project `DFM Pretraining`, using
product `cpu-amd-zen5-32-vcpu` (reported CPU model AMD EPYC 9535), 96 GB RAM,
and a 600 GB root disk. Qualification used a physically allocated 450 GiB
loopback XFS filesystem with project quotas, mounted at
`/var/lib/ucloud-direct-data`. The loop device is appropriate for functional
qualification but is not the production storage topology.

The node ran one systemd-managed direct Warden and no legacy task runtime.
Docker was used only to pull and export the image. The Warden used
`disk_overcommit=1`, `memory_overcommit=2`, and `cpu_overcommit=3`, with
444,000 MB of admitted physical sandbox disk capacity after headroom.

The deployed runsc was built from gVisor commit
`9f653e577965df2ddd13875b5530cd2588661f1c` plus the three-patch hibernation
series. Its SHA-256 is
`a3cd70d5de7cc33d1f58fb1cfbaba32e2a310794aca8b7e30ba0435baed8888e`.
The runtime advertised only `sandbox`, `image-cache`,
`hibernate-local-v1`, and `direct-runsc-v1`.

One complete API lifecycle passed with a persisted file and process-state
restore:

| Operation | Latency |
| :--- | ---: |
| Image pull/export | 1,002 ms |
| Create | 541 ms |
| Binary write | 190 ms |
| Binary read | 15 ms |
| Live exec | 54 ms |
| Park | 180 ms |
| Wake plus exec | 550 ms |
| Delete | 127 ms |

A second sandbox was parked, the daemon was restarted, and the sandbox
remained parked with no live runsc backend. Wake plus exec took 612 ms,
preserved the file payload, and delete took 155 ms. The node epoch changed,
and its quota inventory returned to empty.

Drain was then enabled and rejected create with HTTP 503. After another daemon
restart, admission remained closed with the same drain token. Supplying that
exact token reopened admission. This proves daemon-restart durability; a
whole-node scheduler replacement drill is still required.

Finally, 24 distinct sandboxes ran create, persistent marker write, park,
restore plus marker verification, and delete at concurrency 8:

| Phase | min | p50 | p95 | max |
| :--- | ---: | ---: | ---: | ---: |
| Create | 671 ms | 748 ms | 873 ms | 874 ms |
| Park | 77 ms | 126 ms | 229 ms | 237 ms |
| Wake plus exec | 536 ms | 616 ms | 686 ms | 686 ms |
| Delete | 116 ms | 163 ms | 297 ms | 305 ms |

The burst completed in 6.50 seconds. While all 24 were parked, the heartbeat
reported zero active vCPU and memory and 32,256 MB of physically guaranteed
disk reservation. Every marker was correct after restore, and the final
sandbox, process, artifact, and quota inventories were empty. Raw results are
in
[`benchmarks/gvisor-direct-node-api-2026-07-28.json`](benchmarks/gvisor-direct-node-api-2026-07-28.json).

The workload is intentionally small (`busybox sleep` plus a file marker). It
qualifies ownership, lifecycle, persistence, accounting, and concurrency; it
does not substitute for representative agent-process memory and I/O traces.
After the final empty-state audit, job `12360789` was terminated successfully
so the 32-vCPU qualification allocation was released.

## CPU-quota-matched deployment, 2026-07-28

A matched rerun found that the apparent restore regression was not disk or API
overhead. The original direct benchmarks omitted `SandboxSpec.cpus`, while the
deployed API requested 0.25 vCPU. On the same UCloud host, binary, XFS volume,
network mode, and production directory layout, adding that OCI quota changed
the `runsc restore` phase from approximately 114 ms to 515–666 ms. XFS project
quota, a unified quota directory, 128 versus 512 MB memory limits, and
`cpu.max.burst` were independently ruled out.

The fourth pinned gVisor patch adds a transactional restore CPU startup burst.
A new candidate starts without the steady-state CPU quota, and `runsc` applies
the exact original OCI resources before restore succeeds. The Warden admits no
tool exec before that command and its readiness check complete; failure destroys
the candidate through the existing restore rollback. Node restore concurrency
remains bounded.

The content-addressed runtime for this deployment is
`runsc-hibernate-92ab664bba4c39c77cf1e9d0afe421c37acbdecf4e4c5ac58a1c905ef056c075`.
Its exact source and patch identities are in
[`benchmarks/gvisor-build-manifest-2026-07-28.json`](benchmarks/gvisor-build-manifest-2026-07-28.json).
The focused gVisor tests and optimized build passed on UCloud job `12360810`.

The replacement runtime was then deployed as the sole sandbox task owner on
UCloud job `12360830`, on a physically allocated 50 GiB loopback XFS
qualification volume with project quotas. An isolated 0.25-vCPU API wake
measured:

| Phase | Before fix | After fix |
| :--- | ---: | ---: |
| `runsc restore` | 516 ms | 114 ms |
| Warden restore total | 577 ms | 155 ms |
| Client wake plus exec | 635 ms | 213 ms |

The restored sentry's host cgroup reported `cpu.max = 25000 100000` before the
verification exec, proving that the 0.25-vCPU hard limit was back in force
before sandbox work ran.

A synchronized 24-sandbox burst with eight restore slots then completed the
wake phase in 833 ms. Client wake-plus-exec p50/p95 were 511/750 ms; Warden
restore p50/p95 were 203/283 ms; and the `runsc restore` phase was 122/137 ms.
The p50 restore queue was 197 ms. All markers survived and final sandbox and
quota inventories were empty. Raw phase timings are in
[`benchmarks/gvisor-direct-node-api-cpu-startup-burst-2026-07-28.json`](benchmarks/gvisor-direct-node-api-cpu-startup-burst-2026-07-28.json).

This resolves the benchmark regression and qualified the node artifact used by
the scheduler and gateway canary below.

## Public SDK canary, 2026-07-28

The replacement runtime is deployed as the only sandbox task owner behind
`https://app-sandboxes.cloud.sdu.dk`:

- gateway job `12360876`, deployment `direct-20260728`;
- autoscaled worker job `12360931`, 32 physical vCPU, 96 GiB RAM, 600 GB node
  disk, 440 GiB quota-backed XFS sandbox storage;
- service release `0.3.53` and SDK release `0.4.3`;
- patched runsc SHA-256
  `92ab664bba4c39c77cf1e9d0afe421c37acbdecf4e4c5ac58a1c905ef056c075`
  from gVisor commit
  `9f653e577965df2ddd13875b5530cd2588661f1c`;
- advertised capabilities `sandbox`, `image-cache`, `disk-quota`,
  `hibernate-local-v1`, and `direct-runsc-v1`.

The autoscaler created clean direct-only nodes from the release bundle. VM init
verified the runsc digest and commit, installed root-owned direct state, waited
for the node API before starting heartbeat, and never started the legacy task
agent. Stale qualification workers were terminated before acceptance so the
gateway could only place on the final build.

A public `SandboxClient` request using the default non-root security profile
created a parkable BusyBox sandbox in 3.200 seconds. First exec took 0.562
seconds. Binary upload/download succeeded. After the one-second idle threshold,
the lifecycle journal reported `state=parked` and `authority=parked`, its
checkpoint was complete inside the XFS quota project, and `runsc list` had no
live backend. Wake plus exec took 0.867 seconds and returned the file written
before parking.

SDK delete then removed the direct registry record, disk-ledger reservation,
XFS quota path, OCI bundle, lifecycle journal, and runsc backend. Matching
generation tombstones remained in the registry and disk ledger. The acceptance
run used only the public gateway and SDK APIs for lifecycle operations; SSH was
used afterward only to audit node-local ownership.

The versioned artifact check installed the built SDK `0.4.3` wheel in an
isolated environment and ran it through gateway `0.3.53` and worker `0.3.53`.
Create took 2.497 seconds, automatic park followed, wake plus exec took 0.603
seconds with persisted state intact, and SDK delete succeeded.

This canary supports `network="none"` container sandboxes. Private networking,
SSH, TTY/resize, private-registry auth, representative production images, and
the wider failure matrix remain gated below. Fork remains deliberately
excluded.

## Remaining release gates

The following work is still required before production traffic:

1. Put the sandbox data root on a native XFS volume with project quotas in the
   node image. The qualification loopback filesystem must not become the
   production deployment recipe.
2. Give writable workspace data and runtime/hibernation data independent hard
   quota boundaries inside each sandbox's physically backed total. Otherwise a
   client that fills its writable limit can consume the space needed to park.
   Disk admission must remain at or below physical capacity throughout.
3. Extend the passing public SDK canary from BusyBox to the private registry,
   representative production images, retries, cancellation, expiry, actual
   model/tool cadence, and scheduler saturation accounting.
4. Implement and qualify the selected production network mode, then decide
   whether SSH is supported or deliberately removed. The clean-node run used
   network `none`.
5. Finish TTY/resize behavior or explicitly remove it from the initial product
   contract.
6. Ship the pinned runsc artifact through the signed node-image/release path,
   with its identity manifest, conformance report, and startup fail-closed
   checks.
7. Add production metrics, dashboards, and alerts for park/wake latency,
   restore queueing, bytes written/read, quota failures, reconciliation, and
   leaked ownership.
8. Complete the broader crash-boundary, ENOSPC, rollback, and whole-node drain
   matrix on canary nodes.

Fork remains deliberately deferred and is not a migration blocker. Once these
gates and the product-flow matrix pass, legacy admission can close, legacy
nodes can drain to zero, and the Docker-owned task runtime can be removed.

## Production test record

Production runs must persist:

- deployment and node IDs;
- exact runtime identity and conformance report digest;
- image digests and sandbox generations;
- request IDs for create, exec, file, park/wake, delete, and drain;
- latency distributions and failure/retry results;
- pre/post process, mount, quota, bundle, journal, and artifact inventories;
- disk logical reservation, hard quota, allocated blocks, and free space;
- final proof that the node reached zero owned sandboxes before replacement.

The legacy runtime cannot be deprecated until every required row is passing or
has an approved product decision removing that flow. Fork has that decision:
it is excluded from the initial direct-runtime release and can later be built
over native Warden artifacts without restoring the Docker task lifecycle.

## Removal sequence

1. Stop adding features to the legacy task runtime.
2. Qualify the direct runtime on dedicated non-production nodes.
3. Run the full production matrix on dedicated canary nodes.
4. Close legacy admission and drain all legacy nodes to zero.
5. Remove the Docker task runtime, restore wrapper, checkpoint helper, runtime
   flags, legacy capabilities/probes, and runtime-specific tests.
6. Keep Docker/containerd image build, pull, cache, inspect, create-without-
   start, export, and cleanup infrastructure.
