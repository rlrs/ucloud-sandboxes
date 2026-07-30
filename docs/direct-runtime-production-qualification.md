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
| Networking | none/bridge policy, sockets across park | private network, DNS, egress and service callbacks | isolated veth, DNS/TLS egress, relay callbacks, and a live TCP connection across real park/wake pass; cross-node live connection pending |
| SSH | lifecycle-safe session policy or explicit removal | SDK SSH proxy and reconnect | decision pending |
| Snapshot/fork | excluded from the initial replacement release | none for initial release | explicitly deferred; do not retain legacy runtime |
| Park/wake | two-phase artifact, bounded restores, exact fingerprint | real inference/tool traces and burst tails | local stateful benchmarks, clean-node 24-way API burst, and public SDK + Verifiers inference across real park/wake pass |
| Delete/TTL | running/parked/transitional races and idempotent GC | gateway delete, expiry, retry and cancellation | public SDK parked delete and exact registry/quota/bundle/journal cleanup pass; broader boundary crash matrix required |
| Drain/deployment | admission close, complete inventory, zero-leak exit | actual node replacement and rollback drill | autoscaler-created direct nodes, destination-shortage scale-up, public cross-node migration, automatic parked evacuation, last-owner retention, and final empty-node stop pass; restart injection and rollback pending |
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

## Single-owner paused handoff, 2026-07-30

The fifth pinned gVisor patch can create a restored backend with all guest tasks
stopped and can replay `runsc resume` across a metadata-persistence crash. The
final content-addressed runtime is
`runsc-hibernate-342c8141209d85d7456dee7bf319e7bfebf93b05cb5186ff3cf19f7a73048dd1`.
It was built and qualified on DFM job `12361749` from gVisor commit
`9f653e577965df2ddd13875b5530cd2588661f1c`; the exact five-patch identities
are in
[`benchmarks/gvisor-build-manifest-2026-07-30.json`](benchmarks/gvisor-build-manifest-2026-07-30.json).

The production linear-restore path deliberately uses a single-owner inode
handoff, not a reflink. `runsc restore` moves the parked application-memory
inode into the new backend while that backend remains paused. The Warden then
fences and durably journals the exact candidate before resuming it. A failed
handoff kills the candidate and returns the same inode to the published
generation. This has three useful properties:

- allocated storage never temporarily exceeds the sandbox's hard quota;
- there is exactly one authoritative memory backing throughout the handoff;
- the restored backend retains the backing inode's warm page-cache identity.

A sequential ten-cycle comparison used a 1,024 MiB sandbox at 0.25 vCPU with
512 MiB populated, a 2,048 MiB hard disk request, the production XFS/project
quota layout, and a checksum scan before and after every park/wake:

| Restore mode | Wake p50 | Wake p95 | First 512 MiB scan p50 | First scan p95 |
| :--- | ---: | ---: | ---: | ---: |
| destructive, immediately running baseline | 228 ms | 310 ms | 366 ms | 467 ms |
| reflink, paused | 344 ms | 363 ms | 1,094 ms | 1,220 ms |
| reflink, paused, `POSIX_FADV_WILLNEED` | 308 ms | 344 ms | 818 ms | 1,019 ms |
| **single owner, paused** | **259 ms** | **275 ms** | **416 ms** | **418 ms** |

The single-owner safety boundary costs approximately 31 ms at p50 versus the
unsafe immediately-running baseline. Its median phases were 165 ms in
`runsc restore`, 17 ms for candidate state, 32 ms for replay-safe resume, and
32 ms for the readiness exec. Every restored cgroup reported
`cpu.max = 25000 100000` before guest work was admitted. Raw records are in
[`benchmarks/gvisor-dense-512-single-owner-paused-2026-07-30.json`](benchmarks/gvisor-dense-512-single-owner-paused-2026-07-30.json),
with the three comparison runs beside it.

A separate 20-cycle sparse run measured 259/276 ms wake p50/p95 and 285/400 ms
park p50/p95:
[`benchmarks/gvisor-sparse-single-owner-paused-2026-07-30.json`](benchmarks/gvisor-sparse-single-owner-paused-2026-07-30.json).
Finally, an in-guest counter was held for 250 ms at each candidate handoff. It
remained exactly `19→19`, `72→72`, and `132→132` while paused, then advanced to
`30`, `82`, and `142` after resume:
[`benchmarks/gvisor-paused-counter-single-owner-2026-07-30.json`](benchmarks/gvisor-paused-counter-single-owner-2026-07-30.json).

These results reject reflink as the default linear restore primitive. Reflink
remains useful when fork/fan-out is implemented; prefetch softens but does not
remove its distinct-inode page-cache penalty. OverlayBD, ublk, and dirty-page
tracking remain deferred until a feature actually requires multiple durable
owners, remote backing, or live migration.

## Production cutover on DFM compute, 2026-07-30

The direct-only deployment is live at the established production endpoints:

- gateway `https://app-sandboxes.cloud.sdu.dk`;
- model relay `https://app-sandboxes-relay.cloud.sdu.dk`;
- fallback relay `https://app-sandboxes-relay-v2.cloud.sdu.dk`.

Gateway job `12361919` runs package `0.3.59` and deployment
`direct-20260730` on project `DFM`
(`5530ccd4-2828-4031-9275-d51aa231cc01`). UCloud permits the job to attach the
existing production resources from `DFM Pretraining`: private network
`12345327`, gateway ingress `12345368`, and relay ingresses `12346842` and
`12349454`. This preserves every client URL while charging compute to the DFM
allocation. The always-on gateway uses the 2-vCPU/6-GiB product. Its obsolete
32-vCPU predecessor `12361901`, the previous production job `12360876`, and
the temporary DFM canary gateway `12361866` are stopped with final state
`SUCCESS`; the canary URL is no longer bound.

The production pool uses:

- scale-to-zero worker pool using 32-vCPU/96-GiB VMs with 2,000-GB disks;
- 1,800-GiB XFS project-quota image, 16-GiB disk headroom, and 96-GiB swap;
- exact `1.0` CPU, memory, and disk admission factors;
- eight concurrent restore slots and no builder nodes.

The first production-URL SDK request provisioned worker job `12361906` from a
genuinely empty pool in 78.073 seconds. Its live heartbeat advertised
1,826,816 MiB of effective sandbox disk (1,784 GiB after headroom), 98,304 MiB
memory, and 32 vCPU. Physical XFS capacity was 1,842,300 MiB. The worker ran
only `serve-direct-node-agent` as sandbox task owner; Docker/containerd
remained image/bootstrap infrastructure. Its deployed `runsc` SHA-256 was
`342c8141209d85d7456dee7bf319e7bfebf93b05cb5186ff3cf19f7a73048dd1`,
matching the qualified build manifest.

The SDK created a default non-root, parkable BusyBox 1.36.1 sandbox with
0.25 vCPU, 256 MiB memory, and 256 MiB requested disk. Admission reserved
1,600 MiB because parkable memory backing and filesystem overhead are
physically charged in addition to client disk. XFS project ID 200000 enforced
the same hard limit. Public exec, a 29-byte binary upload/download
including NUL and `0xff`, automatic idle park, and state-preserving wake all
passed. First exec took 770 ms. The wake-plus-exec client wall time was 825 ms
and returned the pre-park marker, appended marker, and binary length. SDK
deletion returned an
empty public sandbox inventory, an empty worker inventory, full advertised
capacity, no disk-ledger reservation, and matching generation tombstones in
the direct registry and disk ledger. At the 600-second node idle threshold, the
autoscaler closed admission, waited for a fresh empty drain proof, submitted
the provider stop, and returned the pool to zero. Worker `12361906` reached
final UCloud state `SUCCESS`. Benchmark job `12361749` was stopped after the
content-addressed artifacts were copied to the project drive.

That canary used the original host-API inactivity heuristic to exercise the
mechanics. It is not the production parking signal: the host cannot distinguish
an idle guest from a harness doing CPU/network work without an API call.
Production direct nodes now disable implicit idle parking and use an explicit
relay or client lifecycle request.

## Live Verifiers parking qualification, 2026-07-30

Canary worker job `12362056` ran the pinned Verifiers v1 `NullHarness` through
the public Python SDK, production gateway, production relay, and a worker-local
`InterceptionServer`. The relay's durable-acceptance callback parked the real
sandbox while the harness was awaiting its OpenAI-compatible response. The
observed lifecycle was:

```text
running -> parked -> waking -> running
```

The first representative attempt exposed that `runsc checkpoint` consumes the
external sandbox-network veth. Restore had entered an existing netns with no
usable address and therefore configured only loopback. The direct runtime now
re-materializes and validates the namespace, both veth ends, the 1420-byte
path MTU, the stable `/31` address, and the default route immediately before
every restore. Partial kernel leases are discarded and rebuilt.

The same test also qualified gVisor's built-in connected-socket checkpointing.
The runtime identity now includes `allow_connected_on_save=true`, and the
restore log proved both the original `eth0` address/route and
`AllowConnectedOnSave:true`. The unmodified harness connection received the
committed relay response after wake; no guest sidecar and no HTTP reattachment
were used.

The final non-debug run on the original qualified runsc completed in 25.052
seconds including image/script setup. The relay-to-park observation took 1.548
seconds. Verifiers returned
`relay-live-park-ok`, with one model generation, one trace turn, zero
reattachments, and final sandbox state `running`. This is a correctness
qualification, not a steady-state latency benchmark.

The test additionally found that a restored PID 1 could exit after the
readiness exec while the durable journal still said `running`. Read paths and
traffic admission now prove the recorded sentry PID/start time and reconcile a
dead owner to `recovery-required` instead of advertising false capacity.

The first installation was incorrectly reported as exit 7 even though every
service became healthy: its fixed two-second delay raced service startup. The
deployment renderer now polls gateway, relay, and registry health for up to 30
seconds each, failing only after the bounded readiness window.

## Isolated Verifiers migration qualification, 2026-07-30

An isolated DFM canary upgraded gateway and relay job `12362088` through 0.3.64,
without changing production job `12361919` or its public URLs. The canary used
the production 2-vCPU gateway shape, internal relay address
`10.36.136.151:8092`, exact relay-only private egress from sandbox network
namespaces, 2-TB worker disks, and CPU/memory/disk admission factors of 1.0.

The pinned Verifiers v1 `NullHarness` ran through the canonical SDK and an
ordinary capability URL. Durable relay acceptance parked the harness on fresh
source worker `12362099`. The gateway then provisioned fresh destination worker
`12362100`, pulled the missing base image before fencing the source, imported
the authenticated archive, and completed the public migration request in
70.96 seconds.

A phase-instrumented 0.3.64 repeat moved source worker `12362103` to destination
worker `12362104`. The public migration request took 77.96 seconds. Destination
provisioning and image readiness accounted for approximately 62.58 seconds;
the migration protocol itself took 15.38 seconds:

| Phase | Time |
| --- | ---: |
| Source prepare and checkpoint export | 1.290 s |
| Archive transfer and destination staging | 13.882 s |
| Atomic route commit | 8.043 ms |
| Destination activation | 22.960 ms |
| Source finalization | 150.695 ms |

Thus the previously reported 16.52 seconds described the whole migration
protocol, not the route handoff. The ownership-changing SQLite transaction is
an 8 ms operation in the instrumented run; archive transfer and staging is the
dominant warm-path cost.

Cross-node restore intentionally selected a different guest IP and closed the
source TCP endpoint. The relay committed the exact model response, woke the
destination in 0.57 seconds, published the changed transport epoch, and served
the stored result to the harness retry. The final counters were one enqueue,
one completion, one accepted notification, one wake notification, one
transport reset, and one reattachment. Verifiers recorded one model call and
one trace turn, returned `relay-live-park-ok`, and the sandbox finished
`running` after:

```text
running -> parked -> moving_out -> parked -> waking -> running
```

The qualification also exercised a create/drain race. A node closed admission
after placement but before create. Version 0.3.63 makes that rejection
structured and retryable, removes the provisional route, and restores pending
demand instead of pinning the sandbox identity to a draining node.

This is an isolated correctness gate, not a production rollout. Production
remains on its existing release until an explicit cutover decision and a final
SDK/Verifiers run through the production gateway and relay URLs.

## Previous public SDK canary, 2026-07-28

The earlier replacement-runtime canary was the only sandbox task owner behind
`https://app-sandboxes.cloud.sdu.dk` until the 2026-07-30 cutover:

- gateway job `12360876`, deployment `direct-20260728`;
- scale-to-zero worker pool; accepted `0.3.56` canary workers used 32 physical
  vCPU, 96 GiB RAM, 600 GB node disk, and 440 GiB quota-backed XFS sandbox
  storage;
- gateway/autoscaler release `0.3.58`, public mobility worker canary releases
  `0.3.56` through `0.3.57`, and SDK release `0.4.4`;
- patched runsc SHA-256
  `92ab664bba4c39c77cf1e9d0afe421c37acbdecf4e4c5ac58a1c905ef056c075`
  from gVisor commit
  `9f653e577965df2ddd13875b5530cd2588661f1c`;
- advertised capabilities `sandbox`, `image-cache`, `disk-quota`,
  `hibernate-local-v1`, `direct-runsc-v1`, and `sandbox-migrate-v2`.

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

After upgrading the gateway to `0.3.54`, a versioned SDK `0.4.4` regression
prepared one parkable sandbox with 128 MiB memory and 64 MiB writable disk. The
gateway normalized this to a 1,280 MiB hard checkpoint reservation, a clean
autoscaled worker created the matching sandbox in 81.209 seconds from
scale-to-zero, exec succeeded, and allocation atomically removed the prepared
unit. SDK cleanup left no pending, prepared, build, or sandbox demand. This
closes the phantom prepared-capacity failure where a raw writable-disk hint did
not match the full parkable sandbox reservation.

This canary supports `network="none"` container sandboxes. Private networking,
SSH, TTY/resize, private-registry auth, representative production images, and
the wider failure matrix remain gated below. Fork remains deliberately
excluded.

## Portable parked-state mobility benchmark, 2026-07-28

Two isolated 32-vCPU, 96-GiB UCloud VMs ran the exact pinned direct runtime over
their private node network. The source produced a portable, digest-verified
parked-state archive; the destination pulled it directly with a single-archive
capability, rebuilt local inode-bound metadata, atomically adopted ownership,
and woke the restored process. The gateway never carried the archive bytes.

An empty sandbox with a logical 1-GiB memory image had only about 368 KiB
physically allocated. Its 389,120-byte archive exported in 11.254 ms and
imported in 18.447 ms. For a process with 256 MiB of resident zero-filled
memory, gzip level 1 reduced the cross-node archive from 272,998,400 bytes to
2,087,019 bytes. Export took 0.932 seconds, network plus destination staging
2.014 seconds, activation 6.5 ms, and the first restored exec 220 ms.

The incompressible 256-MiB case produced a 269,366,479-byte archive. Export took
5.425 seconds, network plus staging 4.380 seconds, activation 12 ms, and the
first exec 308 ms. This is suitable for node evacuation and stranded-wake
repair, but too expensive to put in every idle transition. Raw results are in
[`benchmarks/gvisor-direct-migration-2026-07-28.json`](benchmarks/gvisor-direct-migration-2026-07-28.json).

Local correctness now covers portable archive validation, destination quota
adoption, same-generation return to a prior node with a separate migration
epoch, replayed migration rejection, source fencing, atomic route handoff,
restart-safe phase retry, wake admission, and evacuation planning. A real
autoscaler-driven whole-node evacuation through the public SDK remains a
production gate.

Networked migration now has an explicit safe reconnect contract. The v2
portable manifest authenticates the source guest IP and declares
`connection_policy=disconnect`; an empty destination still skips that IP when
allocating its local `/31`. On restore, gVisor cannot find the checkpointed
TCP endpoint's old local route and closes it rather than attempting live TCP
through a different node's NAT. The gateway hashes committed migration IDs
into a transport epoch on every successful park/wake response. The relay
persists the park epoch and, when wake returns a different epoch, publishes the
request fingerprint before releasing the committed response. This also covers
autoscaler evacuation that completes before the later wake request, and a
same-generation A-to-B-to-A sequence changes the epoch on both handoffs.

Unit coverage proves destination IP exclusion, fail-closed replay of a
conflicting lease, strict v2 manifest validation, epoch changes only after
route commit, and same-request response reattachment despite changed
OpenAI retry/tracing headers. A real two-node networked Verifiers migration
while inference is outstanding remains the production qualification gate.

## Public mobility canary, 2026-07-28

Release `0.3.56` was deployed through the canonical all-in-one builder, which
rebuilt the wheel set, preassembled node-agent runtime, pinned runsc metadata,
and deterministic node bundles together. Worker jobs `12361066` and
`12361067` both advertised exact `1.0` CPU, memory, and disk overcommit plus
`sandbox-migrate-v2`.

The public Python SDK created a default non-root, parkable BusyBox sandbox in
1.965 seconds on job `12361066`. SDK upload, exec, and download through
`/workspace` returned `portable-state-ok`. After idle parking, the first
migration request found no eligible destination and returned retryable `503`
while persisting a disk-only 1,280-MiB relocation shape that excluded the
source node. The autoscaler correctly started job `12361067` even though the
source still had ample free disk.

Retrying the public migration route moved generation 1 from `12361066` to
`12361067` in 1.371 seconds with a complete migration journal and atomic route
handoff. Wake plus exec took 0.667 seconds; SDK exec and download both returned
the pre-migration marker. SDK delete then left zero sandbox routes, pending
requests, prepared capacity, image warmups, logical resource ownership, and
node inventory on both workers. Both temporary jobs terminated with UCloud
state `SUCCESS`, returning the deployment to scale-to-zero.

Here too, idle parking describes the historical qualification trigger, not the
current production policy.

This closes the live destination-shortage, autoscaler scale-up, node-to-node
transfer, route-continuity, non-root file compatibility, wake, and cleanup
gates. Controller-restart phase injection remains below.

The gateway/autoscaler was then upgraded to `0.3.57`. Its pre-drain placement
guard refuses a scale-down candidate unless every parked disk shape fits on
ready nodes that remain after the same stop batch. This prevents the last
disk-owning node from starting a replacement and moving identical state
sideways without reducing node count.

## Automatic scale-down evacuation canary, 2026-07-28

Release `0.3.58` was qualified with two canonical `0.3.57` workers, jobs
`12361072` and `12361073`, and one parked 1,280-MiB disk reservation on each.
The canary temporarily reduced the idle threshold from 600 seconds to 5
seconds. The first live attempt correctly closed admission on job `12361073`
but exposed a missing gateway credential on the autoscaler's internal migration
request. The gateway rejected it with HTTP 401 without changing the route.
Release `0.3.58` now reads the existing gateway token from its root-controlled
file and authenticates only that internal request.

After the upgrade, the durable drain resumed with the same intent. The
autoscaler moved sandbox B from job `12361073` to job `12361072`, observed an
empty drain proof, and terminated job `12361073`. The complete migration
journal spanned 0.673 seconds. The remaining node retained both parked
reservations across repeated scale-down cycles without creating a replacement.
This proves the no-net-gain guard on the real scheduler rather than only in
unit tests.

Public SDK exec then woke each sandbox on job `12361072` in 0.628 and 0.741
seconds. Both pre-migration `/workspace` markers were intact. SDK deletion left
zero routes and the node heartbeat converged from 2,560 MiB to zero reserved
disk. The autoscaler then drained and terminated job `12361072`; the pool
returned to zero without pending or prepared demand. The production idle
threshold was restored to 600 seconds after the canary.

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
8. Complete the remaining drain-failure matrix: a running sandbox that parks
   during drain and controller restart during each migration phase. Parked
   automatic evacuation, last-owner retention, final empty-node stop,
   destination shortage, node scale-up, route continuity, and empty ownership
   now pass in public canaries.
9. Complete the broader crash-boundary, ENOSPC, rollback, and whole-node drain
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
