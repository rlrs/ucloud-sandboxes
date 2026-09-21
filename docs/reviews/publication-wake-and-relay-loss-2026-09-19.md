# Background publication and lost relay callers

Production uploads ran for up to 235 seconds, beyond the storage client's
120-second timeout. The daemon continued publishing after the Warden released
its lock. A subsequent wake then failed with `volume is publishing; cannot
mount`, and restore cleanup failed with the same conflict for `discard`.

Release 0.5.55 gives local wake, discard and deletion precedence over a background
publication. A journal transaction retains the sealed local layers, fails the
superseded publication operation, and advances its revision. Upload completion
and failure remain fenced to their original operation/revision: neither can
replace the resumed volume's authority or remove its local checkpoint files.
The Warden no longer holds its lifecycle lock during network publication. Its
publication request carries the observed revision, so a delayed request cannot
seal a filesystem that has already resumed or been parked again.

Registry and S3 publication queues check that their snapshot is still current.
Superseded queued uploads exit without waiting for active uploads to complete.
An upload already performing remote I/O may finish, but cannot commit stale
local authority. This change does not make local checkpoints survive worker loss.

The relay now reads the gateway's durable loss identities in its maintenance
loop. It completes pending and leased requests for those exact sandbox
generations with HTTP 410 / `node_lost`, releasing queue and payload capacity.
Already committed model responses are retained and their delivery holds released;
no successful wake is fabricated. A replacement sandbox generation is unaffected.
An absent route or transient connection failure alone does not trigger this cleanup.

Regression coverage exercises blocked uploads racing with wake/discard/delete,
both upload success and failure, owner and revision fencing, a delayed upload
after another park cycle, the Unix socket path, Warden lock handoff, cancellation
in both backend queues, and durable relay cleanup with preserved model results.

## Follow-up: exported-layer index identity and deleted callers (0.5.56)

Live testing of 0.5.55 created 16 sandboxes successfully, but repeated
park/publication/wake cycles exposed an independent storage defect. The first
run failed two mounts in its second wake cycle. A second isolated reproduction
failed one of 16 in its third cycle, and another failed one in its second cycle.
These are failed qualification runs, not evidence of reliable 256/512 operation.

On worker 12396596, sandbox `cache-repro-b9c3330e-014` failed with XFS bad
superblock magic. Its two registry blobs and corresponding local block-cache
headers and indexes matched byte for byte. The saved merged index instead
mapped logical sector zero to physical sector 18168 of its newest layer;
the layer's own index mapped it to sector 8. The resulting read returned
`332e31322f686173686c69622e7079da` (Python file contents), rather than the
XFS header `58465342000010000000000000208000`. Direct I/O and a kernel
buffer-cache flush did not change that. Removing only the merged-index artifact
made the same backend and snapshot return the correct header immediately.

Dense export preserves a layer's UUID and can preserve its file size and index
geometry while rearranging physical extents. Those metadata fields therefore
cannot identify cached physical offsets. The native patch hashes the serialized
per-layer index into the merged-cache key, with a new key namespace that cannot
reuse the old artifacts. Identity reads are bounded to 64 KiB chunks; cache hits
still avoid parsing and merging all the indexes. Regression coverage exports
out-of-order writes while preserving UUID and geometry, then checks reads through
the same merged cache. A companion fix prevents blob-cache initialization from
deleting sibling cache directories such as `premerged-index`.

Separately, 111 retained relay requests belonged to explicitly deleted sandbox
callers, not to matching worker-loss records. Relay maintenance now also uses
durable terminal program records with `sandbox deletion requested`, returning
410 / `sandbox_deleted` for unfinished requests and releasing delivery holds
without replacing completed model responses. Cleanup remains generation-specific.

Validation before deployment: the new dense-export regression returned 0x22
instead of 0x11 when the index-content identity was omitted, and passed with the
fix. All seven merged-index tests and 50 block-cache tests passed on the gateway's
Linux kernel (one unrelated cache test is marked ignored upstream). The canonical
repository check passed 929 server tests (six platform skips), 118 SDK tests,
lint, package checks and Go tests. Applying all five pinned native patches to a
clean upstream tree reproduced all 13 modified source files exactly.

## Complete the wake path (0.5.57)

Release 0.5.56 passed 16 concurrent sandboxes through eight publication/wake
cycles with preserved files and process identity, zero gateway health failures,
and gateway health p95 45 ms. Its timing exposed an outer guard in
`DirectNodeRuntime.wake_with_activity_revision`: it rejected wakes while any
publication thread was alive, before the storage journal could supersede that
publication. The first wake cycle took up to 21.6 seconds and later cycles
roughly 4.5–4.9 seconds, with repeated `snapshot_publication_pending` responses.

Release 0.5.57 removes that redundant thread-level veto while preserving the
exclusive lifecycle transition and the storage journal's owner/revision fencing.
The runtime regression now verifies a wake reaches storage and advances its
activity revision even with publication pending. The blocked-upload concurrency
tests continue to verify that stale uploads cannot replace resumed authority or
delete its checkpoint.

## Production qualification

Runtime commit `a73c5f2fc73f57e2e6da6530ec5987441dcc97c1` (0.5.57) was deployed
at 18:34:28 UTC on 2026-09-19. Gateway, relay and autoscaler were restarted;
both worker roles use validated 0.5.57 bundles. The native executable SHA-256
was verified on a live worker as
`26f986bc3eca90f7650129981d8d9d6615832a85c68962db34ddde59e1a93018`.
CI run 35461517645 passed. The follow-up canonical check passed the same 929
server and 118 SDK tests; 156 targeted tests also passed on production Linux.

Using SDK 0.4.23 and managed-process parkable sandboxes (2 requested CPUs,
1024 MiB memory, 5184 MiB disk), every operation passed:

| Runtime | Concurrent sandboxes | Complete park/publish/wake cycles | Wake + tool-check p95 range | Gateway health p95 |
| --- | ---: | ---: | ---: | ---: |
| 0.5.56 | 16 | 8 | 4.46–21.63 s | 45 ms |
| 0.5.56 | 64 | 8 | 8.69–15.42 s | 132 ms |
| 0.5.57 | 64 | 8 | 7.56–8.51 s | 85 ms |
| 0.5.57 | 256 | 3 | 32.27–34.76 s | 407 ms |

Each cycle verified uploaded/downloaded bytes, a persistent file hash, and the
resumed process's UUID and monotonic counter. The 0.5.57 64-way run started on
a fresh worker; the 256-way run used that warm worker plus a newly scaled worker
(174 and 82 sandboxes respectively). Thus the latter is not a 256-way entirely
cold-worker benchmark. Both workers reported 0.5.57. All test sandboxes were
deleted, with no cleanup errors or remaining routes. No gateway health probes
failed (174 in the final 64-way run and 267 in the 256-way run).

The final 64-way run had zero `snapshot_publication_pending` retries, versus
366 in the corresponding 0.5.56 run. The 256-way run also had none. Burst admission
still required retries: provisioning observed retryable 503/no-ready-node/image
warmup responses, and the three 256-way park batches observed 216
`http_request_capacity_exhausted` responses in total. These were retried and all
operations completed, but they remain a throughput/latency limitation; this
qualification does not establish 512-way capacity or zero-backoff service.

Relay maintenance converted all 111 stale deleted-caller requests to completed
terminal results after deployment. Completed model responses remain preserved.
No SDK or Verifiers code was changed for these server-side fixes.
