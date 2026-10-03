# Chunk store M2: migrating the production corpus

M2 of the [chunk store design](chunk-store-design.md#9-build-plan): every
production image's environment moves from today's registry-backed EROFS
components to chunk-store RAFS roots. Dispatch switches to the new roots, then
the old OCI copies and components are released and swept. This plan refines
design §7 with what the code does today (survey of 2026-10-03, `file:line`
below) and with measured production numbers.

Status: **decided 2026-10-03** (§9); nothing has shipped. It depends on the M1
gate (§1).

## 1. Preconditions

M2 starts when all of these hold:

1. **M1 gate run 2 passes** (`docs/benchmarks/m1-gate-<date>/`):
   - full tree on all 181 images;
   - stored bytes within 5% of 17.5 GB, now with per-pack commits;
   - crash injection at all 12 steps;
   - rollback byte-exact on 10 of 10 images.
2. **Cold-start parity with today's path.** In run 2's 20-way burst, the chunk
   store's traced wall time and first-command medians must be within 1.3× of
   the baseline worker `b1` (today's path, same images and worker type).
   Switching a family is a rollout-latency change, so this replaces S11's
   5.5 s as the bar that matters for M2. If it fails, M2 waits for the
   create-path work behind it, not for storage.
3. **A production store node** (§4.1) has passed its C2.6 gates on production
   traffic, warmed with wave 1's foundations.

## 2. Today, measured (2026-10-03, read-only)

| Fact | Value | Source |
| --- | --- | --- |
| Registry Volume | 3.9 TB, **2.8 TB used (76%)**, 920 GB free | `df /mnt/ucloud-registry` on the gateway |
| Managed image repositories | 8,294 | `repositories/ucloud-managed` |
| Environment manifests (roots and components) | 19,948 | `repositories/environments/_manifests/revisions` |
| Workers | `worker_enabled`, 128 GiB environment cache, `attach_concurrency` 1, `chunk_store` off | live `deployment.json`; `make_config.py:123-172` |

The design's "3.81 TB today" was an S10 extrapolation; the Volume holds 2.8 TB.

**Inventory, measured** (`chunk-migrate inventory`, 2026-10-03, read-only, about
2 minutes; summary in
[benchmarks/m2-inventory-2026-10-03](benchmarks/m2-inventory-2026-10-03/summary.json)):

| Family | Images | Task rows | Unique EROFS, GB | Unique OCI, GB | Build inputs |
| --- | ---: | ---: | ---: | ---: | ---: |
| SWE-smith | 118 | 63,585 | 106 | 72 | all |
| OpenSWE (prepared) | 27 | 39,224 | 8 | 5 | all |
| TMax | 1,240 | 12,956 | 552 | 305 | all |
| Terminal-Lego | 2,344 | 11,746 | 320 | 149 | all |
| ScaleSWE | 2,469 | 2,469 | 272 | 193 | all |
| SWE-rebench v2, SWE-Lego, R2E-Gym, MultiSWE | 273 | 273 | 259 | 170 | all |
| Unknown (not in the selection) | 1,455 | 0 | 410 | 302 | 686 |
| Foundations | 6 | 0 | 1 | 0.2 | all |
| **All** (shared bytes once) | **7,932** | | **1,890** | **1,171** | **7,163** |

What it changes:
- **Almost everything is a build input.** Every prepared task image is a
  prepared source or decision, which a later build's `FROM` resolves to, so its
  OCI manifest stays. Releasing the OCI copies of the rest frees only **88 GB**.
  The design assumed ScaleSWE and the SWE-* families were not build inputs.
- **The win is the EROFS side: 1.89 TB.** A switched image's EROFS root and
  components lapse even when its OCI manifest stays (§3.3), so the Volume goes
  from 2.8 TB to about **1.2 TB** (the OCI layers, about 1.17 TB unique), with
  the chunks in S3.
- **The selection is not the scope.** It was drawn from what had already been
  prepared. The goal is every image of every environment prepared, so the
  corpus grows to tens of thousands of images: OpenSWE alone has 35,549 (C2.14).
  "Not in the selection" means not prepared yet, never unneeded.
- **The unknown family** is 1,455 images:
  - 694 `precomputed-*`;
  - 526 `bl20260929-*` (the 2026-09-29 build-load benchmark);
  - 122 `agentic-*`;
  - 56 `import-*`.

  They migrate like any other image. Only proven test artifacts, such as the
  benchmark's warm-up images, are candidates for deletion, from a list the
  operator reviews (§9).
- **OCI copies are the long-term Volume problem** (§9, decision 5). Every
  prepared image is registered as a build input today. With everything
  prepared, keeping each one's OCI copy would grow the Volume by many TB. That
  is what "not many TB more" rules out.

## 3. What the code does today, and what M2 must change

### 3.1 Root resolution

- **The gateway pins the image but not the root.**
  `_resolve_request_image_reference` (`control_plane.py:4810-4821`) and
  `ImageResolution.resolve` (`gateway/image_resolution.py:437-526`) rewrite
  `spec.image` to `repo@sha256:<annotated manifest digest>` and take a
  `ucloud-digest-*` protection tag.
- **The worker reads the root from the manifest annotation.**
  `DirectProvisioner.create` → `EnvironmentRootfsStore._resolved`
  (`environment_rootfs.py:178-219`) → `load_image_environment`
  (`environment_artifact.py:855-873`). The result is cached in a 128-entry LRU
  (`environment_rootfs.py:188-209`).
- **The create payload has no root field, and nothing can add one silently.**
  `SandboxSpec.from_dict` rejects unknown fields (`sandbox.py:493-519`), and
  `_ucloud_operation` must have exactly four keys (`sandbox.py:140`).
- **Hazard: a cross-node move resolves the image again.** Portable import
  re-resolves `spec.image` (`direct_provisioner.py:209`) and checks
  `rootfs_identity_sha256` (`:323-328`). A sandbox that started on the old
  root and moves after the switch would get the new root and fail that check.

**Change (readers first, §5 step 0).** `SandboxSpec` gains an optional
`environment_root` (a root manifest digest):
- **Gateway.** It always sets the field at create: from `image_roots` (§3.2) if
  the image is mapped, otherwise from the annotation it already reads for
  retention (`environment_dependencies.py:15-37`).
- **Worker.** With the field set, the worker loads that signed root directly.
  When the manifest still exists, it also requires the root's `source_image`
  to equal the manifest's config digest, so a mapping can never attach one
  image's root to another. Without the field it reads the annotation as
  today.
- **Pinning.** The root then lives in the spec, so routes, receipts and
  portable snapshots keep the root the sandbox started with, which closes the
  re-resolve hazard.
- **Rollout.** Older releases reject the new field, so the gateway sends it
  only to workers whose heartbeat advertises the capability (§3.5).

### 3.2 The `image_roots` table

The design's mapping, `image_roots(manifest_digest → root)`, plus a migration
journal.
- **Store.** A new non-strict SQLite file in the gateway state directory,
  `image-roots.sqlite3`, opened like `prepared-images.sqlite3`
  (`prepared_images.py:79-96`, `CREATE TABLE IF NOT EXISTS`). Adding it to
  `images.sqlite` or `registry-usage.sqlite` would trip their strict schema
  checks (`images.py:644-651`; `managed_registry.py` around :846) and break a
  rollback to an older release.
- **Key.** `(repository, annotated manifest digest)`. Catalogs, routes and
  `ImageRecord.manifest_digest` all pin the annotated digest
  (`environment_artifact.py:840-852`).
- **Columns.** `old_root`, `new_root`, `config_digest`, `wave`, `state`
  (`converted` → `switched` → `released`, or `reverted`), `build_input`, and
  times.
- **Journal.** One row per state change, so a switch or a release can be
  replayed or reverted (design §7, rollback).
- **Readers:**
  - the gateway's create resolution (§3.1);
  - `ImageResolution.resolve`'s digest branch, which must answer from
    `image_roots` for a `released` image instead of HEADing a deleted manifest
    (`image_resolution.py:442-447`);
  - `enrich_records` and `record_missing_manifest`, which must keep image
    records whose manifest was released (`:301-306, 549-573`);
  - `EnvironmentDependencyResolver`;
  - retention (§3.3);
  - the commit-build path, which reads a parent's root
    (`environment_builder.py:995`).

### 3.3 Retention

- **Live roots.** `ImageEnvironmentIndex.roots` (`registry_retention.py:253-285`)
  must take a mapped image's root from `image_roots`, before the annotation,
  in both the plan and the delete-time recheck (`cli.py:2123-2165`).
  - Rows in `converted` or `switched` state keep the new closure live: root,
    components, and their `rafs-root-*` / `rafs-*` tags. Without that, retention
    deletes a converted root after the 1 h grace.
  - Rows in `switched` state also keep the old closure until no route
    references it.
- **Durable owner rows pin the old roots forever.** Owners named
  `<owner>:environment` hold them:
  - `image-pool:` (`scripts/prepare_image_pool.py:631-634`);
  - `image-foundation:` (`prepare_image_foundations.py:298-300`);
  - `shared-task:` and `shared-source:` (`prepare_shared_task_image.py:479,505`).

  At the switch, the migration tool acquires the same owners on the new
  closure. At release, it releases them on the old closure
  (`release_owner`, `managed_registry.py:1172`).
- **Running routes keep their closure** until the route ends
  (`registry_refs.py`, `release_route_reference`). That is the "no live route
  references an old root" release condition of design decision 6. No calendar
  hold.
- **Pressure eviction** (`cli.py:1951-2031`, `EnvironmentBlobIndex`) must not
  evict a mapped image's record. Its bytes are in S3, not on the Volume, so the
  Volume projection drops them.

### 3.4 Release and sweep

- **Delete.** `RegistryClient.delete_manifest` (`managed_registry.py:516-521`)
  removes a digest and every tag on it. No command releases one image today;
  the migration tool adds one, fenced by leases like `execute_reference_prune`
  (`registry_retention.py:605-682`).
- **Sweep.** `python -m ucloud_sandboxes.systemd registry-gc
  --allow-service-interruption` (`systemd.py:663-692`).
  - It stops the registry for the whole sweep: cold image and EROFS chunk
    reads, builds, imports and checkpoint publication all stall. Running
    sandboxes continue.
  - Blobs released less than 2 h before it survive (`registry_blob_grace_seconds`).
  - Its duration on 2.8 TB is unmeasured; the only figure is 1.2 s on a 5-blob
    fixture. §5 step 1 measures a dry walk first.
  - The sweep needs a window with gateway admission paused, so no create waits
    on the stopped registry.

### 3.5 Placement and capability

- **No advertisement today.** No heartbeat or placement field says a worker can
  read RAFS (`capabilities.py`, `gateway/placement.py`), and a worker without
  a chunk index refuses RAFS components (`environment_backend.py:249-254`).
- **Change (built).** Heartbeats advertise `environment-root-dispatch-v1` (the
  worker honours the spec field) and `environment-rafs-v1` (env-io has a store
  node, `--environment-rafs`). A spec with `environment_root` requires both.
  - The rule is derived from the spec alone, so retries and moves, which
    re-read the route's spec, enforce it too.
  - So `dispatch_roots` is turned on only once every worker runs 0.9.0 with
    the chunk store; with it on, a create no worker can take is a retryable
    `no_ready_node`.

## 4. Infrastructure

### 4.1 Production store node

- **One store node** (CCX43 or larger, with local NVMe) on the private network
  serves `ucloud-chunk-store` and `ucloud-chunk-index`, with S3 as the durable
  store (design §2). Production config has the block ready, with
  `CHUNK_STORE = False` (`make_config.py`).
- **Cache size.** The cache must hold the hot set of a training run: the
  foundations plus the run's task images. 500 tasks × about 300 MB of touched
  extents is about 150 GB. A CCX43's disk caps the cache near 200 GB.
- **S3 stays the store of record.** Converters write every pack to S3, and
  the store node is only a read-through cache in front of it. Workers read the
  store node, not S3: S12 measured S3's tail (1 MiB p99 of 5.5 s) and found
  that private workers reach it through the gateway's NAT.
- **Availability, not durability.** Losing the store node loses no data, and a
  replacement refills from S3. While it is down, workers cannot fetch chunks
  they lack: cold starts of released images fail with EIO, because workers fail
  closed and never read S3. Running sandboxes and images already in a worker's
  cache are unaffected.
- **Today's exposure is the same.** All cold reads come from the one gateway
  registry on its Volume today, and that Volume is the only copy of its data.
  The store node holds only a cache. So M2 needs no second store node for
  parity. It needs a replacement runbook: a new store node from its snapshot,
  warmed from S3 with the running training's foundations. The replacement
  time is measured once before wave 1's release.

### 4.2 Conversion capacity

- **Throughput.** The M1 gate converted and verified about 2.5 images per minute
  on one CCX63 at 12-way, so about 55 hours for 8,300 images on one
  converter.
- **Plan.** Run two CCX63 converters, rate-limited to keep the gateway
  registry's reads under 100 MB/s (design §7). That is about 28 hours of
  conversion, about €26 at list price.
- Converters write chunks to S3 and to the index on the store node. They are
  disposable and keep no state.

## 5. Steps

0. **Readers first: release 0.9.0.** No behaviour changes yet; every switch is
   off.
   - **Sandbox spec.** `environment_root` in the spec, validated as described in
     §3.1.
   - **Heartbeats** advertise the two capabilities.
   - **Gateway.** `image_roots.sqlite3` with its journal, the resolver's
     dispatched root, root assignment at create (clients may not set one; a
     retry keeps its route's root) and retention, behind
     `immutable_environments.dispatch_roots` (default off).
   - **Before the first release, not in 0.9.0:** release-aware resolution
     (`ImageResolution.resolve` and `enrich_records` answering a released
     digest from `image_roots`), and the commit build reading a parent's root
     from it.
   - **Workers** get the `chunk_store` block, so env-io runs with the chunk
     index and the store node. With dispatch off they still mount today's EROFS
     roots.
   - **Rollout.** Through the usual canary and snapshot rollout. Then turn
     `dispatch_roots` on with an empty table: every create carries its
     annotation root, which proves the field end to end with no change in
     roots.
1. **Inventory and dry runs (read-only).**
   - **Inventory.** A `chunk-migrate inventory` command lists every managed image:
     - repository, tags and annotated digest;
     - root and components, with OCI and EROFS bytes;
     - family, taken from the catalog receipts
       (`prepare_image_pool.py:660-664`, `prepare_image_foundations.py:336-338`)
       and the training selection;
     - whether it is a build input (`prepared-images.sqlite3`'s sources,
       foundations and decisions; FROM pins in user Dockerfiles are not indexed,
       so they are treated as build inputs);
     - its live routes and durable owners.
   - **Outputs.** From that, the waves, the predicted release per wave and the
     predicted new S3 bytes.
   - **Sweep timing.** A timed dry walk of the registry for the sweep (no
     deletes), to size the maintenance window.
2. **Per wave: convert and verify** (design §3).
   - **Conversion.** The full-tree check runs on every image, and each converted
     image gets an `image_roots` row in `converted` state.
   - **Warming.** Before the switch, the store node is warmed with the wave's
     foundations and its most-used task images (`warm-chunk-store
     --component`).
3. **Per wave: switch.** Move the wave's rows to `switched`:
   - re-point the durable owners to the new closure;
   - flush the gateway's resolution caches;
   - run canaries: the C9.2 rollout scenario, with 20 sandboxes per family
     running the family's first command, on the new roots;
   - watch the corruption metric (`environment_io` corruptions, which must stay
     at 0) and cold-start latency against the wave-0 baseline.
4. **Per wave: release** once the wave meets every condition in design
   decision 6:
   - all verified;
   - canaries pass;
   - no live route or registration references an old root or component.

   Then:
   - delete the non-build-input OCI manifests and their `ucloud-digest-*`
     tags;
   - release the old owners;
   - let online retention lapse the old roots and components;
   - run one sweep in a window with admission paused, and record the Volume.
5. **Releases come after every wave is switched** (§9, decision 2). Each release
   ends in its own sweep, and the next release starts after the previous sweep.
   - Each wave adds at most 150 GB of new chunks.
   - Each wave must release within ±10% of its predicted bytes.

Waves, ordered by training rows per byte. Chunks are content-addressed, so
order does not change deduplication, only how fast each wave covers training:
1. SWE-smith and OpenSWE's prepared images: 145 images, 103k task rows, 114 GB
   of EROFS;
2. TMax and Terminal-Lego: 3,584 images, 25k rows, 872 GB;
3. ScaleSWE, SWE-rebench v2, SWE-Lego, R2E-Gym, MultiSWE and the foundations:
   2,748 images, 531 GB;
4. the unknown family, less whatever §9 decides to delete: up to 1,455 images,
   410 GB.

### 5.1 Running a wave (built)

`chunk-migrate` carries steps 2 and 3. Wave membership is the inventory
family (`WAVES` in `chunk_migrate.py`, the list above).

1. **Convert, on each converter** (root, with the production
   `environment-producer` signing key, the chunk-store S3 key and the index
   write token):
   ```bash
   ucloud-sandboxes chunk-migrate convert --config converter.json \
     --chunk-index-token-file write.token --work-root /var/lib/convert \
     --environment-registry-url http://10.42.0.2:5000 --environment-registry-repository <environments> \
     --environment-trusted-keys trust.json --environment-signing-key producer.pem \
     --rows inventory.jsonl --wave 1 --results wave-1.jsonl --parallel 12 \
     --verify-device /dev/nbd0 ... --verify-device /dev/nbd23
   ```
   - Each image converts in its own process (`convert-environment
     --nydusd-blobs`), with its own index owner and its slot's NBD devices.
   - The full-tree check is mandatory.
   - A rerun skips converted images and retries failures.
2. **Record, on the gateway** (as `ucloud`): `chunk-migrate record --results
   wave-1.jsonl`. Each verified result becomes a `converted` row, once:
   - both roots load with the gateway's trusted keys;
   - the old root is still the image's annotation;
   - both roots name the image's config.

   Refusals are listed per image.
3. **Switch** (as `ucloud`, with `dispatch_roots` on, or it refuses):
   `chunk-migrate switch --wave 1 [--rows inventory.jsonl --family SWE-smith]`.
   - Rows move to `switched`.
   - Every durable owner of the image (`image-pool:`, `image-foundation:`,
     `shared-task:`, `shared-source:`) is acquired on the new closure, through
     the gateway's own protection path and lease fence.
   - Routes keep the root their spec pinned.
   - No cache to flush: the resolver keys its cache on the dispatched root.
4. **Revert** (§7): `chunk-migrate revert --wave 1 [--family …] --reason …`.
   Retention keeps a reverted root, so the rollback drill can switch the same
   rows again.
5. **Status**: `chunk-migrate status`, rows per wave and state.

**Not built yet: release.** It is needed only after every wave is switched
(§9, decision 2). It needs:
- release-aware resolution;
- the release conditions of design decision 6;
- deleting the manifests, under the lease fence;
- releasing the old closure's owner rows one by one. `release_owner` would
  drop the new closure too.

## 6. Gates

| Gate | Measure | When |
| --- | --- | --- |
| Readers first | 0.9.0 canary plus the autoscaled canary. With dispatch on and an empty table, 100% of creates carry their annotation root and nothing else changes | step 0 |
| Verified | 100% of a wave full-tree equal; no `image_roots` row without a verified conversion | each wave, before the switch |
| Canaries | C9.2: 20 sandboxes per family; first-command p50 and p95 within 1.3× of the wave-0 baseline; zero failures | each switch |
| Integrity | zero `environment_io` corruptions and zero EIO without a store-node outage, over the switch-to-release window | each release |
| Release | Volume drops by the predicted bytes ±10%; S3 grows by at most 150 GB | after each sweep |
| Rollback drill | revert one switched family from the journal, run its canaries on the old roots, then switch it again | wave 1, before its release |

## 7. Rollback

- **Before a wave's release:** revert its rows from the journal. Dispatch falls
  back to the annotation roots, which are intact. Seconds; no data moves.
- **After release:** regenerate an OCI image with `unpack-environment`, which is
  byte-exact since `6431144` and was tested on 10 images in M1. Then rebuild
  today's EROFS root with today's builder and repoint `image_roots`.
  - This costs minutes per image.
  - Plan for a family, not the corpus.
- **Lost for good:** a Docker-worker rollback for released images. Production
  workers already run EROFS (design §7).

## 8. Work breakdown

| Piece | Where | Size |
| --- | --- | --- |
| `SandboxSpec.environment_root`, worker resolution and binding check, portable pinning | `sandbox.py`, `environment_rootfs.py`, `direct_provisioner.py` | ~150 lines |
| Capabilities and RAFS-aware dispatch | heartbeats, `capabilities.py`, `gateway/placement.py`, `control_plane.py` | ~120 |
| `image_roots.sqlite3`, journal, gateway readers | new `gateway/image_roots.py`, `image_resolution.py`, `environment_dependencies.py` | ~250 |
| Retention and eviction from `image_roots`; owner re-pointing | `registry_retention.py`, `cli.py`, `managed_registry.py` | ~150 |
| `chunk-migrate`: `inventory`, `convert --wave`, `record`, `switch`, `revert`, `status` (built); `release` | new `chunk_migrate.py` | ~400 (437 built) |
| Production store node, config, runbook | `make_config.py`, `hetzner.md` | runbook |
| Tests: spec compatibility both ways, mapping, release-then-resolve, retention, the re-resolve hazard | `tests/` | ~500 |

**Effort.** About 1.5 weeks to build, matching design §9. The calendar is set
by the waves: 3 or 4 switch-to-release cycles of 1–3 days each, with no fixed
holds.

## 9. Decisions (2026-10-03)

1. **One store node.** No second node in M2. Instead, a replacement runbook
   whose replacement time is measured before wave 1's release (§4.1). That
   matches today's single gateway registry. A second node (about €110/month)
   is reconsidered only if the measured time is unacceptable during training
   runs. A worker fallback to S3 stays rejected (S12: slow, and through the
   gateway NAT).
2. **Switch everything first, then release wave by wave.** Every wave is
   converted and switched before the first release, which keeps rollback a
   journal revert for as long as possible. There is room for it: 920 GB free,
   and the Volume never receives chunk bytes. Releases then shrink the Volume
   in wave order (design decision 4).
3. **Sweep windows: measure first.**
   - Step 1's timed dry walk sizes the sweep.
   - If it fits about 15 minutes, each release ends in a window with gateway
     admission paused.
   - If not, the sweep is made online (deleting blobs while the registry
     serves) before the first release, rather than stopping the registry for
     longer.
4. **User-built images are build inputs.** FROM pins in user Dockerfiles are
   not indexed, so every wave-4 image keeps its OCI manifest. Only its EROFS
   root and components are released.
5. **Open: which prepared images keep an OCI copy.** Today the catalog registers
   every prepared task image as a build input. Most are leaves that a build
   rarely or never uses as `FROM`, apart from import aliases.

   Recommended:
   - keep OCI for foundations, prepared sources that builds actually name,
     and user-built images;
   - release the leaf task images' OCI copies. A build that needs one gets it
     back with `unpack-environment`, which is byte-exact.
   - C2.14 then builds new prepared images straight into the chunk store,
     keeping OCI only for foundations.

   **What the gateway records (2026-10-03).** Nothing records `FROM`:
   - build history (8,689 builds, 2026-09-29 to 10-01) keeps timings only;
   - `prepared-build` leases are transient and gone once a build ends.

   The selection's family states (review of 2026-10-02) answer most of it:
   - **Attach-only families** (ScaleSWE, SWE-smith, R2E-Gym, SWE-Lego,
     rebench v2, MultiSWE): about 2,860 images and about 435 GB of OCI. They
     are in the catalog for import aliases, which resolve creates, not builds.
     A `FROM` on them would be a user Dockerfile naming an upstream task
     image, which is rare.
   - **TMax and Terminal-Lego sources** are real `FROM` bases for the live task
     setup that still runs at request time.
   - **OpenSWE** builds from its foundations.

   So M2 can release the attach-only families' OCI copies once
   `resolve_build` can regenerate a released reference with `unpack`.

   **No new counter is needed.** `prepared_decisions` already keeps every
   `resolve_build` rewrite, one row per distinct build context. Read-only on
   2026-10-03, it holds 35 decisions:
   - 26 rewrote nothing;
   - 6 named terminal-prefix foundations;
   - 3 named two precomputed sources;
   - none named an attach-only family's image.

   It cannot see a user Dockerfile that names a managed task image directly,
   because no rewrite happens. `unpack` covers that case. Once C2.14 moves the remaining task
   setup offline, sources become intermediate too, and the end state keeps
   OCI only for foundations.
