# Rebuildable image campaign: 2026-09-30

`inputs.json.gz` is a 1.3 MB recipe bundle stored in Git, independent of the registry volume. It contains 35,984 source references and the exact dependency contexts for 8,602 shared foundations: 11 OpenSWE, one explicit TMax scientific foundation, 2,786 TMax inline prefixes and 5,804 Terminal prefixes. It excludes task solutions, registry blobs, credentials, production addresses, build IDs and success receipts.

The initial snapshot contains 191 resolved source digests. `inputs-refreshed.json.gz` preserves the same source definitions and 8,602 foundation contexts, with 451 resolved source digests (SHA-256 `7ac209b302a7a2edd48741ff6bb98ca325f85ac3d3261ba6de6e358b9a52f2e7`). Existing pins and context bytes were compared against the initial snapshot and are unchanged. The remaining references have not all been resolved or built. The preparer records additional immutable resolutions on the gateway root disk, outside `/mnt/ucloud-registry`, before building. Preserve those work directories when replacing only the volume. Refresh this bundle from those directories to preserve later pins off-host as well. A Git snapshot is only as current as its recorded inputs.

`inputs-bases-complete.json.gz` advances the snapshot to 693 source digest pins, including all 94 inventoried generic task bases. It preserves every earlier pin, source definition and foundation context (SHA-256 `1a1c6268971b9cadc1e391638c59fe034c821b13c30a0d6f8b8a76a65362c2f3`). The separate `task-bases` recovery stage prepares all of those generic bases before the expensive task-image tail. This is not a claim that every task-specific SWE base or dependency installation is prepared.

`inputs-shared-proof.json.gz` preserves that snapshot and adds the immutable source pin for the newly qualified Click PR1002 task base: 694 source pins, with unchanged foundation contexts (SHA-256 `289739d8029ef1220988379452894526102afd0d201a1fd849635f746bf225ea`). Shared-image experiments are not an automatic recovery transformation; the canonical original source remains the fallback.

Foundation context identities and bundle checksums are verified before submission. Source dataset revisions are included in the source inventory and foundation records. Input definitions come from research-environments `c7ea0d7fe3379f0e6ffb0a68c82c80c184601f92` and verifiers-ucloud `7856cc18faceae31aa533605ea812d81b92a85ea`; actual training selection was unavailable. BIRD and NeMo remain deferred.

## Recovery after losing a registry volume

Provision an empty working managed registry and healthy deployment using the normal deployment tooling. The rebuild workflow does not provision, resize or delete volumes. Preserve signing configuration and credentials through the existing deployment recovery mechanism; neither belongs in this public-input bundle.

If only the registry volume was lost, first run `pack` below against the surviving preparation directories on the gateway root disk, so recovery includes all source pins learned since this Git snapshot. Use that refreshed bundle in place of `inputs.json.gz`.

On the gateway, use a checked-out repository containing the preparation scripts and a compatible SDK wheel. Run as the gateway service account:

```sh
python scripts/image_campaign.py materialize \
  --bundle image-campaigns/2026-09-30/inputs-bases-complete.json.gz \
  --output /data/image-recovery-1 --generation recovery-1

python scripts/image_campaign.py commands \
  --root /data/image-recovery-1 \
  --gateway https://sandbox.example \
  --sdk-wheel /data/ucloud_sandboxes_sdk-0.4.34-py3-none-any.whl
```

The second command prints explicit preparation commands. Run the small `bases` group first, then all `task-bases`, then the TMax and OpenSWE shared-foundation groups; the remaining TMax-inline, Terminal and source groups can run concurrently. Base inputs are deduplicated by immutable upstream identity and ordered by recipe fanout. Source commands stage admitted images into the existing registry, reusing retained blobs; foundation commands consume the exact prepared bases from both base catalogs. Missing bases remain explicit live upstream dependencies. Run the commands under the deployment's durable job supervisor. Backend admission and the builder ceiling still apply. Success means validated catalog entries, not merely accepted jobs or a zero exit from the materializer. A preparer can exit unsuccessfully after recording problematic entries while still producing useful ready artifacts.

Each loss event needs a new generation and empty output directory. Resume that same generation after ordinary interruption. New image identities and accepted-build journal names prevent old successful builds and publication records from masquerading as restored content. Old aliases remain insert-only: on an existing gateway, rewrite the actual recipe index from the new catalogs and use explicit restored image IDs/references. Existing client requests using stale upstream aliases are **not** automatically repaired by this workflow. Do not reuse an old catalog or old rewritten recipe database after volume loss.

Rebuild the actual recipe index with `plan_image_foundations.py rewrite-index` and `plan_image_pool.py rewrite-index`, then audit it with `plan_image_pool.py audit-index` and qualify the selected artifacts before resuming training. Preserve the original task contexts separately; this bundle contains dependency prefixes and upstream image inputs, not every task's complete context or the unavailable training index.

Before treating the cache as complete, run `plan_image_pool.py report --require-all-task-bases` with the full inventory and all source catalogs. It fails unless every inventoried task base is prepared, including task-specific upstream images; 100% generic-base coverage alone cannot pass. This is a catalog coverage gate, not a substitute for verifying the live artifact closure or measuring remaining recipe steps.

The index audit now counts remaining RUN/COPY/ADD work separately from base readiness. `--max-unqualified-live-builds` defaults to zero: a warm base does not qualify a remaining package installation, download, compiler invocation or arbitrary script as a small live step. Increase that allowance only for separately measured recipes. Terminal preparation also reuses the longest exact validated Dockerfile prefix from its existing catalog, or another plan/catalog passed with `--prepared-prefix-root`; canonical task inputs remain unchanged.

This is recipe recovery, not an exact binary backup. Upstream apt/pip/Conda packages and fetched Git branches are not universally locked, and upstream images can disappear. Rebuilding may produce different digests and package versions. Exact restoration needs a backup of the retained registry closure (and signing configuration), or additional package/source mirrors. Both backup storage and retained OCI/EROFS data must count toward the chosen storage policy; no hidden second multi-terabyte copy is created here.

## Storage policy

The objective is expensive live steps avoided per retained byte. Shared dependency prefixes come first, ranked by recipe fanout; cheap task files and final metadata remain as small live deltas. Source/task images are imported selectively with family coverage and reuse ordering. Counts from public datasets are a proxy until an actual training selection is available.

The recovery commands retain a 3 TB registry ceiling, a 500 GiB admission reserve and a 1,800 GiB observed-growth allowance. Full source imports stop at 1,200 GiB of observed growth, leaving 600 GiB of that allowance for shared foundations. Each group observes total registry growth; pending reservations are local to a group, so these are admission estimates, not physical quotas. OCI blobs, BuildKit cache and EROFS components all consume space; layer reuse must be measured by the union of digests. No command automatically expands storage.

## Refreshing the portable inputs

`pack` reads plans and dependency contexts, takes known source pins from per-source resolution receipts or catalogs, and deliberately omits completed-build state:

```sh
python scripts/image_campaign.py pack \
  --pool-root /data/source-pool \
  --foundation-root /data/tmax-foundations \
  --foundation-root /data/openswe-foundations \
  --foundation-root /data/tmax-inline-foundations \
  --foundation-root /data/terminal-foundations \
  --output /data/inputs-next.json.gz
```

Use original planner directories for snapshotting. Copy the new bundle off the gateway and commit it with its input counts and checksum. Never replace an existing bundle silently. After materialization, source pins are reverified against the public registry by immutable digest.

## Compact flat task-image preparations

`inputs-shared-pilot.json.gz` preserves **743 public source pins** and the unchanged 8,602 foundation contexts (SHA-256 `84747fdc570b71e1550e088df8b92581a0374604ba5f6fabeb4e1f2604a27c9b`). This includes the newly qualified Click canary and 48-source pilot. It is an input snapshot, not completed-task coverage.

`shared-anchors.json` preserves the 118 public, pinned source anchors used by the expanded compact preparation queue. Restore these original sources with a fresh recovery generation, then regenerate private shared-task plans from the new catalogs. [Compact task-image preparation](../../docs/shared-task-images.md) describes qualification, storage limits, resume and recovery. The original full-source preparer remains a fallback; blindly restoring every source in full can exhaust the storage budget.

`image_campaign.py refresh --bundle OLD --catalog READY_CATALOG --output NEW` adds public source pins without changing earlier pins or foundation contexts. It excludes runtime receipts and private prepared references, and fails on source-pin conflicts. Preserve each refreshed bundle off-host.

## October 1 metadata and layered-input snapshot

`inputs-layered-20261001.json.gz` preserves **944 public source pins**, all 35,984 source references, and the unchanged 8,602 foundation contexts. SHA-256: `f08a38a5f411716cc417fc82f988d506a4638b4fad982ea05017027207ba5ed7`. Pins are not prepared coverage. This refresh retains the original conservative 3 TB recovery policy; the running registry was explicitly expanded to 3,500 provider GB based on measured additional storage, while keeping the 500 GiB reserve. A fresh recovery should set its physical capacity and campaign limits explicitly; the commands do not resize volumes.

`source-receipts-20261001.json.gz` separately preserves **662 authenticated public OCI manifest/config receipts**, only 651,335 bytes compressed. SHA-256: `dcd5c2ba8752ec81b193c5f26930724c56fc659fd8dd664eede237a824af6f03`. Older receipts without original manifest/config bytes could not be included (473 receipt files across overlapping campaigns); their known digest pins remain in the input bundle. No blobs, private prepared references, credentials, or ready/build state are included. Full public config content is retained because its digest is part of source identity.

To restore saved metadata into a preparation plan while its coordinator is stopped:

```sh
python scripts/cache_source_receipts.py hydrate \
  --bundle image-campaigns/2026-09-30/source-receipts-20261001.json.gz \
  --root /data/fresh-source-pool --layout normal
```

The destination must already have a schema-1 `plan.json`. Use `--layout shared` for `prepare_shared_task_pool.py` work directories. Run hydration as the preparation service account. The tool verifies manifests/configs by digest, derives accounting and ONBUILD behavior from authenticated metadata, honors explicit plan pins, refuses conflicting existing receipts and requires an explicit pin when a mutable source has multiple saved digests. It never restores successful build state. Blob downloads still require upstream availability, but these sources need no additional manifest lookup.

Create future metadata snapshots from the original preparation roots:

```sh
python scripts/cache_source_receipts.py pack \
  --root /data/source-pool --root /data/shared-task-pool \
  --output /data/source-receipts-next.json.gz
```

Copy every new snapshot off-host. The layered path also requires rebuilding its public anchor with a fresh generation, recreating protected filesystem exports, and requalifying the delta; see [the preparation workflow](../../docs/shared-task-images.md). No private anchor or successful build receipt in the disposable work directories is a recovery input.


## Expanded snapshot, October 1 00:10 UTC

`inputs-expanded-20261001.json.gz` preserves all 35,984 source inputs and 8,602
foundation contexts, advancing to **1,204 source pins** (SHA-256
`0d7ac3e8c3c5bf94ccb3ace3ce288b6f8bf7db662ff45456e581c47739e6443e`).
`source-receipts-20261001T001009Z.json.gz` contains **922** authenticated public
manifest/config receipts (755,731 bytes; SHA-256
`ecfc41ce8fb27e7b755fb4596bcbf1b12fa6dee11f7b28b91363bdfbd78df581`).
Use the latest inputs in the recovery commands and hydrate source metadata while
coordinators are stopped. Existing immutable pins and context bytes remain intact.
The production volume is 3,500 provider GB; the original bundle's 3 TB recovery
policy is unchanged and no command automatically expands it.
