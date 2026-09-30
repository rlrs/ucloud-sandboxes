# Rebuildable image campaign: 2026-09-30

`inputs.json.gz` is a 1.3 MB recipe bundle stored in Git, independent of the registry volume. It contains 35,984 source references and the exact dependency contexts for 8,602 shared foundations: 11 OpenSWE, one explicit TMax scientific foundation, 2,786 TMax inline prefixes and 5,804 Terminal prefixes. It excludes task solutions, registry blobs, credentials, production addresses, build IDs and success receipts.

The initial snapshot contains 191 resolved source digests; the remaining references have not all been resolved or built. The preparer records additional immutable resolutions on the gateway root disk, outside `/mnt/ucloud-registry`, before building. Preserve those work directories when replacing only the volume. Refresh this bundle from those directories to preserve later pins off-host as well. A Git snapshot is only as current as its recorded inputs.

Foundation context identities and bundle checksums are verified before submission. Source dataset revisions are included in the source inventory and foundation records. Input definitions come from research-environments `c7ea0d7fe3379f0e6ffb0a68c82c80c184601f92` and verifiers-ucloud `7856cc18faceae31aa533605ea812d81b92a85ea`; actual training selection was unavailable. BIRD and NeMo remain deferred.

## Recovery after losing a registry volume

Provision an empty working managed registry and healthy deployment using the normal deployment tooling. The rebuild workflow does not provision, resize or delete volumes. Preserve signing configuration and credentials through the existing deployment recovery mechanism; neither belongs in this public-input bundle.

If only the registry volume was lost, first run `pack` below against the surviving preparation directories on the gateway root disk, so recovery includes all source pins learned since this Git snapshot. Use that refreshed bundle in place of `inputs.json.gz`.

On the gateway, use a checked-out repository containing the preparation scripts and a compatible SDK wheel. Run as the gateway service account:

```sh
python scripts/image_campaign.py materialize \
  --bundle image-campaigns/2026-09-30/inputs.json.gz \
  --output /data/image-recovery-1 --generation recovery-1

python scripts/image_campaign.py commands \
  --root /data/image-recovery-1 \
  --gateway https://sandbox.example \
  --sdk-wheel /data/ucloud_sandboxes_sdk-0.4.34-py3-none-any.whl
```

The second command prints explicit preparation commands. Execute the first two small shared-foundation groups first; the remaining TMax-inline, Terminal and source groups can run concurrently. Run them under the deployment's durable job supervisor. Backend admission and the builder ceiling still apply. Success means validated catalog entries, not merely accepted jobs or a zero exit from the materializer. A preparer can exit unsuccessfully after recording problematic entries while still producing useful ready artifacts.

Each loss event needs a new generation and empty output directory. Resume that same generation after ordinary interruption. New image identities and accepted-build journal names prevent old successful builds and publication records from masquerading as restored content. Old aliases remain insert-only: on an existing gateway, rewrite the actual recipe index from the new catalogs and use explicit restored image IDs/references. Existing client requests using stale upstream aliases are **not** automatically repaired by this workflow. Do not reuse an old catalog or old rewritten recipe database after volume loss.

Rebuild the actual recipe index with `plan_image_foundations.py rewrite-index` and `plan_image_pool.py rewrite-index`, then audit it with `plan_image_pool.py audit-index` and qualify the selected artifacts before resuming training. Preserve the original task contexts separately; this bundle contains dependency prefixes and upstream image inputs, not every task's complete context or the unavailable training index.

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
