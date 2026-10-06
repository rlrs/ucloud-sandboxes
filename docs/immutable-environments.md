# Immutable environments

This is an optional image adapter for the existing `OverlayRootfsManager`.
Docker remains the default adapter and rollback path. Neither the sandbox API
nor the managed model relay acquires another lifecycle owner.

The trusted builder constructs a **fresh allowlisted filesystem view** from a
completed immutable Docker image. It preserves filesystem metadata and links;
it does not publish a live workspace, checkpoint, memory file, or a snapshot
with selected paths removed. Literal `.wh.*` filenames in this mounted input
remain ordinary filenames. Actual whiteout devices and opaque directory xattrs
retain their filesystem semantics.

The builder makes an uncompressed host EROFS component, signs its chunk index
with Ed25519, and publishes its config and one image blob through the existing
OCI registry client. Workers fetch authenticated 256 KiB chunks using HTTP Range.
A separately signed environment root describes
base, optional workspace seed, ordered toolkits, original Docker config digest,
and process configuration. The source OCI image carries the root digest in
`org.ucloud.immutable-environment.v1`; its Docker config and layers stay intact.
The normal `ImageRecord.manifest_digest` records the annotated image. There is
no second image catalog. Independently authored components can be composed
without copying their file contents into every combination. The regular image
builder can share groups of original Docker layer diffs, as described below;
it never infers deleted paths from a merged image.

Workers authenticate the producer and composition before exposing filesystem
bytes. A cache miss reads a complete bounded chunk and checks its digest before
answering a block request. Cache hits are also checked, including after restart.
An interrupted reader does not cancel another reader's shared fetch. A single
nodewide bounded miss pool and read pool serve all components. Cache files are
disposable derived bytes and need no fsync. A missing or corrupt chunk produces
an I/O error, never an unverified sparse hole or zero-filled substitute.

The rootfs fingerprint includes the host EROFS backend ABI and ordered component
identities. Existing Docker fingerprints and bundle schema 1 are unchanged.
EROFS bundles use the existing schema 2 environment binding. Runtime boot
fingerprints also distinguish the chosen adapter. An old Docker worker can
still run the annotated OCI input using its original layers, but it cannot
transparently restore an EROFS checkpoint with a different rootfs ABI.

## Shared image layers (0.7.0)

Whole-image publication now first tries to publish groups of the image's
original OCI layer diffs as reusable EROFS components. Layers are grouped from
bottom to top around a 64 MiB compressed-input threshold, with at most 24 groups
(fewer when explicit toolkits need manifest slots). Completed lower groups stay
stable across related images. Small trailing base layers may join task-specific
layers. On a cache miss, publication now probes shorter group prefixes for an
already signed component, keeping that base component separate from the new
delta. Speculative lookups are limited to 16 per plan and a shared one-second
budget, and never exceed the existing group limit. Both selective extraction and
the Docker fallback use this refinement. The signed source layers, parent chain
and format must match exactly; publication refreshes each reused component's
retention before referencing it. Missing or unsupported prefixes keep the
ordinary grouping behavior.

A component's identity binds its ordered diff IDs, parent ChainID, layout, mkfs
version, compression and exclusions. The signed v2 schema is `ucloud-environment-erofs-v2`.
The parent is essential because squashing may remove whiteouts that hide nothing
below the group. Publication checks that the groups reconstruct exactly the
source image's ordered diff IDs, ignoring empty-tar layers. Existing signed v1
whole-image components still load. Unsplittable inputs and local conversion
failures fall back to whole-image publication; registry transport failures remain
errors rather than triggering more upload work.

### File mtimes (layout 2)

Layout 1 runs `mkfs.erofs -T 0`. With erofs-utils' default `--all-time`, that
sets every file's mtime to 0. Python checks a timestamp-based `.pyc` against its
source's mtime, so every module imported from such an image is recompiled, and
its new `.pyc` lands in the sandbox's writable layer. On the scientific stack the
first import took 3.74 s instead of 0.86 s and wrote 943 files (26 MB); see the
[RL-scale spikes](benchmarks/rl-scale-spikes-2026-10-01/README.md).

Layout 2 runs `mkfs.erofs -T 0 --mkfs-time --MZ` (erofs-utils 1.9 or later). The fixed
time applies only to the build time, and each file and symlink keeps the mtime
from its layer tar header. A file whose mtime differs from the build time needs
an extended inode, 32 bytes more than a compact one. The builder sets directory
and whiteout times to 0 in every view it owns: squashed groups, selective
extractions, prepared views and allowlisted views. Their bytes then depend only
on the layers. Two views it only borrows keep Docker's times: a single-layer
group read straight from Docker's diff directory, and the merged rootfs of
whole-image publication. Docker takes directory times from the tar headers, but
uses the creation time for each diff root, any parent it creates implicitly and
each whiteout. Those images can therefore differ between builders. File times,
and with them `.pyc` validity, are the same on every path.

`--MZ` (C2.12) writes every inode and directory block into one metadata zone
instead of interleaving them with file data. Attach-time [metadata
hints](#metadata-hints-c22) then cover few chunks. On a 13.5k-inode scientific
rootfs, the metadata spans 37 instead of 272 chunks of 256 KiB. The whole hint
fits the 32 MiB attach budget, and `find` after attach makes no remote reads
instead of 144 ([qualification](benchmarks/rl-scale-qualification-2026-10-02/README.md)).
The zone sets no feature bit (`compat` and `incompat` are unchanged), so
workers need no change, and the walker is qualified on zoned images. Rebuilds
stay byte-identical. mkfs stages the zone in an unlinked `$TMPDIR` file; the
builder points `TMPDIR` at the build's own scratch directory.

Workers and gateways accept layouts 1 and 2; any other layout is rejected.
Builders write layout 2 only when `immutable_environments.preserve_mtimes` is
`true` (default `false`); the builder bootstrap then requires a `mkfs.erofs` with
`--mkfs-time` and `--MZ`. The layout is part of the group key, so layout-2 groups never
reuse layout-1 components, and existing layout-1 components and roots stay
valid. A whole-image (v1) component records no format. Every EROFS reader
handles both layouts, so it follows the same switch without a schema
change. The shared-image qualification checks whole-second file and symlink
mtimes against the source under layout 2.

Roll out in two steps. First, deploy a release that reads layout 2 to every
worker, gateway and builder, with the flag off. A layout-2 component would make
an older worker fail to load the image. While the flag is off, rendered configs
omit it, so a rollback to the previous release still reads them. Second, once
no older worker or gateway remains, set the flag and replace or re-bootstrap the
builders. Then republish the images that import Python, which is nearly all of
them, by building or preparing them again; their catalogs then pin the new
annotated digests.
Republish base and foundation images first, so task images share their new base
components. Images that are not republished keep their layout-1 attachments and
run as before.

A `layer-*` tag indexes reusable components; the signed root remains authoritative.
Reuse re-publishes the tag before a new root references it, and reference retention
checks that refresh again before deleting a candidate. Shared components remain
live while any protected root needs them. Physical blob collection separately
stops registry writers and holds an exclusive startup fence; see
[registry collection](managed-registry.md#blob-sweep).

Workers mount each distinct component once. Images sharing a component share its
NBD device, verified chunk cache and EROFS mount. Ordered relative lower paths
keep OverlayFS mount options within the kernel's size limit. Fresh workers load
1,024 NBD devices; an already loaded pool is not resized. Device exhaustion is a
capacity error, and failed partial compositions release unused exports under
exclusive component leases. Kernel dependencies retain components still used by
other images. Operation metrics expose device totals and mounted composition use;
they are not a reservation of every device needed by future images.

The [Hetzner qualification](benchmarks/per-layer-erofs-2026-09-27/README.md)
compares a base and two derived images with real Docker overlay2 trees, exercises
a live guest on the layered image, and checks shared-component collection and
zero-upload reuse. Run on a disposable Linux host with Docker overlay2,
`busybox-static`, `erofs-utils`, EROFS/NBD modules and the qualified runsc:

```sh
sudo modprobe erofs
# Only on a fresh qualification host with no existing NBD users:
sudo modprobe nbd nbds_max=1024 max_part=0
sudo PYTHONPATH=. python3 runtime/storage_native/qualify_environment.py \
  --runsc /path/to/runsc --layers --output /tmp/environment-qualification.json
```

The scenario uses an in-process HTTP registry. Its layer-planning sizes come from
local diff files rather than compressed registry tars. It qualifies filesystem
correctness and component reuse, not production network throughput or high-load
wake latency. It does not change memory checkpoint parking or restoration.

## Chunk-store images (C2.13, M1)

[`chunk-store-design.md`](chunk-store-design.md) replaces per-image EROFS
components with Nydus RAFS v6 images whose 256 KiB chunks are stored once, by
the sha256 of their uncompressed bytes, in S3. Milestone M1 ("store core") is
in the package and stays off unless `immutable_environments.chunk_store` is
configured. Without the block, rendered configs, node init and gateway
behaviour are unchanged; the `ucloud-sandbox-chunk-index.service` unit is
installed but disabled.

* **Formats** (`chunk_store.py`): packs (at most 64 MiB, named by sha256,
  address-ordered data and a footer sorted by chunk id), the signed chunk map
  (`ucloud-chunk-map-v1`, device offset to chunk id) and the unsigned locator
  (chunk to pack range). zstd runs through the node's `libzstd` (ctypes), or
  Python 3.14's `compression.zstd`; no package dependency is added.
* **Signed root.** A new component kind, `ucloud-environment-rafs-v1`, signed
  under `ucloud.immutable-environment-rafs.v1\0`, binds the bootstrap and
  chunk-map digests and sizes, the device size, the format and the diff IDs.
  Its manifest in `environments` has `layers: []`; the root schema is
  unchanged. Layout `image` is one merged bootstrap bound to the OCI config;
  layout `layer` is one bootstrap per layer with overlayfs whiteouts, stacked
  like today's layer components and shared across images.
  `mount_granularity` picks the layout for new conversions (S12 decides);
  workers read both.
* **`ucloud-chunk-index`** (`chunk_index.py`, `serve-chunk-index`) runs on the
  gateway (decision 5). It keeps the SQLite index of design §1.3, answers batch
  lookups, commits (after HEAD and footer checks of each pack), layer claims
  and root registration, and serves each registered component's locator with
  SigV4-presigned GET URLs. Only it and builders hold the S3 key
  (`/etc/ucloud-sandboxes/chunk-store.env`); writes need its write token and
  locators its read token, which it creates on first start.
* **Converter** (`convert-environment`): per layer, `nydus-image create`
  (v2.4.5, `--fs-version 6 --digester sha256 --compressor zstd --chunk-size
  0x40000`, no chunk dictionary; never `--repeatable`, which writes every
  owner as 0:0) on the layer read from our
  registry with its digest and diff ID checked. It verifies each chunk the
  index does not know, packs, PUTs, commits; then merges (layout `image`),
  uploads the bootstrap and map, signs, verifies, publishes the components,
  registers them, and publishes the root last. Every object is content
  addressed, so a crash leaves nothing visible and a rerun yields the same
  root digest. `--verify-device` mounts the result through the worker's own
  device and compares the whole tree with the OCI layers (root, Linux 5.16+).
  `--attach-tag` also tags a copy of the image manifest annotated with the new
  root, so workers can run it before M2's dispatched roots; the source tag
  and its digest are never rewritten.
* **Worker read path** (`environment_rafs.py`): `serve-environment-io
  --chunk-index-url --chunk-index-token-file` (rendered by node init when the
  block is set) fetches the locator, then the bootstrap and chunk map, and
  verifies both before any ioctl. A block read binary-searches the map; holes
  read as zeros. A miss fetches one window of up to 1 MiB of the same pack,
  verifies every chunk after decompression and installs its neighbours.
  The node cache is keyed by chunk id, so images share it. A 403 or a chunk
  that does not verify refetches the locator once, then fails with EIO. Traces
  record chunk ids and replay as pack ranges of up to 4 MiB. Misses use 32
  slots instead of 8.
* **Concurrent attach.** The backend attaches each component in its own single
  flight; its guard covers only device selection and its maps, for EROFS
  components too.
* **Rollback** (`unpack-environment`): rebuilds the blobs from verified chunks,
  runs `nydus-image unpack` on a private copy of the bootstrap, and pushes a
  one-layer OCI image without the old root annotation. Layout `image` only.

* **Store node (C2.6)** (`chunk_store_node.py`): with
  `chunk_store.store_node`, workers read packs, bootstraps and chunk maps only
  from `ucloud-chunk-store` on the private network (`--chunk-store-url`, the
  same read token), a read-through NVMe cache over S3 that fills aligned
  extents, coalesces and hedges misses, and takes prefetch jobs; the index can
  move there too (`serve_index`). Workers fail closed: no S3 fallback. See
  [chunk-store-design.md](chunk-store-design.md#c26-store-node-as-built) and
  [hetzner.md](hetzner.md#chunk-store-node-c26).
* **nydusd (C2.1)** (`environment_nydusd.py`): with `chunk_store.nydusd`
  (`{"path", "sha256"}`, needs `store_node`), each RAFS image is served by one
  stock `nydusd nbd` (v2.4.5 built with `block-nbd`) on its device, reading the
  store node's virtual blobs with the read token. Every chunk is checked against
  the digests that the signed bootstrap's TOC pins. One filecache serves every
  daemon on the node. The backend refuses a binary that does not match its
  sha256. EROFS components keep the Python export, and our cache, prefetch and
  traces do not see nydusd's reads. Images must be converted with
  `--nydusd-blobs`. See
  [benchmarks/nydusd-spike-2026-10-03](benchmarks/nydusd-spike-2026-10-03/README.md).

M1 does not include GC (M3), the `image_roots` dispatch (M2) or builder
automation: conversion is an explicit command.

## Lifetime and ownership

The artifact I/O backend is a **separate nodewide process**, reached only through
a mode-0600 Unix socket with peer-UID checks. It serves read-only NBD devices and
shared EROFS mounts. It owns physical I/O resources, not sandbox identity,
scheduling, or registry retention. Agent and storage-frontend restarts must not
restart this backend. The selected image adapter holds filesystem leases until
the ordinary sandbox registry commits its durable image identity.

The gateway protects environment roots and component manifests with the same
`RegistryUsageStore` owner leases used for images and checkpoints. Immutable
protection tags keep the OCI registry's native blob collector aware of each
manifest's config/chunk closure. Dependency owner rows persist the exact old
closure. Release removes that owner, without re-reading a tag that may have
changed, and preserves other owners. Failed or uncertain release retains data.

Two kernel details are deliberately covered by real Linux qualification:

* `NBD_SET_SOCK` supports multiple connections; it is not an exclusive claim.
  The backend holds an exclusive device-inode flock for the entire export and
  checks for an existing kernel owner before any configuration. Use a dedicated
  nodewide NBD pool, with no unrelated non-cooperating allocator.
* Unmounting an OverlayFS lower can succeed while another overlay retains its
  superblock. Before disconnecting a component, the backend checks kernel mount
  dependencies, including bind mounts. Component leases fence concurrent
  composition/GC. The rootfs adapter also checks dependencies before collecting
  a composed lower. All services must share the host mount namespace.

If the artifact backend dies with retained mounts, its replacement refuses to
adopt them or silently rebind new devices underneath them. Affected sandboxes
must be fenced and drained, then their owned mounts removed before restart.
Ordinary frontend replacement leaves the backend and running guests intact.
The backend is not a transparent high-availability filesystem service.

## Explicit qualification setup

No production deployment enables this adapter automatically. The CLI accepts
these optional bootstrap settings on the gateway, builder, and worker:

```
--environment-registry-url http://managed-registry:5000
--environment-registry-repository environments
--environment-trusted-keys /etc/ucloud/environment-producers.json
```

The trust file is a nonempty JSON mapping of `sha256:<raw-public-key-hash>` to
base64 Ed25519 public key bytes. It must be owned and not writable by others.
The builder additionally requires `--environment-signing-key` (a private,
owner-only Ed25519 PEM) and repeated `--environment-allow-path` entries. These
paths are trusted build policy, not request-provided access to runtime state.
`--environment-preserve-mtimes` publishes layout-2 components
([file mtimes](#file-mtimes-layout-2)). The worker additionally requires
`--environment-backend-socket`.

Install `erofs-utils` on builders. Load `erofs` and `nbd` on workers, with a
nodewide device pool sized for measured simultaneous immutable components.
Do not reconfigure an already-used NBD module/device pool. Run the following as
an independent service, using the installed qualified package:

```
ucloud-sandboxes serve-environment-io \
  --root /var/lib/ucloud-sandboxes/environment-io \
  --socket /run/ucloud-environment/io.sock \
  --environment-registry-url http://managed-registry:5000 \
  --environment-registry-repository environments \
  --environment-trusted-keys /etc/ucloud/environment-producers.json
```

Add `--disable-prefetch` to turn off attach-time prefetch
([below](#off-switch)).

Its service must use the host mount namespace (`PrivateMounts=no`), start before
the selected worker adapter, and remain independent of node-agent/frontend
service restart propagation (`PartOf` those services is inappropriate). Stop it
only after draining its users. Do not activate it by merely replacing a live
Docker worker's adapter: qualify a fresh worker and its runtime/checkpoint ABI.
The normal deployment config below installs the independent service and forwards
owned key material. Migration destinations must advertise the exact existing runtime
compatibility hash retained in the checkpoint (including the rootfs ABI); legacy
attached Docker peers remain compatible, while unknown cold destinations are excluded.
A fleet rollout still needs qualified OS/module closure, image coverage, and measured
256/512-sandbox concurrency/device/cache sizing. No full rollout
or network-load latency claim is made by the local qualification below.

## Qualification

Run on an isolated Linux host as root with the pinned five-patch gVisor bundle
(including its companion binaries), `busybox-static`, `erofs-utils`, and the NBD
and EROFS modules loaded:

```
python runtime/storage_native/qualify_environment.py \
  --runsc /path/to/qualified/runsc --output /tmp/environment-qualification.json
```

The script creates its own private fixture and OCI HTTP registry, launches the
artifact backend in a separate process, creates a composed lower and writable
sandbox through the canonical manager from a separate frontend process, runs a
live gVisor guest while replacing that frontend, and validates whiteouts,
opaque directories, hardlinks, xattrs, copy-up, retention, and explicit fencing
after backend loss. It removes only its own mounts and reports cleanup errors.
The fixture contains many unrelated files, so passing also requires demand reads
to transfer less than one eighth of the complete artifacts.

`--prefetch` publishes each component with its signed metadata hint and runs
the backend with its metrics exported and an 8 s trace window. Every
component must attach with its hint present, and both startup traces must be
recorded. After the backend-loss check, it attaches again on a fresh backend
and chunk cache that hold only those traces, and a second live guest must run
with the traces replayed. The 2026-10-02
[qualification](benchmarks/rl-scale-qualification-2026-10-02/README.md) found
zero demand misses on that replay.

The recorded [Linux result](benchmarks/immutable-environment-2026-09-23/qualification.json)
passed with 69,312,512 artifact bytes and 2,391,363 HTTP blob bytes read (3.45%).
Cold materialization, including a new Python frontend process, took 386 ms;
frontend replacement took 201 ms and downloaded zero additional blob bytes.
These numbers include a local HTTP fixture and are **not** WAN results, a 512-way
capacity qualification, or checkpoint-wake latency.

## Normal deployment and canary

The optional top-level `immutable_environments` deployment configuration is absent
by default. Its presence enables gateway dependency protection; worker and builder
adapters require their separate explicit booleans. All key paths below are owned
controller-side paths. The normal autoscaler bootstrap distributes public trust to
workers and the signing key only to opted-in builders. Neither secrets nor private
key bytes belong in deployment JSON. Run provisioning as the controller service account (`ucloud`), using the qualified launcher:

```sh
"$UCLOUD_AGENT" provision-environment-key --directory /home/ucloud/.local/share/ucloud-environment-producer
```

The command prints the public producer identity and file paths, never private
material. Repeating it recovers the same key; missing/mismatched private material
requires explicit rotation. A canary configuration is:

```json
{
  "immutable_environments": {
    "trusted_keys_file": "/home/ucloud/.local/share/ucloud-environment-producer/producers.json",
    "signing_key_file": "/home/ucloud/.local/share/ucloud-environment-producer/producer.pem",
    "repository": "environments",
    "worker_enabled": true,
    "builder_enabled": true,
    "allow_paths": ["bin", "etc"],
    "cache_bytes": 1073741824,
    "preserve_mtimes": false,
    "prefetch_enabled": true
  }
}
```

`preserve_mtimes` selects layout-2 publication; see
[file mtimes](#file-mtimes-layout-2) for when to turn it on.

These two allowlisted paths describe the tiny canary below, **not** a policy for
arbitrary application images. Use a dedicated canary deployment/worker selection;
all images admitted to an EROFS worker must have trusted attachments. Before a
fleet switch, publish the complete immutable allowlist for every supported image.
Docker workers can still run the annotated OCI layers. Existing live workers
cannot switch adapters; bootstrap explicitly refuses both directions. Replace
and drain workers for rollback. A new Docker worker remains the supported rollback.

The installer creates `ucloud-environment-io.service` independently of agent and
storage frontend restarts, sharing the host mount namespace. Reinitialization
starts an existing backend without restarting it. A newly loaded NBD module gets
1,024 dedicated devices (one per distinct mounted component); an already loaded
module is never reconfigured. Component
cache bytes consume at most half of existing reserved disk headroom, leaving the
other half for safety rather than silently increasing writable admission.

On the qualified builder, provision/copy the same producer files with owner-only
private-key permissions, then run the ready-made canary build. Set these explicit
values for the target deployment (use a unique owned tag):

```sh
export UCLOUD_AGENT=/path/to/qualified/bin/ucloud-sandboxes
export CANARY_IMAGE_REF=managed-registry:5000/canary/environment:qualification-1
export ENVIRONMENT_REGISTRY_URL=http://managed-registry:5000
export ENVIRONMENT_TRUST_FILE=/etc/ucloud-sandboxes/environment/producers.json
export ENVIRONMENT_SIGNING_KEY=/etc/ucloud-sandboxes/environment/producer.pem
export ENVIRONMENT_BUILD_ROOT=/var/lib/ucloud-environment-canary
bash scripts/build_environment_canary.sh
```

The script builds a fresh scratch image containing the host's `busybox-static`
fixture, pushes it, and invokes the same `publish-environment` implementation as
the normal image builder. Its final JSON records the pinned annotated image ref,
manifest digest, signed environment root, and component digests. Use the pinned
ref in the existing sandbox API and expect `CANARY_OK`. Private producer key
ownership is the release operator's responsibility; public trust goes to gateway,
worker and builder, while private key material stays on controller/builder.

Package additions compared with the frozen pre-P4 bundle:

* Python: locked `cryptography` plus `cffi`/`pycparser` wheels matching the node's
  Python ABI. Replacing only the project wheel does not add this dependency closure.
* Builder: `erofs-utils` (`mkfs.erofs` 1.4 qualified; layout 2 needs 1.9 or later
  for `--mkfs-time` and `--MZ`). `busybox-static` and `file` are canary fixture tools only.
* Worker: kernel-version-matched `erofs` and `nbd` modules/dependencies, existing
  `util-linux` mount tools and `kmod`. There is no `nbd-client` daemon dependency.

The bundle repacker deliberately preserves Debian/module closure. A fresh package
must include those artifacts explicitly before enabling the optional feature;
shipping the source with both booleans disabled needs no new OS modules. The
local 5.15-kernel fixture is not qualification for a different production kernel.

The follow-up [production-kernel qualification](benchmarks/immutable-environment-2026-09-23/qualification-kernel7.json)
also passes on Linux `7.0.0-30-generic`, `erofs-utils` 1.9, and the final qualified
runsc SHA `a005058b5a097a9ec6c28d0d3e14ea7d2992fc0cf2c11a0eb62d7553e7068613`.
It found and fixed a kernel-dependent NBD startup race: the adapter now waits for
both owned kernel attachment and published capacity before returning a mountable
device. This run read the same 3.45% of artifact bytes, cold materialization took
514 ms and frontend replacement took 327 ms with no extra blob transfers. All
filesystem semantics, GC retention, explicit backend-loss fencing, and scoped
cleanup passed. These remain local HTTP fixture timings, not fleet/wake results.

For a bundle extension, `scripts/repack_node_bundle.py --extra-runtime-deb FILE`
(repeatable) adds explicitly qualified packages, checks target architecture and
refuses to replace an existing qualified package version or payload. Provide only
missing packages from the closure; never silently upgrade the baseline OS.
`--kernel-module-dir` replaces the full module closure, so merge the two additional
EROFS/NBD modules with the existing qualified module directory first. Both paths
refresh the same verified bundle manifest; required default package/module lists
remain unchanged and the optional bootstrap activates the extra modules explicitly.
Artifact builders run as root only when this adapter is selected, because their
canonical immutable Docker mount view and metadata-preserving copy require it.

### Heterogeneous first-use qualification

The [four-image result](benchmarks/immutable-environment-2026-09-23/heterogeneous-images.json)
uses an isolated four-vCPU, 12 GiB Linux 7.0 worker, the same exact qualified
six-patch runsc, and freshly published Python/Node slim and tool images. Each
contains the same pinned Verifiers repository. Python exercises TLS setup,
SQLite, repository hashing, subprocess execution and writable copy-up; Node
exercises repository hashing, subprocess execution and writable copy-up. All 32
live guests returned matching proofs; image GC, component release and scoped
mount cleanup completed without errors. The 60 environment contract tests also
pass on that Linux VM.

The test found a missing case in the original tiny composition qualification:
Linux rejects read-only OverlayFS with only one lower. A single-component image
now binds its already read-only EROFS filesystem; multiple components keep the
ordered OverlayFS composition. Image-view removal fences OverlayFS users, while
only the backend disconnects devices and must still account for every retained
bind. Regression tests cover that ownership distinction, and all four images
exercise the corrected path through the canonical rootfs manager.

These are medians of two alternating-order trials, in seconds to the first
verified guest result, including cold registry materialization. They are not
percentiles or relay wake measurements. Every cold trial has a new private Docker
image store or empty verified chunk cache. Both use the same local HTTP registry;
host page caches remain intact.

| Image | EROFS size | Cold Docker | Cold EROFS | Cold transfer Docker / EROFS | EROFS fraction read |
| --- | ---: | ---: | ---: | ---: | ---: |
| Python slim | 117 MiB | 1.81 s | 1.16 s | 44 / 38 MiB | 32.2% |
| Node slim | 219 MiB | 2.13 s | 1.29 s | 78 / 67 MiB | 30.6% |
| Python tools | 973 MiB | 8.43 s | 1.29 s | 367 / 50 MiB | 5.2% |
| Node tools | 1,082 MiB | 8.41 s | 1.27 s | 392 / 69 MiB | 6.4% |

Warm EROFS results were 0.15–0.30 seconds, versus Docker's 0.16–0.33 seconds,
with no blob transfers in either path. The large tool images saved 82–86% of
network bytes compared with compressed Docker layers and about 6.5× cold startup
time. Slim images saved only 15% of transferred bytes; their larger fraction of
actually used content makes the advantage smaller. EROFS here is uncompressed,
so percentages of EROFS bytes and percentages of compressed OCI transfer are
intentionally different denominators.

The backend process ended at 44–51 MiB RSS versus the private dockerd's 88–89 MiB.
Those are process observations, not total deployment/guest memory: they exclude
filesystem page cache and Docker's shared containerd. The runsc command's peak
RSS is recorded separately. No density claim follows from these numbers. Builder
publication took 7–58 seconds per image and is outside worker startup; its serial
verified chunk publication is a remaining builder-side optimization opportunity.

Run `runtime/storage_native/benchmark_environment_images.py` as root only in an
owned disposable Linux VM, with the qualified runtime, Docker overlay2,
erofs-utils and the EROFS/NBD modules. Supply `--runsc`, `--repo`, a new `--root`
and `--output`. The script creates a private local registry and per-trial adapter
stores, retains signed image/base identities, and removes its containers and
mounts. The recorded evidence also includes the exact measured source hashes. `--resume-prepared` reuses that fixture's
four signed publications after an interrupted qualification, but always creates
new adapter caches. Private signing material remains only in the disposable
fixture; never copy its keys into a production deployment.

This qualifies real heterogeneous first use and fixes the single-component
activation blocker. It does not qualify 256/512 simultaneous cold image misses,
NBD device exhaustion, registry WAN latency, or the production image inventory.
Keep fleet activation behind the existing explicit worker/builder switches until
all supported production images have trusted attachments and a canary at the
intended concurrency confirms bounded cache/device use. Docker remains the
supported adapter for images without those attachments and for fresh-worker
rollback; no live worker changes adapters in place.

### Transient immutable reads

Verified chunk misses use the existing RegistryClient and remain coalesced
across readers. Transient transport failures and HTTP 408/429/500/502/503/504
retry with a shared 30-second fetch budget and bounded exponential backoff.
Permanent responses, TLS verification failures, and content identity failures
are not retried. Cancelling the last reader cancels further attempts; cancelling
one reader does not cancel a sibling's shared fetch. Cancelled work continues
occupying its miss slot until its current bounded transport operation exits.

This is a retry budget, not an absolute network deadline or a queue deadline.
Public `read1` checks the monotonic budget between bounded body reads; an
individual blocking read can overshoot by the remaining socket timeout, and
urllib retains its existing DNS/header/framing behavior. Expired or cancelled
fetches never install verified cache content. Linux Python 3.13 qualification
passed 58 cache/backend/rootfs/registry tests, including real HTTP transient
failure recovery, slow-trickle expiry, permanent errors, corruption, shared
cancellation, cancellation after temporary-file write, and shutdown during backoff.

## Attach-time prefetch: metadata hints and startup traces

Two hints warm a component's chunk cache when the artifact backend attaches
it. Both are prefetch orders only. Every fetched chunk is still verified
against the signed index, and a missing, unsupported or failing hint means
ordinary demand loading. A hint never fails an attach.

### Metadata hints (C2.2)

At publication, `erofs_metadata.py` walks the EROFS image. It is a strict
reader of the on-disk format in `fs/erofs/erofs_fs.h`. It returns the bytes
that path walks, `stat`, `readlink`, xattr reads and first data maps can
touch:

* block 0 (superblock and compression configs);
* each inode record with its inline xattrs, tail-packed inline data and
  compressed or chunk index array;
* shared xattrs;
* directory and symlink blocks.

It never reads regular-file data. It refuses unqualified features and
layouts instead of guessing. Refused features are DEVICE_TABLE/COMPR_HEAD2,
ZTAILPACKING, FRAGMENTS/DEDUPE, XATTR_PREFIXES, 48BIT, METABOX, unknown
compat bits, non-4 KiB blocks, extra devices, chunk-based or compressed
directories and symlinks, and fragment or inline pclusters. A refused image
publishes the unchanged hint-free manifest.

The builder signs a hint with the producer key. Its domain is
`ucloud.immutable-environment-metadata.v1`. The hint lists `(chunk index,
metadata bytes)` for every 256 KiB chunk holding metadata. It is bound to the
signed config digest, the image digest and the chunk count.

**Why an annotation.** The hint is the `org.ucloud.environment.metadata.v1`
annotation on the component's OCI manifest, for these reasons:

* Workers parse the signed config with an exact key set, so a new signed
  field would make every older worker reject the component.
* Workers also require `layers` to be exactly the image blob or the
  per-chunk list, so a referenced blob is ruled out as well.
* Workers ignore manifest annotations, yet the manifest digest still binds
  them.
* The annotation adds no blob, so OCI blob collection, protection tags and
  owner leases are unchanged.

The worker parser at 90ce959 loads hinted manifests. The new parser loads
older manifests and reports their hint as absent. A builder that reuses a
`layer-*` tag re-puts exactly the bytes it found. A registry object keeps the
verified hints of only the 32 most recently loaded manifests: the backend
reads a hint right after its own load, and a large hint holds about 2 MiB.

Builders of different versions that race on a new layer group may publish
two valid manifests over the same EROFS blob. These share the verified chunk
cache, but not the device or mount. Annotations are bounded to 16,384 chunks,
and an oversized hint keeps the densest chunks with `complete: false`.

On attach, the backend starts a metadata job concurrently with the mount, so
the superblock read joins the first bulk range:

* The job selects the densest chunks that fit its budget, always chunk 0,
  and fetches them in index order so adjacent chunks share a range.
* It is bounded by `PrefetchPolicy.metadata_bytes`, 32 MiB by default, and a
  30 s deadline.
* `ensure` waits at most `metadata_wait_seconds`, 5 s by default, after the
  mount. Other callers of `ensure` for that component wait until the same
  moment.
* The RPC server runs one thread per request. The backend guard still
  serializes attach and drop, but one attach's metadata wait never delays
  another component's `ensure` (each composition's liveness check) or `drop`.

### Startup traces (C2.3)

The first attach of a component with no stored trace records the ordered set
of chunk indices the guest first reads, at the cache layer. The window is
bounded to 30 s or 2,048 chunks. The trace is saved atomically, with no fsync,
as `<backend root>/traces/<image digest>.json`. The store keeps the 4,096
most recently written traces. It is untrusted hint data: invalid, stale or
foreign files are deleted and recorded again. A detach before the window
closes ends it early and saves what was read, since deleting a sandbox drops
its components at once; a failed attach saves nothing.

Later attaches replay the trace in the background, with lower priority than
metadata. The replay sorts the trace into runs ordered by first touch and is
bounded to 256 MiB and 120 s. `LocalTraceStore` (`load`/`save`) is the seam
for cross-node distribution (C2.7).

### Scheduling bounds

* Bulk reads are single-attempt HTTP ranges of at most 16 adjacent chunks
  (4 MiB). They run on the existing nodewide miss pool.
* Prefetch holds at most a quarter of the miss slots. It never starts a range
  while a demand miss waits for a slot.
* Each job is capped at a quarter of the chunk cache. Trace replays stop
  once unfinished jobs together have scheduled half of it. Metadata counts
  toward that share but is bounded only by its own budget, so a replay never
  starves a later attach's metadata.
* A reader whose chunk is in an in-flight bulk read joins it. If that read
  fails, or has not finished within half the fetch timeout, the reader
  fetches the chunk itself. Joining and the fallback share one fetch timeout,
  which matches the kernel's 30 s NBD request timeout.
* Detach and close cancel jobs. Cancelled jobs install nothing, but verified
  bytes still answer readers that already joined.

### Off switch

`immutable_environments.prefetch_enabled` is a strict boolean and defaults to
`true`. Set it to `false` to stop both hint kinds: bootstrap then starts
`serve-environment-io --disable-prefetch`, and the backend only demand-loads.
Startup traces are then neither replayed nor recorded.

Bootstrap writes the setting into the backend's service, and the backend
reads it once at start. Bootstrap never restarts a live backend, because that
would detach mounted filesystems. A change therefore applies to newly
provisioned workers; replace workers to roll it out. To see the mode a node
actually runs, read `prefetch_enabled` in its heartbeat (next section).

### Idle image cache

Deleting a sandbox leaves its image's composition mounted, with its components
attached, for the next sandbox of that image. Materializing it again would
cost a backend attach per component, an overlay mount and a receipt
(`image_resolve` p50 0.3-0.5 s and p95 2.5-3.6 s when every delete collected
it). The agent's deletion reconciler (every 5 s) runs a sweep that collects
unregistered compositions, least recently used first, only while the node is
over budget:

* attached components exceed `immutable_environments.device_budget_percent`
  (default 75) of the node's block devices, 768 of the 1,024 VM init loads; or
* nydusd's shared device cache is over `cache_bytes`. Idle images keep their
  components referenced, so the backend cannot detach them itself.

An attach that finds every device taken runs the sweep at once and retries
once. The sweep skips any image with a registration, an overlay user or a
lease (a create, a materialization or a noded create holds one), and any image
used in the last 60 s. "Used" means leased, or a sandbox of it was created
(runtime/noded's creates included, at finish) or deleted. After an agent
restart, reconciliation keeps a mounted composition whose I/O answers and
collects the rest; the receipt's write time orders the sweep until the image
is next used.

### Metrics

`EnvironmentBackend.metrics()` merges the cache metrics with the attach
counters. `downloaded_bytes` now also counts prefetch bytes; `misses` counts
only demand misses.

The `metrics` RPC (`{"method": "metrics"}`) returns this map. Each worker
heartbeat carries it as `runtime_metrics.environment_io`, which the gateway
shows in each node's `actual_usage`:

* The RPC uses a separate counter lock, not the attach guard, and a 1 s
  timeout, half the gateway's 2 s wake heartbeat read. A slow registry load
  or mount never delays a heartbeat; a stalled backend delays it by about 1 s.
* The map must hold exactly `models.ENVIRONMENT_IO_METRICS`. Counters are
  non-negative integers, `*_seconds` totals are floats and
  `prefetch_enabled` is a boolean.
* The field is `null` on Docker workers and while the backend is
  unreachable or reports other names. The backend outlives agent upgrades, so
  an older backend rejects the method until the worker is replaced.
* Gateways must be upgraded before workers. An upgraded agent always sends
  the field, `null` included, and like every new runtime metric an older
  gateway rejects such a heartbeat.
* The gateway also rejects a map with any other set of names. Adding or
  renaming a counter therefore needs gateways that accept the older set
  first.

| Group | Counters |
| --- | --- |
| Cache | `hits`, `misses`, `downloaded_bytes`, `corruptions`, `fetch_retries`, `cached_bytes`, `pending_misses` |
| Backend | `active_components`, `prefetch_enabled` |
| Prefetch, per kind (`metadata` or `trace`) | `{kind}_prefetch_jobs`, `_chunks`, `_bytes`, `_failed_chunks`, `_skipped_chunks`, `_truncated`, `_seconds` |
| Prefetch, shared | `prefetch_joined_reads`, `prefetch_jobs_active`, `prefetch_ranges_inflight` |
| Hint status | `metadata_hint_present`, `_absent`, `_unsupported`; `trace_hint_present`, `_absent`, `_invalid` |
| Trace recording | `trace_recordings_started`, `traces_recorded`, `trace_chunks_recorded` |
| Attach | `metadata_prefetch_wait_timeouts`, `prefetch_start_failures` |

Build history records these publication counters:

* `metadata_hints`
* `metadata_hint_unsupported`
* `metadata_hint_chunks`
* `metadata_hint_bytes`
* `metadata_hint_ms`

### Qualification and measured coverage

`tests/test_erofs_metadata.py` proves completeness on real `mkfs.erofs`
images built with the builder's exact options. The images cover:

* deep trees and many small files;
* multi-block directories;
* hardlinks;
* inline, medium and long symlinks;
* inline and shared xattrs;
* opaque directories;
* whiteout devices, FIFOs and sockets;
* compressed files of many logical-cluster counts.

The same checks run on images built with extended inodes, legacy indexes,
big pclusters, chunked files and no compression, and on the builder's layout 2
when mkfs is 1.9 or later (older ones leave it out). Three independent
checks must pass:

1. Every byte that `fsck.erofs --extract` and `dump.erofs --nid -e` read
   (traced with strace), minus the regular-file extents dump.erofs reports,
   lies in a walker block.
2. With everything else overwritten, fsck passes and every inode dump is
   identical.
3. On the overwritten image, a separate reader returns the source tree's
   names, lstat fields, symlink targets and xattrs.

Deliberately dropping shared xattrs, directory blocks, inline data, or eight
bytes of compact or legacy indexes each fails at least one check.

The images were built locally with erofs-utils 1.4 (lz4) from Docker
exports:

| Image | EROFS | Metadata | Metadata blocks | Chunks touched | 80% / 90% of metadata bytes in |
| --- | ---: | ---: | ---: | ---: | ---: |
| ubuntu:26.04 | 62 MiB | 3.4 MiB (5.6%) | 898 | 67 / 247 (27%) | 5.2 / 7.0 MiB |
| postgres:17 | 249 MiB | 7.2 MiB (2.9%) | 1,886 | 232 / 997 (23%) | 19.5 / 29.5 MiB |
| Python 3.12 scientific | 222 MiB | 8.3 MiB (3.7%) | 2,167 | 390 / 890 (44%) | 40.5 / 56.5 MiB |

mkfs.erofs 1.4 interleaves inodes with file data. Metadata is therefore a
few percent of the bytes, but it touches a quarter to almost half of the
256 KiB chunks.

Chunk-granular hints therefore rank chunks by metadata density. The default
32 MiB budget covers 100%, 92% and 73% of metadata bytes for the three
images. A complete `find /` with no remote reads needs metadata clustered at
build time, which layout 2's `--MZ` provides (see
[file mtimes](#file-mtimes-layout-2)).

These numbers come from mkfs 1.4. The walker is qualified on erofs-utils 1.9,
with and without `--MZ`, by the
[2026-10-02 qualification](benchmarks/rl-scale-qualification-2026-10-02/README.md).
Unqualified features fall back to no hint rather than an incomplete one.
`RealImagePrefetchTests` checks the budget in the Python path. On a 39 MiB
fixture, layout 1 spreads metadata over all 155 chunks, so attach prefetches
128 and later reads go remote. Layout 2 needs 6 chunks, all prefetched.

No kernel mount was qualified here. The worker gate is shown in the Python
path only: after attach, replaying every metadata range of a real image
through the cache makes zero registry requests.
