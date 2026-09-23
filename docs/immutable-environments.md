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
with Ed25519, and publishes its config and 256 KiB chunk blobs through the
existing OCI registry client. A separately signed environment root describes
base, optional workspace seed, ordered toolkits, original Docker config digest,
and process configuration. The source OCI image carries the root digest in
`org.ucloud.immutable-environment.v1`; its Docker config and layers stay intact.
The normal `ImageRecord.manifest_digest` records the annotated image. There is
no second image catalog. Independently authored components can be composed
without copying their file contents into every combination. The regular image
builder currently produces the base component; it does not reconstruct deleted
lower-layer paths from a merged Docker image to infer toolkit whiteouts.

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
The worker additionally requires `--environment-backend-socket`.

Install `erofs-utils` on builders. Load `erofs` and `nbd` on workers, with a
nodewide device pool sized for measured simultaneous immutable components.
Do not reconfigure an already-used NBD module/device pool. Run the following as
an independent service, using the installed qualified package:

```
python -m ucloud_sandboxes.environment_backend \
  --root /var/lib/ucloud-sandboxes/environment-io \
  --socket /run/ucloud-environment/io.sock \
  --registry-url http://managed-registry:5000 \
  --repository environments \
  --trusted-keys /etc/ucloud/environment-producers.json
```

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
    "cache_bytes": 1073741824
  }
}
```

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
64 dedicated devices; an already loaded module is never reconfigured. Component
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
* Builder: `erofs-utils` (`mkfs.erofs` 1.4 qualified). `busybox-static` and `file`
  are canary fixture tools only.
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
