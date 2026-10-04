"""Chunk store M2 operator tool (docs/chunk-store-m2-plan.md §5).

- ``inventory`` (step 1): a read-only list of every managed image with its
  environment root, components, sizes, build-input status and family, from
  which the waves and their predicted release are planned.
- ``convert`` (step 2, on a disposable converter): converts and full-tree
  verifies a wave's images into the chunk store, appending one result line per
  image.
- ``record`` (gateway): each verified result becomes a ``converted``
  ``image_roots`` row once its root loads with the gateway's trusted keys.
- ``switch`` / ``revert`` (step 3, gateway): dispatch a wave's new roots, and
  re-point its durable owners to the new closure; or go back to the
  annotation.
- ``status``: rows per wave and state.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
import logging
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import threading
from urllib.parse import urlparse
import zipfile

SELECTORS_NAME = "all-image-selectors.json"
_LOG = logging.getLogger(__name__)
# Plan §5: waves in order of training rows per byte. A name ending in ":" is a
# family prefix (the foundations).
WAVES = {"1": ("SWE-smith", "OpenSWE"), "2": ("TMax", "Terminal-Lego"),
         "3": ("ScaleSWE", "SWE-rebench v2", "SWE-Lego", "R2E-Gym", "MultiSWE", "foundation:"),
         "4": ("unknown",)}
# Owners a switch re-points to the new closure (plan §3.3). Routes keep the
# root their spec pinned; any other owner keeps the old closure, which blocks
# that image's release.
DURABLE_OWNERS = ("image-pool:", "image-foundation:", "shared-task:", "shared-source:")
CLI = "import sys; from ucloud_sandboxes.cli import main; sys.exit(main(sys.argv[1:]))"


def pinned_references(values):
    """(repository, digest) for every pinned image reference found in nested values."""
    from .managed_registry import manifest_digest_from_image_ref, registry_repository_tag_from_image_ref
    found, stack = set(), list(values)
    while stack:
        value = stack.pop()
        if isinstance(value, dict):
            stack.extend(value.values())
        elif isinstance(value, (list, tuple)):
            stack.extend(value)
        elif isinstance(value, str) and "@sha256:" in value:
            if value.lstrip().startswith(("{", "[")):
                try:
                    stack.append(json.loads(value))
                except ValueError:
                    pass
                continue
            coordinates, digest = registry_repository_tag_from_image_ref(value), manifest_digest_from_image_ref(value)
            if coordinates and digest:
                found.add((coordinates[0], digest))
    return found


def build_inputs(catalog_file):
    """Images a build names (prepared sources, foundations, decisions): their
    OCI manifests stay byte-identical (plan §5). Also family by foundation."""
    inputs, families = set(), {}
    if not Path(catalog_file).exists():
        return inputs, families
    db = sqlite3.connect(f"file:{catalog_file}?mode=ro", uri=True)
    try:
        for table in ("prepared_sources", "prepared_foundations", "prepared_decisions"):
            for row in db.execute(f"SELECT * FROM {table}"):  # noqa: S608 - fixed table names
                inputs |= pinned_references(row)
        for family, payload in db.execute("SELECT family, payload FROM prepared_foundations"):
            for key in pinned_references([payload]):
                families.setdefault(key, f"foundation:{family}")
    finally:
        db.close()
    return inputs, families


def selection_families(path):
    """(repository, digest) → (family, task rows) from the training selection."""
    from .managed_registry import manifest_digest_from_image_ref, registry_repository_tag_from_image_ref
    path = Path(path)
    if zipfile.is_zipfile(path):
        with zipfile.ZipFile(path) as archive:
            name = min((n for n in archive.namelist() if n.rsplit("/", 1)[-1] == SELECTORS_NAME), key=len)
            document = json.loads(archive.read(name))
    else:
        document = json.loads(path.read_text())
    families = {}
    for entry in document.get("images", []):
        reference = entry.get("prepared_reference") or ""
        coordinates, digest = registry_repository_tag_from_image_ref(reference), manifest_digest_from_image_ref(reference)
        if coordinates and digest:
            family, rows = families.get((coordinates[0], digest), (entry.get("family"), 0))
            families[(coordinates[0], digest)] = (family, rows + int(entry.get("upstream_rows", 1)))
    return families


def inventory(registry, environments, *, prefix, catalog_file, selection=None, concurrency=8, out_rows, out_summary):
    """One JSON row per (repository, manifest digest); tags that alias it are listed."""
    inputs, foundation_families = build_inputs(catalog_file)
    selected = selection_families(selection) if selection else {}
    components, layer_sizes, guard, rows = {}, {}, threading.Lock(), []

    def component_bytes(digest):
        with guard:
            if digest in components:
                return components[digest]
        component = environments.load(digest)
        with guard:
            components[digest] = {"digest": digest, "bytes": int(getattr(component, "image_size", 0) or 0)}
            return components[digest]

    def repository_rows(repository):
        from .environment_artifact import load_image_environment
        by_digest = {}
        for tag in registry.tags(repository):
            by_digest.setdefault(registry.manifest_digest(repository, tag), []).append(tag)
        result = []
        for digest, tags in sorted(by_digest.items()):
            layers = registry.manifest_layers(repository, digest)
            with guard:
                layer_sizes.update((layer.digest, layer.size) for layer in layers.layers)
            attachment = load_image_environment(environments, repository, digest, required=False)
            parts = [component_bytes(item) for item in attachment[1].components] if attachment else []
            family, task_rows = selected.get((repository, digest), (None, 0))
            result.append({
                "repository": repository, "manifest_digest": digest, "tags": sorted(tags),
                "environment_root": attachment[0] if attachment else None,
                "config_digest": attachment[1].source_image if attachment else None,
                "components": [part["digest"] for part in parts],
                "erofs_bytes": sum(part["bytes"] for part in parts),
                "oci_layers": [layer.digest for layer in layers.layers], "oci_bytes": layers.total_size,
                "build_input": (repository, digest) in inputs,
                "family": family or foundation_families.get((repository, digest)) or "unknown",
                "task_rows": task_rows})
        return result

    repositories = sorted(name for name in registry.catalog() if name.startswith(prefix))
    errors = {}
    with ThreadPoolExecutor(concurrency) as pool:
        for repository, future in [(name, pool.submit(repository_rows, name)) for name in repositories]:
            try:
                rows.extend(future.result())
            except Exception as exc:  # noqa: BLE001 - one bad repository is reported, not fatal
                errors[repository] = f"{type(exc).__name__}: {exc}"[:300]
    Path(out_rows).write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows))
    summary = summarize(rows, components, layer_sizes)
    summary.update(repositories=len(repositories), errors=errors)
    Path(out_summary).write_text(json.dumps(summary, indent=1, sort_keys=True) + "\n")
    return summary


def summarize(rows, components, layer_sizes):
    """Per family: images, unique OCI and EROFS bytes, and the OCI bytes only
    non-build-input images use, which releasing them frees (a layer shared with
    a build input or another family's kept image stays)."""
    kept = {layer for row in rows if row["build_input"] for layer in row["oci_layers"]}
    erofs = {digest: item["bytes"] for digest, item in components.items()}
    families = {}
    for row in rows:
        family = families.setdefault(row["family"], {"images": 0, "environment_images": 0, "build_inputs": 0,
                                                     "task_rows": 0, "oci": set(), "erofs": set()})
        family["images"] += 1
        family["environment_images"] += row["environment_root"] is not None
        family["build_inputs"] += row["build_input"]
        family["task_rows"] += row["task_rows"]
        family["oci"].update(row["oci_layers"])
        family["erofs"].update(row["components"])

    def total(digests, sizes):
        return sum(sizes.get(digest, 0) for digest in digests)
    return {"images": len(rows), "environment_images": sum(row["environment_root"] is not None for row in rows),
            "build_inputs": sum(row["build_input"] for row in rows),
            "unique_oci_bytes": total(layer_sizes, layer_sizes), "unique_erofs_bytes": total(erofs, erofs),
            "releasable_oci_bytes": total(set(layer_sizes) - kept, layer_sizes),
            "families": {name: {**{key: value for key, value in family.items() if key not in ("oci", "erofs")},
                                "unique_oci_bytes": total(family["oci"], layer_sizes),
                                "releasable_oci_bytes": total(family["oci"] - kept, layer_sizes),
                                "unique_erofs_bytes": total(family["erofs"], erofs)}
                         for name, family in sorted(families.items())}}


def wave_of(family):
    for wave, families in WAVES.items():
        if any(family == name or (name.endswith(":") and family.startswith(name)) for name in families):
            return wave
    return "4"


def read_jsonl(path):
    path = Path(path)
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()] if path.exists() else []


def shard_of(row, count):
    """A stable converter for each image: sha256 of repository@digest, mod ``count``."""
    import hashlib
    return int(hashlib.sha256(f"{row['repository']}@{row['manifest_digest']}".encode()).hexdigest(), 16) % count


def convert_wave(rows, wave, *, convert, results, parallel, shard=(0, 1)):
    """Step 2: convert a wave's images, ``parallel`` at once. ``convert``
    (repository, manifest digest, slot) returns the converter's result after
    its full-tree verification, or raises. A rerun skips converted images."""
    done = {(row["repository"], row["manifest_digest"]) for row in read_jsonl(results) if row.get("new_root")}
    pending = [row for row in rows if row.get("environment_root") and wave_of(row["family"]) == wave
               and shard_of(row, shard[1]) == shard[0] and (row["repository"], row["manifest_digest"]) not in done]
    guard, slots = threading.Lock(), list(range(parallel))

    def one(row):
        with guard:
            slot = slots.pop()
        record = {key: row[key] for key in ("repository", "manifest_digest", "config_digest", "build_input", "family")}
        record.update(wave=wave, old_root=row["environment_root"])
        try:
            result = convert(row["repository"], row["manifest_digest"], slot)
            if result["source_image"] != row["config_digest"]:
                raise ValueError("the converted root names another image config")
            record.update(new_root=result["root"], components=result["components"], verified=True)
        except Exception as exc:  # noqa: BLE001 - one failed image is reported, not fatal
            record["error"] = f"{type(exc).__name__}: {exc}"[-500:]
        finally:
            with guard:
                slots.append(slot)
        with guard, open(results, "a") as handle:
            handle.write(json.dumps(record, sort_keys=True) + "\n")
        return record

    with ThreadPoolExecutor(parallel) as pool:
        records = list(pool.map(one, pending))
    return {"wave": wave, "pending": len(pending), "converted": sum("new_root" in item for item in records),
            "failed": [f"{item['repository']}@{item['manifest_digest']}" for item in records if "error" in item]}


def record_results(roots, environments, results):
    """Each verified conversion becomes a ``converted`` row once both roots
    load with the gateway's trusted keys, the old one is still the image's
    annotation, and both name the image's config. Idempotent."""
    from .environment_artifact import load_environment, load_image_environment
    summary = {"recorded": 0, "unchanged": 0, "refused": {}}
    for result in read_jsonl(results):
        if not result.get("new_root"):
            continue
        key = (result["repository"], result["manifest_digest"])
        try:
            current = roots.get(*key)
            if current is not None and current["new_root"] == result["new_root"]:
                summary["unchanged"] += 1
                continue
            if result.get("verified") is not True:
                raise ValueError("no full-tree verification")
            old_root, old = load_image_environment(environments, *key)
            new = load_environment(environments, result["new_root"])
            if old_root != result["old_root"] or {old.source_image, new.source_image} != {result["config_digest"]}:
                raise ValueError("the image's root or config changed since its conversion")
            roots.record_converted(*key, config_digest=result["config_digest"], old_root=old_root,
                                   new_root=result["new_root"], wave=result["wave"],
                                   build_input=result["build_input"], detail="recorded from " + str(results))
            summary["recorded"] += 1
        except Exception as exc:  # noqa: BLE001 - reported per image
            summary["refused"]["@".join(key)] = f"{type(exc).__name__}: {exc}"[:300]
    return summary


def durable_owners(usage):
    """(repository, manifest digest) -> {(owner, tag)} for the durable,
    non-expiring image owners a switch re-points."""
    owners = {}
    for lease in usage.snapshot().leases.values():
        if (not lease.expires_at and lease.owner.startswith(DURABLE_OWNERS)
                and not lease.owner.endswith(":environment")):
            owners.setdefault((lease.repository, lease.digest), set()).add((lease.owner, lease.tag))
    return owners


SWITCH_BATCH = 100


def switch_wave(roots, environments, usage, wave, *, registry_host, keys=None, warm=None):
    """Step 3: dispatch the wave's converted (or reverted) roots, then acquire
    every durable owner of the image on the new closure. The old closure
    stays with those owners until the image is released, and with every route
    that started on it until the route ends.

    ``warm({image: components})`` makes every object those images' workers read
    resident on the store node, in one check and one fill per batch, and
    returns {image: failed objects}; an image switches only with none (M2
    wave 1: a cold object waits on S3's tail, past the NBD timeout)."""
    from .environment_artifact import load_environment
    from .environment_dependencies import EnvironmentDependencyResolver
    from .gateway.registry_refs import _persist_registry_image_protection
    resolver = EnvironmentDependencyResolver(environments, image_roots=roots)
    owners, switched, repointed, cold = durable_owners(usage), 0, 0, {}
    pending = [(row["repository"], row["manifest_digest"], row["new_root"]) for row in roots.rows(wave=wave)
               if row["state"] in ("converted", "reverted")
               and (keys is None or (row["repository"], row["manifest_digest"]) in keys)]
    for start in range(0, len(pending), SWITCH_BATCH):
        batch = {}
        for repository, digest, root in pending[start:start + SWITCH_BATCH]:
            try:  # The whole closure, signed and published: retention may have taken a part.
                components = load_environment(environments, root).components
                for component in components:
                    environments.load(component)
                batch[(repository, digest)] = components
            except (OSError, ValueError) as exc:  # RegistryRequestError (404) is a ValueError.
                cold[f"{repository}@{digest}"] = [f"closure: {type(exc).__name__}: {exc}"[:300]]
        try:
            failed = warm(batch) if warm is not None else {}
        except (OSError, ValueError) as exc:  # The index or node timed out: this batch waits for a rerun.
            failed = dict.fromkeys(batch, [f"{type(exc).__name__}: {exc}"[:300]])
        for key in batch:
            if failed.get(key):
                cold["@".join(key)] = failed[key][:3]
                continue
            held = sorted(owners.get(key, ()))
            roots.transition(*key, "switched", detail=json.dumps({"owners": [owner for owner, _ in held]}))
            for owner, tag in held:
                image = f"{registry_host}/{key[0]}:{tag}@{key[1]}"
                if not _persist_registry_image_protection(usage, image, owner, touch=False, persistent=True,
                                                          dependency_resolver=resolver):
                    raise RuntimeError(f"{image}: the new closure was not retained for {owner}")
            switched += 1
            repointed += len(held)
    return {"wave": wave, "switched": switched, "owners_repointed": repointed, "not_warm": cold}


def revert_wave(roots, wave, *, keys=None, detail=""):
    """Rollback (plan §7): creates take the annotation's root again. Owners
    keep both closures, and retention keeps a reverted root, so a switch can
    follow without reconversion."""
    reverted = 0
    for row in roots.rows(wave=wave, state="switched"):
        key = (row["repository"], row["manifest_digest"])
        if keys is None or key in keys:
            roots.transition(*key, "reverted", detail=detail)
            reverted += 1
    return {"wave": wave, "reverted": reverted}


def release_wave(roots, environments, usage, wave, *, keys=None, execute=False):
    """Step 4 (plan §3.3), EROFS only: drop each switched image's durable
    owners' leases on its old closure. Retention then deletes the old root and
    every component nothing else keeps, and the registry sweep frees them. The
    OCI manifest stays: readers for a deleted manifest are not built, and
    builds read FROM layers from it. An owner's lease on a digest another of
    its images still needs (by its dispatched root, else its annotation) stays."""
    from .environment_artifact import load_environment, load_image_environment

    def closure(root):
        try:
            return {root, *load_environment(environments, root).components}
        except (OSError, ValueError):  # Retention may have taken part of an old closure already.
            return {root}
    owners, held_by = durable_owners(usage), {}
    for key, entries in owners.items():
        for owner, _tag in entries:
            held_by.setdefault(owner, set()).add(key)
    leases = {}
    for lease in usage.snapshot().leases.values():
        if not lease.expires_at and lease.owner.endswith(":environment"):
            leases.setdefault(lease.owner, []).append(lease)
    rows = [row for row in roots.rows(wave=wave, state="switched")
            if keys is None or (row["repository"], row["manifest_digest"]) in keys]
    needed = {}  # Closures cached per image key.

    def keeps(key):
        """The digests an image still needs; None when unknown (keep every lease)."""
        if key not in needed:
            try:
                root = roots.dispatch_root(*key) or next(
                    iter(load_image_environment(environments, *key, required=False) or ("",)), "")
                needed[key] = closure(root) if root else set()
            except (OSError, ValueError):
                needed[key] = None
        return needed[key]
    summary = {"wave": wave, "execute": execute, "images": 0, "leases": 0, "old_digests": set()}
    for row in rows:
        key = (row["repository"], row["manifest_digest"])
        drop = closure(row["old_root"]) - closure(row["new_root"])
        released = 0
        for owner, _tag in sorted(owners.get(key, ())):
            others = [keeps(other) for other in held_by[owner] if other != key]
            if None in others:
                summary["owners_kept_unknown"] = summary.get("owners_kept_unknown", 0) + 1
                continue
            kept = set().union(*others)
            for lease in leases.get(owner + ":environment", ()):
                if lease.digest in drop and lease.digest not in kept:
                    if execute:
                        usage.release_lease(lease.repository, lease.tag, lease.owner)
                    released += 1
                    summary["old_digests"].add(lease.digest)
        if execute:
            roots.transition(*key, "released", detail=json.dumps({"leases": released}))
        summary["images"] += 1
        summary["leases"] += released
    summary["old_digests"] = len(summary["old_digests"])
    return summary


def family_keys(rows_file, family):
    if family and rows_file is None:
        raise ValueError("--family needs the inventory --rows")
    return None if not family else {(row["repository"], row["manifest_digest"])
                                    for row in read_jsonl(rows_file) if row["family"] == family}


def inventory_command(args):
    from .config import DeploymentConfig
    from .environment_config import environment_registry_from_deployment
    from .managed_registry import RegistryClient
    from .prepared_images import catalog_path
    config = DeploymentConfig.from_file(args.config)
    environments = environment_registry_from_deployment(config)
    if environments is None:
        raise ValueError("the deployment has no immutable_environments block")
    summary = inventory(RegistryClient(config.registry_url), environments, prefix=args.prefix,
                        catalog_file=catalog_path(config.image_file()), selection=args.selection,
                        concurrency=args.concurrency, out_rows=args.out, out_summary=args.summary)
    print(json.dumps({key: value for key, value in summary.items() if key != "families"}, sort_keys=True))
    return 0 if not summary["errors"] else 1


def convert_command(args):
    """Each image converts in its own process: its own index owner (a shared one
    let parallel converters claim as one builder, M1 gate run 3), its own
    verification devices, and a crash costs one image."""
    per_slot = len(args.verify_device) // args.parallel
    if per_slot < 1:
        raise ValueError("M2 conversions are full-tree verified: give each parallel slot a --verify-device")
    host = urlparse(args.environment_registry_url).netloc
    base = [sys.executable, "-c", CLI, "convert-environment", "--config", str(args.config),
            "--chunk-index-token-file", str(args.chunk_index_token_file), "--work-root", str(args.work_root),
            "--environment-registry-url", args.environment_registry_url,
            "--environment-registry-repository", args.environment_registry_repository,
            "--environment-trusted-keys", str(args.environment_trusted_keys),
            "--environment-signing-key", str(args.environment_signing_key), "--nydusd-blobs"]

    def convert(repository, digest, slot):
        argv = base + ["--image-ref", f"{host}/{repository}@{digest}"]
        for device in args.verify_device[slot * per_slot:(slot + 1) * per_slot]:
            argv += ["--verify-device", device]
        completed = subprocess.run(argv, capture_output=True, text=True, timeout=3 * 3600, env=os.environ)
        if completed.returncode:
            raise RuntimeError(" | ".join(completed.stderr.strip().splitlines()[-3:]))
        return json.loads(completed.stdout.strip().splitlines()[-1])
    index, _, count = args.shard.partition("/")
    if not (index.isdigit() and count.isdigit() and int(index) < int(count)):
        raise ValueError("--shard is I/N with 0 <= I < N")
    summary = convert_wave(read_jsonl(args.rows), args.wave, convert=convert, results=args.results,
                           parallel=args.parallel, shard=(int(index), int(count)))
    print(json.dumps(summary, sort_keys=True))
    return 0 if not summary["failed"] else 1


def store_warmer(config):
    """warm({image: components}) -> {image: failed objects}, through the
    deployment's store node: one residency check, one fill of what is
    missing, one recheck."""
    from .chunk_index import ChunkIndexClient
    from .chunk_store_node import ChunkStoreClient, locator_objects
    from .environment_config import read_token
    store = config.immutable_environments.chunk_store
    if store is None or store.store_node is None:
        raise ValueError("switch needs immutable_environments.chunk_store with a store node to warm")
    index = ChunkIndexClient(store.index_url, read_token(store.read_token_file).decode())
    token = read_token(store.write_token_file).decode()
    client = ChunkStoreClient(store.store_node.url, token, timeout=120.0)

    def warm(batch):
        objects = {image: [item["key"] for component in components for item in
                           locator_objects(index.locator(component), store.store_node.url, token)]
                   for image, components in batch.items()}
        missing = set(client.missing({key for keys in objects.values() for key in keys}))
        if missing:
            done = client.wait(client.warm([{"key": key, "ranges": None} for key in sorted(missing)])["job"],
                               timeout=3600)
            missing = set(client.missing(missing))
            if missing and done.get("errors"):
                _LOG.warning("store node fill: %s", "; ".join(done["errors"][:3]))
        return {image: [key for key in keys if key in missing] for image, keys in objects.items()}
    return warm


def gateway_command(args):
    from .config import DeploymentConfig
    from .environment_config import environment_registry_from_deployment
    from .gateway.image_roots import ImageRootsStore, roots_path
    config = DeploymentConfig.from_file(args.config)
    environments = environment_registry_from_deployment(config)
    if environments is None:
        raise ValueError("the deployment has no immutable_environments block")
    roots, command = ImageRootsStore(roots_path(config.image_file())), args.chunk_migrate_command
    if command == "record":
        result = record_results(roots, environments, args.results)
    elif command == "switch":
        if not config.immutable_environments.dispatch_roots:
            raise ValueError("switch needs immutable_environments.dispatch_roots on: otherwise owners and new "
                             "routes lease the new closure while creates still mount the annotation's root")
        from .host_locks import HOST_LOCKS
        from .managed_registry import RegistryUsageStore
        HOST_LOCKS.configure(config.control_state_file().parent / "gateway-locks")  # The gateway's lease fence.
        result = switch_wave(roots, environments, RegistryUsageStore(config.registry_usage_file()), args.wave,
                             registry_host=urlparse(config.registry_url).netloc,
                             keys=family_keys(args.rows, args.family), warm=store_warmer(config))
    elif command == "revert":
        result = revert_wave(roots, args.wave, keys=family_keys(args.rows, args.family), detail=args.reason)
    elif command == "release":
        from .host_locks import HOST_LOCKS
        from .managed_registry import RegistryUsageStore
        HOST_LOCKS.configure(config.control_state_file().parent / "gateway-locks")
        result = release_wave(roots, environments, RegistryUsageStore(config.registry_usage_file()), args.wave,
                              keys=family_keys(args.rows, args.family), execute=args.execute)
    else:
        result = {}
        for row in roots.rows():
            states = result.setdefault(row["wave"], {})
            states[row["state"]] = states.get(row["state"], 0) + 1
    print(json.dumps(result, sort_keys=True))
    return 0 if not result.get("refused") else 1


def add_commands(subparsers):
    migrate = subparsers.add_parser("chunk-migrate", help="Chunk store M2 migration (plan §5).")
    commands = migrate.add_subparsers(dest="chunk_migrate_command", required=True)
    listing = commands.add_parser("inventory", help="Read-only inventory of managed images (step 1).")
    listing.add_argument("--config", type=Path, required=True)
    listing.add_argument("--prefix", default="ucloud-managed/")
    listing.add_argument("--selection", type=Path, help="the training selection (zip or selectors JSON)")
    listing.add_argument("--concurrency", type=int, default=8)
    listing.add_argument("--out", type=Path, required=True, help="JSON lines, one per image")
    listing.add_argument("--summary", type=Path, required=True)
    listing.set_defaults(func=inventory_command)
    from .environment_config import add_environment_registry_args
    convert = commands.add_parser("convert", help="Convert and verify a wave's images (step 2, converter host).")
    convert.add_argument("--config", type=Path, required=True, help="deployment.json with chunk_store")
    convert.add_argument("--chunk-index-token-file", type=Path, required=True)
    convert.add_argument("--work-root", type=Path, required=True)
    add_environment_registry_args(convert)
    convert.add_argument("--environment-signing-key", type=Path, required=True)
    convert.add_argument("--verify-device", action="append", default=[], help="NBD device; split across slots")
    convert.add_argument("--parallel", type=int, default=12)
    convert.add_argument("--shard", default="0/1", help="I/N: this converter's share of the wave")
    convert.set_defaults(func=convert_command)
    for name, text in (("record", "Record verified conversions as converted rows (gateway)."),
                       ("switch", "Dispatch a wave's new roots and re-point its durable owners (step 3)."),
                       ("revert", "Dispatch a switched wave's old roots again (rollback)."),
                       ("release", "Drop durable owners' leases on switched images' old EROFS closures (dry run "
                                   "unless --execute)."),
                       ("status", "image_roots rows per wave and state.")):
        command = commands.add_parser(name, help=text)
        command.add_argument("--config", type=Path, required=True)
        if name == "record":
            command.add_argument("--results", type=Path, required=True)
        if name in ("switch", "revert", "release"):
            command.add_argument("--family", default="", help="only this inventory family (needs --rows)")
        if name == "release":
            command.add_argument("--execute", action="store_true", help="release; without it, only count")
        if name == "revert":
            command.add_argument("--reason", default="")
        command.set_defaults(func=gateway_command)
    for command in (convert, commands.choices["switch"], commands.choices["revert"], commands.choices["release"]):
        command.add_argument("--wave", choices=sorted(WAVES), required=True)
        command.add_argument("--rows", type=Path, required=command is convert, help="inventory rows (JSON lines)")
    convert.add_argument("--results", type=Path, required=True, help="JSON lines, appended; a rerun resumes")
