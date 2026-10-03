"""Chunk store M2 operator tool (docs/chunk-store-m2-plan.md §5).

``chunk-migrate inventory`` is step 1: a read-only list of every managed image
with its environment root, components, sizes, build-input status and family,
from which the waves and their predicted release are planned. It reads
manifests and the prepared catalog only; it writes its two output files and
nothing else.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import sqlite3
import threading
import zipfile

SELECTORS_NAME = "all-image-selectors.json"


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
