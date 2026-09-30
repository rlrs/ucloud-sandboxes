#!/usr/bin/env python3
"""Archive rebuild inputs without registry blobs, credentials or success receipts.

Materialization always creates fresh work directories and build identities.
Package repositories remain external dependencies; this is not a byte backup.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
from pathlib import Path
import re
import shlex
import sys

from prepare_image_foundations import validate_context
from prepare_image_pool import rebuild_generation, registry_parts

POLICY = {"registry_ceiling_gb": 3000, "free_floor_gib": 500,
          "growth_limit_gib": 1800, "source_growth_limit_gib": 1200,
          "max_source_compressed_gib": 5, "foundation_reservation_gib": 8,
          "source_workers": 8, "tmax_workers": 8, "terminal_workers": 8}


def read(path):
    return json.loads(path.read_text())


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def pack(pool_root, foundation_roots, output):
    if output.exists():
        raise ValueError("refusing to replace an existing recovery bundle")
    source_plan = read(pool_root / "plan.json")
    sources = []
    catalog = read(pool_root / "catalog.json") if (pool_root / "catalog.json").exists() else {}
    for item in source_plan["images"]:
        row = {k: item[k] for k in ("source", "families", "task_rows", "uses", "repository", "preparation") if k in item}
        registry_parts(row["source"])
        receipt_path = pool_root / (hashlib.sha256(row["source"].encode()).hexdigest() + ".json")
        receipt = read(receipt_path) if receipt_path.exists() else {}
        if receipt and receipt.get("source") != row["source"]:
            raise ValueError("source receipt identity mismatch")
        pin = (receipt.get("resolved", {}).get("reference") or item.get("pinned_source")
               or catalog.get("images", {}).get(row["source"], {}).get("source_reference"))
        if pin:
            registry_parts(pin)
            if not re.fullmatch(r"[^\s@]+@sha256:[a-f0-9]{64}", pin):
                raise ValueError("source pin must be immutable")
            row["pinned_source"] = pin
        sources.append(row)
    foundations = {}
    for root in foundation_roots:
        plan = read(root / "plan.json")
        for item in plan["foundations"]:
            context = validate_context(root, item)
            entry = {"item": item, "revision": plan["revision"],
                     "files": {p.name: p.read_text() for p in sorted(context.iterdir())}}
            previous = foundations.get(item["key"])
            if previous and previous != entry:
                raise ValueError("conflicting foundation input")
            foundations[item["key"]] = entry
    payload = {"schema": 1, "policy": POLICY, "sources": sources,
               "foundations": sorted(foundations.values(), key=lambda x: x["item"]["key"])}
    envelope = {"payload_sha256": hashlib.sha256(encode(payload)).hexdigest(), "payload": payload}
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(gzip.compress(encode(envelope), mtime=0))
    return {"sources": len(sources), "pinned_sources": sum("pinned_source" in s for s in sources),
            "foundations": len(foundations), "bytes": output.stat().st_size,
            "sha256": hashlib.sha256(output.read_bytes()).hexdigest()}


def load_bundle(path):
    envelope = json.loads(gzip.decompress(path.read_bytes()))
    payload = envelope["payload"]
    if hashlib.sha256(encode(payload)).hexdigest() != envelope["payload_sha256"] or payload.get("schema") != 1:
        raise ValueError("recovery bundle checksum or schema mismatch")
    return payload


def materialize(bundle, output, generation):
    if not generation:
        raise ValueError("a fresh rebuild generation is required")
    rebuild_generation({"rebuild_generation": generation})
    if output.exists():
        raise ValueError("use a fresh recovery directory; old success records are unsafe after volume loss")
    payload = load_bundle(bundle)
    output.mkdir(parents=True)
    groups = {}
    revisions = {}
    bases = {}
    for entry in payload["foundations"]:
        item = entry["item"]
        family = item.get("family", "tmax")
        if family not in {"tmax", "tmax-inline", "openswe", "terminal-prefix"}:
            raise ValueError("unknown foundation family")
        expected = "foundation-" + family + "-" + item["key"][:32]
        if item["image_id"] != expected or not re.fullmatch(r"[a-f0-9]{64}", item["key"]):
            raise ValueError("invalid foundation path identity")
        root = output / family
        context = root / expected
        context.mkdir(parents=True)
        for name, content in entry["files"].items():
            if name not in {"Dockerfile", "base_install.sh"}:
                raise ValueError("unexpected foundation input file")
            (context / name).write_text(content)
        validate_context(root, item)
        first_line = entry["files"]["Dockerfile"].splitlines()[0]
        if not first_line.startswith("FROM "):
            raise ValueError("foundation must start with its pinned base")
        host, repository, digest = registry_parts(first_line[5:])
        if not re.fullmatch(r"sha256:[a-f0-9]{64}", digest):
            raise ValueError("foundation recovery base must be immutable")
        reference = host + "/" + repository + "@" + digest
        base = bases.setdefault(reference, {"source": reference, "pinned_source": reference,
            "families": [], "task_rows": 0, "preparation": "source"})
        base["task_rows"] += item["tasks"]
        if family not in base["families"]:
            base["families"].append(family)
        groups.setdefault(family, []).append(item)
        if family in revisions and revisions[family] != entry["revision"]:
            raise ValueError("conflicting source revisions within a foundation family")
        revisions[family] = entry["revision"]
    for family, items in groups.items():
        # High fanout dependencies first. This minimizes repeated live steps;
        # recipe count is a proxy until a real workload selection is available.
        plan = {"schema": 1, "rebuild_generation": generation, "revision": revisions[family],
                "foundations": sorted(items, key=lambda x: (-x["tasks"], x["key"]))}
        (output / family / "plan.json").write_bytes(encode(plan))
    (output / "sources").mkdir()
    task_bases = [item for item in payload["sources"]
                  if any(use.get("level") == "base_only" for use in item.get("uses", []))]
    if any(item.get("preparation", "source") != "source" for item in task_bases):
        raise ValueError("generic task bases require faithful source preparation")
    task_base_sources = {item["source"] for item in task_bases}
    remaining_sources = [item for item in payload["sources"] if item["source"] not in task_base_sources]
    (output / "sources" / "plan.json").write_bytes(encode({"schema": 1,
        "rebuild_generation": generation, "images": remaining_sources}))
    (output / "task-bases").mkdir()
    (output / "task-bases" / "plan.json").write_bytes(encode({"schema": 1,
        "rebuild_generation": generation,
        "images": sorted(task_bases, key=lambda x: (-x["task_rows"], x["source"]))}))
    (output / "bases").mkdir()
    (output / "bases" / "plan.json").write_bytes(encode({"schema": 1,
        "rebuild_generation": generation,
        "images": sorted(bases.values(), key=lambda x: (-x["task_rows"], x["source"]))}))
    manifest = {"schema": 1, "rebuild_generation": generation, "policy": payload["policy"],
                "bundle_sha256": hashlib.sha256(bundle.read_bytes()).hexdigest(),
                "foundation_groups": {name: len(items) for name, items in groups.items()},
                "sources": len(remaining_sources), "task_bases": len(task_bases),
                "foundation_bases": len(bases)}
    (output / "campaign.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def commands(root, gateway, sdk_wheel, config, python):
    """Print explicit commands for review; never resize storage or delete data."""
    manifest = read(root / "campaign.json")
    policy = manifest["policy"]
    scripts = Path(__file__).resolve().parent
    result = []
    # Resolve/stage the small common base set first. Foundation builds then
    # read private bases even when the upstream registry throttles task images.
    for family in ("bases", "task-bases", "tmax", "openswe", "tmax-inline", "terminal-prefix", "sources"):
        source = family in {"sources", "bases", "task-bases"}
        count = (manifest.get("foundation_bases", 0) if family == "bases" else
                 manifest.get("task_bases", 0) if family == "task-bases" else
                 manifest["sources"] if source else manifest["foundation_groups"].get(family, 0))
        if not count:
            continue
        workers = (policy["source_workers"] if source else policy["terminal_workers"]
                   if family == "terminal-prefix" else policy["tmax_workers"])
        cmd = [python, str(scripts / ("prepare_image_pool.py" if source else "prepare_image_foundations.py")),
               "--root", str(root / family), "--gateway", gateway, "--sdk-wheel", str(sdk_wheel),
               "--config", str(config), "--limit", str(count), "--workers", str(workers),
               "--growth-limit-gib", str(policy["source_growth_limit_gib"] if source else policy["growth_limit_gib"]),
               "--free-floor-gib", str(policy["free_floor_gib"])]
        cmd += (["--max-image-gib", str(policy["max_source_compressed_gib"]), "--stage-upstream"] if source else
                ["--reservation-gib", str(policy["foundation_reservation_gib"])])
        if not source and manifest.get("foundation_bases"):
            cmd += ["--base-catalog", str(root / "bases" / "catalog.json")]
        if not source and manifest.get("task_bases"):
            cmd += ["--base-catalog", str(root / "task-bases" / "catalog.json")]
        result.append(shlex.join(cmd))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    export = sub.add_parser("pack")
    export.add_argument("--pool-root", type=Path, required=True)
    export.add_argument("--foundation-root", type=Path, action="append", required=True)
    export.add_argument("--output", type=Path, required=True)
    restore = sub.add_parser("materialize")
    restore.add_argument("--bundle", type=Path, required=True)
    restore.add_argument("--output", type=Path, required=True)
    restore.add_argument("--generation", required=True)
    cmd = sub.add_parser("commands")
    cmd.add_argument("--root", type=Path, required=True)
    cmd.add_argument("--gateway", required=True)
    cmd.add_argument("--sdk-wheel", type=Path, required=True)
    cmd.add_argument("--config", type=Path, default=Path("/etc/ucloud-sandboxes/deployment.json"))
    cmd.add_argument("--python", default=sys.executable)
    args = vars(parser.parse_args())
    action = args.pop("command")
    if action == "pack":
        args["foundation_roots"] = args.pop("foundation_root")
    result = globals()[action](**args)
    print("\n".join(result) if action == "commands" else json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
