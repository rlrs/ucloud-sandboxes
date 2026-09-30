#!/usr/bin/env python3
"""Plan shared foundations or rewrite a recipe index to use a validated catalog.

Operations write new outputs. Task commands stay in order; monolithic installers
are factored only at a recognized literal initial apt/pip boundary.
"""
from __future__ import annotations

import argparse
from contextlib import closing
import hashlib
import json
import os
import re
from collections import Counter
from pathlib import Path
import sqlite3

from ucloud_sandboxes.image_foundations import openswe_foundation, tmax_foundation, tmax_inline_foundation


def plan(root: Path, output: Path, base: str, revision: str):
    if output.exists():
        raise ValueError("use a new plan directory")
    entries = {}
    for path in sorted(root.glob("*/environment/base_install.sh")):
        foundation = tmax_foundation((path.parent / "Dockerfile").read_text(), path.read_bytes(), ubuntu_base=base)
        entry = entries.get(foundation.key)
        if entry is None:
            context = output / foundation.image_id
            context.mkdir(parents=True)
            (context / "Dockerfile").write_text(foundation.dockerfile)
            (context / "base_install.sh").write_bytes(foundation.installer)
            entry = entries[foundation.key] = {
                "image_id": foundation.image_id, "key": foundation.key, "base": base,
                "source_prefix": foundation.source_prefix,
                "installer_sha256": hashlib.sha256(foundation.installer).hexdigest(),
                "example": path.parent.parent.name, "tasks": 0,
            }
        entry["tasks"] += 1
    if not entries:
        raise ValueError("no explicit TMax base stages found")
    result = {"schema": 1, "revision": revision,
              "foundations": sorted(entries.values(), key=lambda item: (-item["tasks"], item["key"]))}
    (output / "plan.json").write_text(json.dumps(result, indent=2) + "\n")
    return {"foundations": len(entries), "tasks": sum(item["tasks"] for item in entries.values())}


def plan_openswe(recipes: Path, output: Path, base: str, revision: str):
    if output.exists():
        raise ValueError("use a new plan directory")
    versions = Counter()
    with recipes.open() as rows:
        for line in rows:
            row = json.loads(line)
            match = re.search(r"^FROM openswe-python-(\d+\.\d+)[ \t]*$", row["Dockerfile"], re.M)
            if match:
                versions[match[1]] += 1
    entries = []
    for version, count in versions.most_common():
        foundation = openswe_foundation(version, miniconda_base=base)
        context = output / foundation.image_id
        context.mkdir(parents=True)
        (context / "Dockerfile").write_text(foundation.dockerfile)
        entries.append({"family": "openswe", "image_id": foundation.image_id, "key": foundation.key,
                        "base": base, "source_prefix": foundation.source_prefix,
                        "python_version": version, "tasks": count})
    if not entries:
        raise ValueError("no supported OpenSWE foundations found")
    (output / "plan.json").write_text(json.dumps({"schema": 1, "revision": revision,
                                                 "foundations": entries}, indent=2) + "\n")
    return {"foundations": len(entries), "tasks": sum(versions.values())}


def plan_tmax_inline(root: Path, output: Path, base: str, revision: str):
    if output.exists():
        raise ValueError("use a new plan directory")
    groups = {}
    skipped = 0
    for path in sorted(root.glob("*/environment/post_install.sh")):
        if (path.parent / "base_install.sh").exists():
            continue
        try:
            foundation, _ = tmax_inline_foundation((path.parent / "Dockerfile").read_text(),
                                                 path.read_bytes(), ubuntu_base=base)
        except ValueError:
            skipped += 1
            continue
        if foundation.key not in groups:
            context = output / foundation.image_id
            context.mkdir(parents=True)
            (context / "Dockerfile").write_text(foundation.dockerfile)
            (context / "base_install.sh").write_bytes(foundation.installer)
            groups[foundation.key] = {"family": "tmax-inline", "image_id": foundation.image_id,
                "key": foundation.key, "base": base, "source_prefix": foundation.source_prefix,
                "example": path.parent.parent.name, "tasks": 0}
        groups[foundation.key]["tasks"] += 1
    if not groups:
        raise ValueError("no supported initial dependency prefixes")
    entries = sorted(groups.values(), key=lambda x: (-x["tasks"], x["key"]))
    (output / "plan.json").write_text(json.dumps({"schema": 1, "revision": revision, "foundations": entries}, indent=2) + "\n")
    return {"foundations": len(entries), "tasks": sum(x["tasks"] for x in entries), "skipped": skipped}


def rewrite_index(source: Path, output: Path, catalog: dict):
    if output.exists():
        raise ValueError("output index already exists; never rewrite a live index")
    if catalog.get("schema") != 1:
        raise ValueError("unsupported foundation catalog")
    ready = [item for item in catalog["foundations"].values() if item.get("validated") is True]
    if not ready:
        raise ValueError("catalog has no validated foundations")
    temporary = output.with_name(output.name + ".preparing")
    with temporary.open("xb"):
        pass
    changed = 0
    try:
        with closing(sqlite3.connect(source.resolve().as_uri() + "?mode=ro", uri=True)) as original:
            with closing(sqlite3.connect(temporary)) as target:
                original.backup(target)
                rows = target.execute("SELECT source, family, recipe FROM images WHERE family IN ('tmax', 'openswe')").fetchall()
                for source_name, family, encoded in rows:
                    candidates = [item for item in ready if item.get("family", "tmax") == family
                                  or (family == "tmax" and item.get("family") == "tmax-inline")]
                    if not candidates:
                        continue
                    recipe = json.loads(encoded)
                    if family == "tmax":
                        explicit = "COPY base_install.sh /tmp/base_install.sh\n" in recipe["dockerfile"]
                        script_name = "base_install.sh" if explicit else "post_install.sh"
                        if f"COPY {script_name} /tmp/{script_name}\n" not in recipe["dockerfile"]:
                            continue
                        inline = recipe.get("files", {}).get(script_name)
                        installer = (inline.encode() if inline is not None else
                                     (Path(recipe["context_dir"]) / script_name).read_bytes())
                    for item in candidates:
                        if family == "tmax":
                            try:
                                if item.get("family") == "tmax-inline":
                                    if explicit:
                                        continue
                                    foundation, remainder = tmax_inline_foundation(recipe["dockerfile"], installer, ubuntu_base=item["base"])
                                else:
                                    if not explicit:
                                        continue
                                    foundation = tmax_foundation(recipe["dockerfile"], installer, ubuntu_base=item["base"])
                            except ValueError:
                                # Unusual task prefixes must remain unchanged.
                                continue
                        else:
                            foundation = openswe_foundation(item["python_version"], miniconda_base=item["base"])
                            if foundation.prefix_offset(recipe["dockerfile"]) is None:
                                continue
                        if foundation.key != item["key"]:
                            continue
                        recipe["dockerfile"] = foundation.task_dockerfile(recipe["dockerfile"], item["reference"])
                        if item.get("family") == "tmax-inline":
                            recipe["files"] = {**recipe.get("files", {}), "post_install.sh": remainder.decode()}
                        target.execute("UPDATE images SET recipe = ?, prepared_image = NULL WHERE source = ?",
                                       (json.dumps(recipe, sort_keys=True), source_name))
                        changed += 1
                        break
                target.commit()
        os.link(temporary, output)  # Publish atomically without overwriting another writer.
        temporary.unlink()
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    return {"rewritten": changed, "eligible_family_rows": len(rows), "output": str(output)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    create = commands.add_parser("plan")
    create.add_argument("--tmax-root", type=Path, required=True)
    create.add_argument("--output", type=Path, required=True)
    create.add_argument("--ubuntu-base", required=True)
    create.add_argument("--source-revision", required=True)
    inline = commands.add_parser("plan-tmax-inline")
    inline.add_argument("--tmax-root", type=Path, required=True)
    inline.add_argument("--output", type=Path, required=True)
    inline.add_argument("--ubuntu-base", required=True)
    inline.add_argument("--source-revision", required=True)
    openswe = commands.add_parser("plan-openswe")
    openswe.add_argument("--recipes", type=Path, required=True)
    openswe.add_argument("--output", type=Path, required=True)
    openswe.add_argument("--miniconda-base", required=True)
    openswe.add_argument("--source-revision", required=True)
    rewrite = commands.add_parser("rewrite-index")
    rewrite.add_argument("--source", type=Path, required=True)
    rewrite.add_argument("--output", type=Path, required=True)
    rewrite.add_argument("--catalog", type=Path, required=True, action="append")
    args = parser.parse_args()
    if args.command == "plan":
        result = plan(args.tmax_root, args.output, args.ubuntu_base, args.source_revision)
    elif args.command == "plan-tmax-inline":
        result = plan_tmax_inline(args.tmax_root, args.output, args.ubuntu_base, args.source_revision)
    elif args.command == "plan-openswe":
        result = plan_openswe(args.recipes, args.output, args.miniconda_base, args.source_revision)
    else:
        catalog = {"schema": 1, "foundations": {}}
        for path in args.catalog:
            part = json.loads(path.read_text())
            if part.get("schema") != 1:
                raise ValueError("unsupported foundation catalog")
            for key, entry in part["foundations"].items():
                previous = catalog["foundations"].get(key)
                if previous is not None and previous != entry:
                    raise ValueError("conflicting foundation catalog entries")
                catalog["foundations"][key] = entry
        result = rewrite_index(args.source, args.output, catalog)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
