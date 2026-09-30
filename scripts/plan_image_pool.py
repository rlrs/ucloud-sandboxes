#!/usr/bin/env python3
"""Select a mixed image preparation batch or apply its ready bases to an index."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from contextlib import closing
import json
import os
from pathlib import Path
import re
import sqlite3

from ucloud_sandboxes.image_foundations import require_pinned_reference


def select_images(images, limit, per_family, *, balanced=False):
    if limit < 1 or per_family < 0:
        raise ValueError("invalid selection limits")
    by_source = {}
    for item in images:
        source = item["source"]
        if source in by_source:
            raise ValueError("inventory must group uses by unique source")
        if not item.get("families") or item.get("task_rows", 0) < 1:
            raise ValueError("image needs families and a positive task count")
        by_source[source] = item
    ranked = sorted(by_source.values(), key=lambda x: (-x["task_rows"], x["source"]))
    families = sorted({f for item in ranked for f in item["families"]})
    selected = {}
    # A small round-robin floor prevents a single high-fanout family from
    # hiding unresolved inputs in every other family.
    queues = {}
    for family in families:
        candidates = [item for item in ranked if family in item["families"]]
        groups = defaultdict(list)
        for item in candidates:
            groups[item.get("repository") or item["source"]].append(item)
        ordered = sorted(groups.values(), key=lambda group: (-sum(x["task_rows"] for x in group), group[0]["source"]))
        representatives = [group[0] for group in ordered]
        first = {item["source"] for item in representatives}
        queues[family] = representatives + [item for item in candidates if item["source"] not in first]
    for index in range(per_family):
        for family in families:
            if index < len(queues[family]) and len(selected) < limit:
                item = queues[family][index]
                selected[item["source"]] = item
    if balanced:
        # Equalize the fraction visited within each family's queue so a huge
        # family cannot postpone all smaller evaluation pools until the end.
        import heapq
        heap = [(0.0, family, 0) for family in families if queues[family]]
        heapq.heapify(heap)
        ordered = []
        seen = set(selected)
        while heap and len(selected) + len(ordered) < limit:
            _, family, index = heapq.heappop(heap)
            item = queues[family][index]
            if item["source"] not in seen:
                ordered.append(item)
                seen.add(item["source"])
            index += 1
            if index < len(queues[family]):
                heapq.heappush(heap, (index / len(queues[family]), family, index))
        ranked = ordered
    for item in ranked:
        if len(selected) >= limit:
            break
        selected[item["source"]] = item
    return list(selected.values())


def coverage_report(inventory, catalogs):
    ready = {}
    components = {}
    summed_bytes = 0
    for catalog in catalogs:
        if catalog.get("schema") != 1:
            raise ValueError("unsupported pool catalog")
        for source, item in catalog["images"].items():
            if item.get("status") != "ready":
                continue
            if source in ready:
                if ready[source]["reference"] != item["reference"]:
                    raise ValueError("conflicting prepared source versions")
                continue
            ready[source] = item
            for component in item["components"]:
                components[component["digest"]] = component["bytes"]
                summed_bytes += component["bytes"]
    families = defaultdict(Counter)
    for item in inventory["images"]:
        uses = item.get("uses") or [{"family": f, "level": "upstream_task_image", "task_rows": item["task_rows"]}
                                    for f in item["families"]]
        for use in uses:
            bucket = "base_only" if use["level"] == "base_only" else "upstream_image"
            families[use["family"]][bucket + "_rows"] += use["task_rows"]
            if item["source"] in ready:
                families[use["family"]][bucket + "_rows_ready"] += use["task_rows"]
    return {"families": dict(families), "ready_source_references": len(ready),
            "unique_erofs_bytes": sum(components.values()), "summed_erofs_bytes": summed_bytes,
            "scope": "Upstream image/base coverage; not proof of task-specific setup or an unavailable training selection"}


def rewrite_bases(recipe, mappings):
    """Replace only exact FROM operands, preserving instructions and contexts."""
    text = recipe["dockerfile"]
    if re.search(r"^\s*#\s*(syntax|escape|check)\s*=", text, re.I | re.M):
        return recipe
    pattern = r"^([ \t]*FROM[ \t]+)([^\s\\]+)([ \t]*(?:[Aa][Ss][ \t]+[\w.-]+)?[ \t]*)$"
    if len(re.findall(r"^[ \t]*FROM\b", text, re.I | re.M)) != len(re.findall(pattern, text, re.I | re.M)):
        return recipe
    stages = set()

    def replace(match):
        source = match[2]
        ready = None if source.lower() in stages else mappings.get(source)
        if ready and ready.get("preparation", "source") != "source" and complete_prepared_image(recipe, {source: ready}) is None:
            # Enriched images have different defaults and contents. Substitute
            # them only for the exact preparation recipe they implement.
            ready = None
        stage = re.search(r"\bAS[ \t]+([\w.-]+)", match[3], re.I)
        if stage:
            stages.add(stage[1].lower())
        if ready is None:
            return match[0]
        return match[1] + require_pinned_reference(ready["reference"]) + match[3]

    rewritten = re.sub(pattern, replace, text, flags=re.M | re.I)
    return {**recipe, "dockerfile": rewritten} if rewritten != text else recipe


def complete_prepared_image(recipe, mappings):
    text = recipe["dockerfile"]
    if re.search(r"^\s*#\s*(syntax|escape|check)\s*=", text, re.I | re.M):
        return None
    lines = [line.strip() for line in text.splitlines() if line.strip() and not line.lstrip().startswith("#")]
    if not lines or not lines[0].startswith("FROM "):
        return None
    item = mappings.get(lines[0][5:])
    if item is None:
        return None
    if lines == [lines[0]] and item.get("preparation", "source") == "source":
        return item["image_id"]
    expected = [lines[0], "USER root", "WORKDIR /testbed",
                "RUN git fetch origin '+refs/heads/*:refs/remotes/origin/*'",
                "RUN command -v rg || (apt-get update && apt-get install -y --no-install-recommends ripgrep)"]
    if lines == expected and item.get("preparation") == "swesmith-v1":
        return item["image_id"]
    return None


def rewrite_index(source, output, catalogs):
    if output.exists():
        raise ValueError("never overwrite an existing recipe index")
    mappings = {}
    for catalog in catalogs:
        if catalog.get("schema") != 1:
            raise ValueError("unsupported pool catalog")
        for source_ref, item in catalog["images"].items():
            if item.get("status") != "ready":
                continue
            require_pinned_reference(item["reference"])
            # Enriched preparations are gated by their exact recipe below.
            for alias in {source_ref, item["source_reference"], *item.get("import_aliases", {})}:
                if alias in mappings and mappings[alias]["reference"] != item["reference"]:
                    raise ValueError("conflicting prepared image mappings")
                mappings[alias] = item
    temporary = output.with_name(output.name + ".preparing")
    with temporary.open("xb"):
        pass
    changed = 0
    try:
        with closing(sqlite3.connect(source.resolve().as_uri() + "?mode=ro", uri=True)) as old:
            with closing(sqlite3.connect(temporary)) as new:
                old.backup(new)
                for identity, encoded in new.execute("SELECT source, recipe FROM images").fetchall():
                    recipe = json.loads(encoded)
                    rewritten = rewrite_bases(recipe, mappings)
                    if rewritten == recipe:
                        continue
                    prepared = complete_prepared_image(recipe, mappings)
                    new.execute("UPDATE images SET recipe=?,prepared_image=? WHERE source=?",
                                (json.dumps(rewritten, sort_keys=True), prepared, identity))
                    changed += 1
                new.commit()
        os.link(temporary, output)
        temporary.unlink()
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    return {"rewritten": changed}


def audit_index(source, catalogs):
    """Account for every recipe without inferring task readiness from a warm base.

    Run after rewriting. A prepared_image must occur in a validated catalog;
    arbitrary existing IDs and unsupported Dockerfiles remain unverified.
    This is an offline receipt audit, not a live registry availability check.
    """
    references, image_ids = set(), set()
    for catalog in catalogs:
        if catalog.get("schema") != 1:
            raise ValueError("unsupported image catalog")
        entries = list(catalog.get("images", {}).values()) + list(catalog.get("foundations", {}).values())
        for item in entries:
            if item.get("status") != "ready" and item.get("validated") is not True:
                continue
            references.add(require_pinned_reference(item["reference"]))
            image_ids.add(item["image_id"])
    counts = Counter()
    cold_sources = Counter()
    with closing(sqlite3.connect(source.resolve().as_uri() + "?mode=ro", uri=True)) as connection:
        for _, encoded, prepared in connection.execute("SELECT source,recipe,prepared_image FROM images"):
            counts["recipes"] += 1
            if prepared:
                counts["prepared" if prepared in image_ids else "unverified_prepared"] += 1
                continue
            counts["live_builds"] += 1
            recipe = json.loads(encoded)
            text = recipe["dockerfile"]
            supported = not re.search(r"^\s*#\s*(syntax|escape|check)\s*=", text, re.I | re.M)
            pattern = r"^[ \t]*FROM[ \t]+([^\s\\]+)(?:[ \t]+AS[ \t]+([\w.-]+))?[ \t]*$"
            matches = list(re.finditer(pattern, text, re.I | re.M))
            supported &= bool(matches) and len(matches) == len(re.findall(r"^[ \t]*FROM\b", text, re.I | re.M))
            stages, cold = set(), set()
            for match in matches:
                base = match[1]
                if base.lower() not in stages and base.lower() != "scratch" and base not in references:
                    cold.add(base)
                if match[2]:
                    stages.add(match[2].lower())
            # COPY --from and RUN --mount can introduce additional image inputs.
            # Reject unknown/numeric-unbounded sources rather than undercounting.
            for dependency in re.findall(r"(?:--from[= ]|\bfrom=)([^\s,]+)", text):
                if dependency.lower() not in stages and not (dependency.isdecimal() and int(dependency) < len(matches)):
                    supported = False
            if cold or not supported:
                counts["cold_or_unknown_builds"] += 1
                cold_sources.update(cold or {"<unsupported Dockerfile>"})
            else:
                counts["builds_with_prepared_bases"] += 1
    return {**{key: counts[key] for key in ("recipes", "prepared", "unverified_prepared", "live_builds",
                                          "cold_or_unknown_builds", "builds_with_prepared_bases")},
            "cold_bases": dict(cold_sources.most_common()),
            "scope": "Catalog receipt audit; remaining RUN/COPY work and live artifact availability are not qualified"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    plan = commands.add_parser("plan")
    plan.add_argument("--inventory", type=Path, required=True)
    plan.add_argument("--output", type=Path, required=True)
    plan.add_argument("--limit", type=int, default=100)
    plan.add_argument("--per-family", type=int, default=2)
    plan.add_argument("--balanced", action="store_true", help="distribute remaining work by fraction of each family visited")
    rewrite = commands.add_parser("rewrite-index")
    rewrite.add_argument("--source", type=Path, required=True)
    rewrite.add_argument("--output", type=Path, required=True)
    rewrite.add_argument("--catalog", type=Path, action="append", required=True)
    report = commands.add_parser("report")
    report.add_argument("--inventory", type=Path, required=True)
    report.add_argument("--catalog", type=Path, action="append", required=True)
    audit = commands.add_parser("audit-index")
    audit.add_argument("--source", type=Path, required=True)
    audit.add_argument("--catalog", type=Path, action="append", required=True)
    audit.add_argument("--max-live-builds", type=int, required=True)
    audit.add_argument("--max-cold-builds", type=int, default=0)
    args = parser.parse_args()
    if args.command == "plan":
        inventory = json.loads(args.inventory.read_text())
        if inventory.get("schema") != 1:
            raise ValueError("unsupported inventory")
        images = select_images(inventory["images"], args.limit, args.per_family, balanced=args.balanced)
        args.output.mkdir(exist_ok=False)
        (args.output / "plan.json").write_text(json.dumps({**inventory, "images": images}, indent=2) + "\n")
        print(json.dumps({"images": len(images), "task_rows": sum(x["task_rows"] for x in images)}))
    elif args.command == "report":
        print(json.dumps(coverage_report(json.loads(args.inventory.read_text()),
                                         [json.loads(p.read_text()) for p in args.catalog]), indent=2))
    elif args.command == "audit-index":
        if min(args.max_live_builds, args.max_cold_builds) < 0:
            parser.error("build allowances cannot be negative")
        result = audit_index(args.source, [json.loads(p.read_text()) for p in args.catalog])
        result["within_budget"] = (result["live_builds"] <= args.max_live_builds
                                   and result["cold_or_unknown_builds"] <= args.max_cold_builds
                                   and result["unverified_prepared"] == 0)
        print(json.dumps(result, indent=2))
        if not result["within_budget"]:
            raise SystemExit(1)
    else:
        print(json.dumps(rewrite_index(args.source, args.output, [json.loads(p.read_text()) for p in args.catalog])))


if __name__ == "__main__":
    main()
