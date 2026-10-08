#!/usr/bin/env python3
"""Fill the gateway's image index with every training task that has something
prepared (docs/image-index.md).

  export --inventory ZIP --tmax-bundle DIR --terminal-lego-bundle DIR
         --openswe-recipes SQLITE --out DIR
      Where the dataset access is (Hugging Face). Writes DIR/<environment>/manifest.jsonl:
      one row per image name, with its environment, its dataset tasks, its source, and
      either its prepared image or its recipe's build context (DIR/<environment>/contexts/,
      so DIR is self-contained). DIR/excluded.jsonl says why a name was left out;
      DIR/summary.json counts both.
  register DIR [--environment E ...] [--gateway URL] [--token-file FILE]
      Where the gateway is. Uploads recipe contexts, registers the names (the gateway
      refuses any whose image or base is not in the chunk store) and writes
      DIR/<environment>/refused.jsonl. Re-running is safe: unchanged names are no-ops.

The inventory is the 2026-10-01 training inventory
(all-cached-training-tasks-with-terminal-lego-2026-10-01.zip): for every image name the
pinned research-environments tasksets (c7ea0d7, feat/lumi-ucloud-envs-20260930) ask for,
it records the prepared image or base and the recipe's sha256. A name with no remaining
build work is registered as its prepared image; every other name as its recipe, checked
against that sha256. Names the inventory does not list have nothing prepared and are
not registered.

Task ids are what each taskset's task_ids_file filters on: TMax and Terminal-Lego task
names, R2E-Gym's commit_hash, SWE-smith's "<language>:<instance_id>", and instance_id
elsewhere. They are read from the datasets at the inventory's pinned revisions.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
import gzip
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import sys
import time
import zipfile

ENVIRONMENT_REVISION = "c7ea0d7fe3379f0e6ffb0a68c82c80c184601f92"
INVENTORY = "2026-10-01"
ENVIRONMENTS = {"TMax": "tmax", "Terminal-Lego": "terminal-lego", "OpenSWE": "openswe", "ScaleSWE": "scaleswe",
                "R2E-Gym": "r2e-gym", "SWE-Lego": "swe-lego", "SWE-rebench v2": "swe-rebench-v2",
                "MultiSWE": "multiswe", "SWE-smith": "swe-smith"}
# The dataset column each taskset names its tasks by (its task_ids_file key).
TASK_COLUMN = {"openswe": "instance_id", "scaleswe": "instance_id", "r2e-gym": "commit_hash",
               "swe-lego": "instance_id", "swe-rebench-v2": "instance_id", "multiswe": "instance_id",
               "swe-smith": "instance_id"}


def recipe_sha(recipe):
    """The inventory's recipe identity."""
    return hashlib.sha256(json.dumps(recipe, sort_keys=True).encode()).hexdigest()


def prepared_reference(reference):
    """host:port/repository:tag@digest -> repository@digest (the gateway adds its host)."""
    location, _, digest = reference.partition("@")
    repository = location.split("/", 1)[1]
    if ":" in repository.rsplit("/", 1)[-1]:
        repository = repository.rsplit(":", 1)[0]
    return f"{repository}@{digest}"


def read_inventory(path):
    with zipfile.ZipFile(path) as archive:
        images = json.loads(archive.read("all-cached-training-tasks/all-image-selectors.json"))["images"]
        rows = [json.loads(line) for line in gzip.decompress(
            archive.read("all-cached-training-tasks/all-task-rows.jsonl.gz")).decode().splitlines()]
    return images, rows


def dataset_task_ids(rows, cache):
    """{(dataset, revision, shard, row_index): task id} for rows outside TMax and Terminal-Lego."""
    from huggingface_hub import hf_hub_download
    wanted = defaultdict(set)
    for row in rows:
        environment = ENVIRONMENTS[row["family"]]
        if environment in TASK_COLUMN:
            wanted[(row["dataset"], row["revision"], row["shard"], environment)].add(row["row_index"])
    ids = {}
    for (dataset, revision, shard, environment), indexes in sorted(wanted.items()):
        path = hf_hub_download(dataset, shard, repo_type="dataset", revision=revision, cache_dir=cache)
        column = TASK_COLUMN[environment]
        if shard.endswith(".jsonl"):
            with open(path) as handle:
                values = [json.loads(line).get(column) for line in handle]
        else:
            import pyarrow.parquet as parquet
            values = parquet.read_table(path, columns=[column]).column(column).to_pylist()
        language = dataset.rsplit("-", 1)[1] if environment == "swe-smith" else None
        for index in indexes:
            value = values[index]
            if not value:
                raise ValueError(f"{dataset}@{revision} {shard} row {index} has no {column}")
            ids[(dataset, revision, shard, index)] = f"{language}:{value}" if language else str(value)
        print(f"  {dataset} {shard}: {len(indexes)} task ids", file=sys.stderr, flush=True)
    return ids


def bundle_contexts(bundle):
    """{task: context directory} of an import_image_recipes.py export."""
    root = Path(bundle).resolve()
    return {row["task"]: root / row["context"] for row in map(json.loads, (root / "manifest.jsonl").open())}


def bundle_recipe(environment, context):
    """The recipe a TMax or Terminal-Lego context carries, as the inventory hashed it."""
    recipe = {"dockerfile": (context / "Dockerfile").read_text()}
    if environment == "terminal-lego":
        recipe["files"] = {"verifier-bootstrap.sh": (context / "verifier-bootstrap.sh").read_text()}
        task_file = context / "task_file"
        if task_file.is_dir() and not any(task_file.iterdir()):
            recipe["directories"] = ["task_file"]
    return recipe


def export(args):
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=False)  # Never write into an earlier export.
    images, rows = read_inventory(args.inventory)
    print(f"inventory: {len(images)} names, {len(rows)} task rows", file=sys.stderr, flush=True)
    ids = dataset_task_ids(rows, args.hf_cache)
    tasks, revisions = defaultdict(list), {}
    for row in rows:
        revisions.setdefault(row["image"], row["revision"])
        environment = ENVIRONMENTS[row["family"]]
        task = row.get("task_name") if environment in ("tmax", "terminal-lego") else \
            ids[(row["dataset"], row["revision"], row["shard"], row["row_index"])]
        tasks[(environment, row["image"])].append(task)
    contexts = {"tmax": bundle_contexts(args.tmax_bundle),
                "terminal-lego": bundle_contexts(args.terminal_lego_bundle)}
    with sqlite3.connect(f"file:{args.openswe_recipes}?mode=ro", uri=True) as db:
        openswe = {source: json.loads(recipe) for source, recipe in db.execute("SELECT source, recipe FROM images")}
    counts, manifests = Counter(), {}
    excluded = (out / "excluded.jsonl").open("w")

    def exclude(environment, name, reason):
        counts[(environment, "excluded: " + reason)] += 1
        excluded.write(json.dumps({"environment": environment, "name": name, "reason": reason}) + "\n")

    for entry in sorted(images, key=lambda item: (item["family"], item["image"])):
        environment, name = ENVIRONMENTS[entry["family"]], entry["image"]
        source = {"dataset": entry["dataset"], "revision": entry["revision"] or revisions.get(name),
                  "inventory": INVENTORY,
                  "environment_revision": ENVIRONMENT_REVISION}
        row = {"name": name, "environment": environment, "tasks": sorted(set(tasks[(environment, name)])),
               "source": source}
        if not row["tasks"]:
            exclude(environment, name, "no task uses it")
            continue
        if not entry["remaining_build_work"]:  # The whole image is prepared.
            row["prepared_reference"] = prepared_reference(entry["prepared_reference"])
        else:
            source["recipe_sha256"] = entry["recipe_sha256"]
            if environment == "openswe":
                recipe = openswe.get(entry["dataset_image"])
                if recipe is None or set(recipe) != {"dockerfile"}:
                    exclude(environment, name, "no Dockerfile-only recipe")
                    continue
                context = out / environment / "contexts" / entry["dataset_image"]
                context.mkdir(parents=True)
                (context / "Dockerfile").write_text(recipe["dockerfile"])
            else:
                exported = contexts[environment].get(entry["task_name"])
                if exported is None:
                    exclude(environment, name, "not in the recipe export")
                    continue
                context = out / environment / "contexts" / entry["task_name"]
                shutil.copytree(exported, context, symlinks=True)
                recipe = bundle_recipe(environment, context)
            if recipe_sha(recipe) != entry["recipe_sha256"]:
                exclude(environment, name, "recipe differs from the inventory's")
                continue
            row["context"] = str(context.relative_to(out))
        if environment not in manifests:
            (out / environment).mkdir(parents=True, exist_ok=True)
            manifests[environment] = (out / environment / "manifest.jsonl").open("w")
        manifests[environment].write(json.dumps(row) + "\n")
        counts[(environment, "prepared" if "prepared_reference" in row else "recipe")] += 1
        counts[(environment, "tasks")] += len(row["tasks"])
    excluded.close()
    for handle in manifests.values():
        handle.close()
    summary = defaultdict(dict)
    for (environment, key), value in sorted(counts.items()):
        summary[environment][key] = value
    meta = {"inventory": str(args.inventory), "environment_revision": ENVIRONMENT_REVISION,
            "exported_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "environments": summary}
    (out / "summary.json").write_text(json.dumps(meta, indent=1) + "\n")
    print(json.dumps(meta["environments"], indent=1))


def register(args):
    import ucloud_sandboxes_sdk as sdk
    from ucloud_sandboxes_sdk.client import _image_build_request
    root = Path(args.directory)
    token = Path(args.token_file).read_text().strip()
    client = sdk.SandboxClient(args.gateway, api_token=token, timeout_seconds=300)
    environments = args.environment or sorted(p.name for p in root.iterdir() if (p / "manifest.jsonl").is_file())

    def with_context(row):
        if "context" not in row:
            return row
        image = sdk.Image.from_dockerfile(name=row["name"], context_path=root / row["context"])
        with _image_build_request(image) as (payload, archive):
            client._upload_build_context(payload, archive, time.monotonic() + 600)
        out = {key: value for key, value in row.items() if key != "context"}
        return {**out, "context_archive_digest": payload["context_archive_digest"],
                "context_archive_size": payload["context_archive_size"], "retention": args.retention}

    for environment in environments:
        rows = [json.loads(line) for line in (root / environment / "manifest.jsonl").open()]
        registered = refused = 0
        with (root / environment / "refused.jsonl").open("w") as refusals, ThreadPoolExecutor(args.uploads) as pool:
            for start in range(0, len(rows), args.batch):
                batch = list(pool.map(with_context, rows[start:start + args.batch]))
                for row in batch:
                    row.setdefault("retention", args.retention)
                result = client._request_json("POST", "/v1/image-recipes",
                                              payload={"recipes": batch, "partial": True}, timeout_seconds=600)
                registered += result["registered"]
                refused += len(result["refused"])
                for item in result["refused"]:
                    refusals.write(json.dumps(item) + "\n")
                print(json.dumps({"environment": environment, "registered": registered, "refused": refused,
                                  "of": len(rows)}), flush=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    out = commands.add_parser("export")
    out.add_argument("--inventory", type=Path, required=True)
    out.add_argument("--tmax-bundle", type=Path, required=True)
    out.add_argument("--terminal-lego-bundle", type=Path, required=True)
    out.add_argument("--openswe-recipes", type=Path, required=True)
    out.add_argument("--hf-cache", default=None, help="Hugging Face cache directory for dataset shards")
    out.add_argument("--out", type=Path, required=True)
    reg = commands.add_parser("register")
    reg.add_argument("directory", type=Path)
    reg.add_argument("--environment", action="append")
    reg.add_argument("--gateway", default="http://127.0.0.1:8090")
    reg.add_argument("--token-file", default="/var/lib/ucloud-sandboxes/state/sandbox-api-token")
    reg.add_argument("--retention", choices=("pinned", "cached"), default="cached")
    reg.add_argument("--batch", type=int, default=500)
    reg.add_argument("--uploads", type=int, default=8, help="contexts uploaded at once")
    args = parser.parse_args(argv)
    (export if args.command == "export" else register)(args)


if __name__ == "__main__":
    main()
