"""Build-pilot inputs: sample foundation-backed training tasks per family and lay out
each task's exact training recipe as a build context.
  prepare.py OUT_DIR
Recipes are checked against the selection's recipe_sha256, so the pilot builds what
training would ask for. Writes OUT_DIR/tasks.json and OUT_DIR/contexts/<id>/.
"""
import hashlib
import json
import random
import shutil
import subprocess
import sqlite3
import sys
import zipfile
from pathlib import Path

SEED = 20261007
COUNTS = {"TMax": 100, "Terminal-Lego": 100, "OpenSWE": 50}
REPO = Path("/home/alex-admin/ucloud-sandboxes")
SELECTION = Path("/home/alex-admin/all-cached-training-tasks-with-terminal-lego-2026-10-01.zip")
TMAX_REPO = Path("/tmp/ucloud-cache-study-prime-tasks-20260930")
TMAX = TMAX_REPO / "datasets/tmax"
TERMINAL = Path("/tmp/ucloud-cache-study-terminal-20260930")
TERMINAL_RECIPES = REPO / "build/terminal-resolver-20261001/eligible.json"
OPENSWE = REPO / "build/openswe-foundations-20260930/coverage/source.sqlite"

out = Path(sys.argv[1])
contexts = out / "contexts"
contexts.mkdir(parents=True, exist_ok=False)


def recipe_sha(recipe):
    return hashlib.sha256(json.dumps(recipe, sort_keys=True).encode()).hexdigest()


selectors = json.loads(zipfile.ZipFile(SELECTION).read("all-cached-training-tasks/all-image-selectors.json"))
pools = {family: sorted((entry for entry in selectors["images"]
                         if entry["family"] == family and entry["cached_kind"] == "foundation"),
                        key=lambda entry: entry["image"])
         for family in COUNTS}
terminal = {row["task"]: row for row in json.loads(TERMINAL_RECIPES.read_text())}
terminal_tree = {}
for row in json.loads((TERMINAL_RECIPES.parent / "context-tree.json").read_text()):
    if "/environment/" in row["path"]:
        terminal_tree.setdefault(row["path"].split("/", 1)[0], []).append(row)


def git_blob(oid, repo=TERMINAL):
    """A blob from a dataset checkout, fetched from its origin when the partial clone lacks it."""
    command = ["git", "-C", str(repo), "cat-file", "blob", oid]
    found = subprocess.run(command, capture_output=True)
    if found.returncode:
        subprocess.run(["git", "-c", "gc.auto=0", "-C", str(repo), "fetch", "--no-tags", "--no-write-fetch-head",
                        "--recurse-submodules=no", "--filter=blob:none", "origin", oid],
                       check=True, capture_output=True, timeout=120)
        found = subprocess.run(command, capture_output=True, check=True)
    return found.stdout
with sqlite3.connect(OPENSWE) as db:
    openswe = {source: json.loads(recipe) for source, recipe in db.execute("SELECT source, recipe FROM images")}

rng = random.Random(SEED)
tasks, mismatched, lfs_pointers = [], [], []
for family, count in COUNTS.items():
    for entry in rng.sample(pools[family], count):
        name = entry["task_name"] or entry["dataset_image"]
        task_id = f"{family.lower()}-{len(tasks):03d}"
        context = contexts / task_id
        context.mkdir()
        if family == "TMax":
            environment = TMAX / name / "environment"
            recipe = {"dockerfile": (environment / "Dockerfile").read_text()}
            # A sparse checkout: write the whole environment tree from git
            # (fixtures included), not just the checked-out files.
            prefix = f"datasets/tmax/{name}/environment/"
            listing = subprocess.run(["git", "-C", str(TMAX_REPO), "ls-tree", "-r", "-z", "HEAD", prefix],
                                     check=True, capture_output=True).stdout.decode()
            for item in filter(None, listing.split("\0")):
                meta, path = item.split("\t", 1)
                mode, kind, oid = meta.split()
                if kind != "blob":
                    continue
                target = context / path.removeprefix(prefix)
                target.parent.mkdir(parents=True, exist_ok=True)
                data = git_blob(oid, TMAX_REPO)
                if data.startswith(b"version https://git-lfs.github.com/spec/v1"):
                    lfs_pointers.append((task_id, path))
                if mode == "120000":
                    target.symlink_to(data.decode())
                else:
                    target.write_bytes(data)
                    target.chmod(0o755 if mode == "100755" else 0o644)
        elif family == "Terminal-Lego":
            recipe = terminal[name]["recipe"]
            # The checkout is a partial clone: write each context file from its
            # git object (the tree the selection was checked against).
            for row in terminal_tree.get(name, ()):
                relative = row["path"].split("/environment/", 1)[1]
                if relative == "Dockerfile":
                    continue
                target = context / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(git_blob(row["oid"]))
                target.chmod(0o755 if row["mode"] == "100755" else 0o644)
            for directory in recipe.get("directories", []):
                (context / directory).mkdir(parents=True, exist_ok=True)
            for relative, text in recipe.get("files", {}).items():
                (context / relative).write_text(text)
            (context / "Dockerfile").write_text(recipe["dockerfile"])
        else:
            recipe = openswe[entry["dataset_image"]]
            (context / "Dockerfile").write_text(recipe["dockerfile"])
        if recipe_sha(recipe) != entry["recipe_sha256"]:
            mismatched.append(task_id)
        tasks.append({"id": task_id, "family": family, "task": name, "image": entry["image"],
                      "foundation_key": entry["foundation_key"],
                      "remaining_build_work": entry["remaining_build_work"],
                      "recipe_sha256": entry["recipe_sha256"], "recipe_matches": task_id not in mismatched,
                      "context_bytes": sum(p.stat().st_size for p in context.rglob("*") if p.is_file())})

(out / "tasks.json").write_text(json.dumps({"seed": SEED, "tasks": tasks}, indent=1) + "\n")
print(json.dumps({family: sum(t["family"] == family for t in tasks) for family in COUNTS}),
      "recipe mismatches:", len(mismatched), mismatched[:10],
      "context MB:", round(sum(t["context_bytes"] for t in tasks) / 1e6, 1), "LFS pointers:", lfs_pointers[:5])
